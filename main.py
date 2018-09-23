import functools
import math
import re
from argparse import ArgumentParser
from typing import List, Mapping

requests: Mapping[int, int] = {}
requests_durations: List[int] = []

main_info_re = re.compile(r'(?P<timestamp>\d+)\s*(?P<id>\d+)\s+(?P<type>\w+)\s(?P<arguments>.*)')

REQUEST_TYPE_START = 'StartRequest'
REQUEST_TYPE_FINISH = 'FinishRequest'


def percentile(array, percent, key=lambda x: x):
    """
    Find the percentile of a list of values.

    @parameter array - is a list of values. Note N MUST BE already sorted.
    @parameter percent - a float value from 0.0 to 1.0.
    @parameter key - optional key function to compute value from each element of N.

    @return - the percentile of the values
    """
    if not array:
        return None

    k = (len(array) - 1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(array[int(k)])

    d0 = key(array[int(f)]) * (c - k)
    d1 = key(array[int(c)]) * (k - f)

    return d0 + d1


percentil_95 = functools.partial(percentile, percent=0.95)


def parse_request_record(request_id: int, request_type: str, timestamp: int, arguments: str):
    if request_type == REQUEST_TYPE_START:
        requests[request_id] = timestamp

    if request_type == REQUEST_TYPE_FINISH:
        request_start_at = requests[request_id]
        duration = timestamp - request_start_at

        requests_durations.append(duration)


def parse_line(line: str):
    match = main_info_re.match(line)
    if not match:
        print("Can't parse string %s", line)
        return

    request_id: int = int(match.group('id'))
    request_type: str = match.group('type')
    timestamp: int = int(match.group('timestamp'))
    arguments: str = match.group('arguments')

    parse_request_record(request_id=request_id, request_type=request_type, timestamp=timestamp, arguments=arguments)


def parse_input(file_name: str) -> None:
    with open(file_name, 'r') as source:
        for line in source:
            parse_line(line)


def print_result(file_name: str) -> None:
    requests_percentil = int(percentil_95(requests_durations))
    with open(file_name, 'w') as output:
        output.write(f'95-й перцентиль времени работы: {requests_percentil}\n')


median = functools.partial(percentile, percent=0.5)

if __name__ == '__main__':
    argparser = ArgumentParser()

    argparser.add_argument('-f', '--file', default='input.txt', help='Name of input file')
    argparser.add_argument('-o', '--output', default='output.txt', help='Name of output file')

    args = argparser.parse_args()

    parse_input(file_name=args.file)
    print_result(file_name=args.output)
