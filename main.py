import functools
import math
import re
from argparse import ArgumentParser
from collections import defaultdict
from typing import DefaultDict, Dict, List

REQUEST_TYPE_START = 'StartRequest'
REQUEST_TYPE_FINISH = 'FinishRequest'
REQUEST_TYPE_BACKEND_CONNECT = 'BackendConnect'
REQUEST_TYPE_BACKEND_OK = 'BackendOk'
REQUEST_TYPE_BACKEND_ERROR = 'BackendError'
REQUEST_TYPE_START_MERGE = 'StartMerge'
REQUEST_TYPE_START_SEND_RESULT = 'StartSendResult'

MAX_REQUEST_WITH_TOO_LONG_SEND_RESULT_PHASE = 10

requests_start_time: Dict[int, int] = {}

backend_poll_phase_times: Dict[int, int] = {}
merge_results_phase_time: Dict[int, int] = {}
request_send_result_phase: Dict[int, int] = {}

requests_durations: List[int] = []

request_connections: DefaultDict[int, Dict[int, str]] = defaultdict(dict)
request_connections_response_status: DefaultDict[int, Dict[int, str]] = defaultdict(dict)

backends_replicas: Dict[str, int] = {}
backends_connections: DefaultDict[str, int] = defaultdict(lambda: 0)
backends_errors: DefaultDict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(lambda: 0))

requests_with_too_long_send_results_phase = []

general_re = re.compile(r'(?P<timestamp>\d+)\s*(?P<id>\d+)\s+(?P<type>\w+)\s.*')
backend_connect_re = re.compile(f'.*{REQUEST_TYPE_BACKEND_CONNECT}\s(?P<group>\d+)\s+(?P<url>\S+)')
backend_ok_re = re.compile(f'.*{REQUEST_TYPE_BACKEND_OK}\s+(?P<group>\d+)')
backend_error_re = re.compile(f'.*{REQUEST_TYPE_BACKEND_ERROR}\s(?P<group>\d+)\s+(?P<error>.*)')


class Counter:

    def __init__(self, default=0):
        self.value = default

    def __str__(self):
        return f'{self.value}'

    def increase(self):
        self.value += 1


requests_not_completed = Counter()


def percentile(array, percent):
    """
    Find the percentile of a list of values.

    @parameter array - is a list of values. Note N MUST BE already sorted.
    @parameter percent - a float value from 0.0 to 1.0.

    @return - the percentile of the values
    """
    if not array:
        return None

    k = (len(array) - 1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return array[int(k)]

    d0 = array[int(f)] * (c - k)
    d1 = array[int(c)] * (k - f)

    return d0 + d1


percentil_95 = functools.partial(percentile, percent=0.95)


def parse_request_record(request_id: int, request_type: str, timestamp: int):
    if request_type == REQUEST_TYPE_START:
        requests_start_time[request_id] = timestamp

    if request_type == REQUEST_TYPE_START_MERGE:
        backend_poll_phase_times[request_id] = timestamp

    if request_type == REQUEST_TYPE_START_SEND_RESULT:
        merge_results_phase_time[request_id] = timestamp

    if request_type == REQUEST_TYPE_FINISH:
        request_send_result_phase[request_id] = timestamp

        # calculate total time of request
        request_start_at = requests_start_time[request_id]
        duration = timestamp - request_start_at

        requests_durations.append(duration)

        # compare phase duration
        if len(requests_with_too_long_send_results_phase) < MAX_REQUEST_WITH_TOO_LONG_SEND_RESULT_PHASE:
            backend_poll_phase_time = backend_poll_phase_times[request_id]
            merge_phase_time = merge_results_phase_time[request_id]
            send_results_phase_time = request_send_result_phase[request_id]

            if send_results_phase_time > merge_phase_time and send_results_phase_time > backend_poll_phase_time:
                requests_with_too_long_send_results_phase.append(request_id)

        if not all(request_connections_response_status[request_id].values()):
            requests_not_completed.increase()

        # clean already handled results (memory optimization)
        del requests_start_time[request_id]
        del backend_poll_phase_times[request_id]
        del merge_results_phase_time[request_id]
        del request_send_result_phase[request_id]

        del request_connections[request_id]
        del request_connections_response_status[request_id]


def parse_request_backend_connect_record(request_id: int, group_id: int, backend_url: str):
    request_connections[request_id][group_id] = backend_url
    request_connections_response_status[request_id][group_id] = False

    backends_replicas[backend_url] = group_id
    backends_connections[backend_url] += 1


def parse_request_backend_ok_record(request_id: int, group_id: int):
    request_connections_response_status[request_id][group_id] = True


def parse_request_backend_error_record(request_id: int, group_id: int, error: str):
    backend_url = request_connections[request_id][group_id]
    backends_errors[backend_url][error] += 1


def parse_line(line: str):
    match = general_re.match(line)
    if not match:
        print(f"Can't parse line: {line}")
        return

    request_id: int = int(match.group('id'))
    request_type: str = match.group('type')
    timestamp: int = int(match.group('timestamp'))

    if request_type == REQUEST_TYPE_BACKEND_CONNECT:
        match = backend_connect_re.match(line)
        if not match:
            print(f"Can't parse backend arguments from line: {line}")
            return

        group_id = int(match.group('group'))
        backend_url = match.group('url')

        parse_request_backend_connect_record(request_id=request_id, group_id=group_id, backend_url=backend_url)

    elif request_type == REQUEST_TYPE_BACKEND_ERROR:
        match = backend_error_re.match(line)
        if not match:
            print(f"Can't parse backend arguments from line: {line}")
            return

        error = match.group('error')
        group_id = int(match.group('group'))

        parse_request_backend_error_record(request_id=request_id, group_id=group_id, error=error)

    elif request_type == REQUEST_TYPE_BACKEND_OK:

        match = backend_ok_re.match(line)
        if not match:
            print(f"Can't parse backend arguments from line: {line}")
            return

        group_id = int(match.group('group'))

        parse_request_backend_ok_record(request_id=request_id, group_id=group_id)
    else:
        parse_request_record(request_id=request_id, request_type=request_type, timestamp=timestamp)


def parse_input(file_name: str) -> None:
    with open(file_name, 'r') as source:
        for line in source:
            parse_line(line)


def print_result(file_name: str) -> None:
    requests_time_percentil = int(percentil_95(requests_durations))

    replica_groups = defaultdict(list)
    for backend, group_id in backends_replicas.items():
        replica_groups[group_id].append(backend)

    with open(file_name, 'w') as output:
        output.write(f'95-й перцентиль времени работы: {requests_time_percentil}\n')
        output.write(f'Идентификаторы запросов с самой долгой фазой отправки результатов пользователю:\n')

        for request_id in requests_with_too_long_send_results_phase:
            output.write(f'    {request_id}\n')

        output.write(f'Запросов с неполным набором ответивших ГР: {requests_not_completed}\n')
        output.write('Обращения и ошибки по бэкандам\n')
        groups = sorted(replica_groups.keys())
        for group in groups:
            output.write(f'    ГР {group}\n')

            backends = sorted(replica_groups[group])
            for backend in backends:
                connections = backends_connections[backend]

                output.write(f'        {backend}\n')
                output.write(f'            Обращения: {connections}\n')

                errors = backends_errors[backend]
                if errors:
                    output.write(f'            Ошибки\n')
                for error, count in errors.items():
                    output.write(f'                {error}: {count}\n')


if __name__ == '__main__':
    argparser = ArgumentParser()

    argparser.add_argument('-f', '--file', default='input.txt', help='Name of input file')
    argparser.add_argument('-o', '--output', default='output.txt', help='Name of output file')

    args = argparser.parse_args()

    parse_input(file_name=args.file)
    print_result(file_name=args.output)
