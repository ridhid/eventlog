import functools
import math
import re
from argparse import ArgumentParser
from collections import defaultdict
from typing import DefaultDict, Dict, List, Optional

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
request_connections_response_status: DefaultDict[int, Dict[int, bool]] = defaultdict(dict)

backends_replicas: Dict[str, int] = {}
backends_connections: DefaultDict[str, int] = defaultdict(lambda: 0)
backends_errors: DefaultDict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(lambda: 0))

requests_with_too_long_send_results_phase = []

general_re = re.compile(r'(?P<timestamp>\d+)\s(?P<id>\d+)\s(?P<type>\w+).*')
backend_connect_re = re.compile(rf'.*{REQUEST_TYPE_BACKEND_CONNECT}\s(?P<group>\d+)\s+(?P<url>\S+)')
backend_ok_re = re.compile(rf'.*{REQUEST_TYPE_BACKEND_OK}\s+(?P<group>\d+)')
backend_error_re = re.compile(rf'.*{REQUEST_TYPE_BACKEND_ERROR}\s(?P<group>\d+)\s+(?P<error>.*)')


class Counter:

    def __init__(self, default=0):
        self.value = default

    def __str__(self):
        return f'{self.value}'

    def increase(self):
        self.value += 1


class LineInfo:
    request_id: int
    request_type: str
    timestamp: int

    attrs = ('request_id', 'request_type', 'timestamp')

    def __init__(self, request_id: int, request_type: str, timestamp: int):
        self.request_id = request_id
        self.request_type = request_type
        self.timestamp = timestamp

    def __eq__(self, other):
        # print(list((getattr(self, key, None) , getattr(other, key, None)) for key in self.attrs))
        # import ipdb; ipdb.set_trace()
        return all(getattr(self, key, None) == getattr(other, key, None) for key in self.attrs)

    def __repr__(self):
        attrs = " ".join(f"{key}: {getattr(self, key, None)}" for key in self.attrs)
        return f'<{self.__class__.__name__} {attrs}>'


class BackendErrorLineInfo(LineInfo):
    group_id: int
    error: str

    attrs = LineInfo.attrs + ('group_id', 'error')

    def __init__(self, group_id: int, error: str, *args, **kwargs):
        self.group_id = group_id
        self.error = error
        super().__init__(*args, **kwargs)


class BackendOkLineInfo(LineInfo):
    group_id: int

    attrs = LineInfo.attrs + ('group_id',)

    def __init__(self, group_id: int, *args, **kwargs):
        self.group_id = group_id
        super().__init__(*args, **kwargs)


class BackendConnectLineInfo(LineInfo):
    group_id: int
    backend_url: str

    attrs = LineInfo.attrs + ('group_id', 'backend_url')

    def __init__(self, group_id: int, backend_url: str, *args, **kwargs):
        self.group_id = group_id
        self.backend_url = backend_url
        super().__init__(*args, **kwargs)


requests_not_completed = Counter()


def percentile(array, percent) -> Optional[int]:
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

    return int(d0 + d1)


percentil_95 = functools.partial(percentile, percent=0.95)


def clean_resource(request_id: int):
    del requests_start_time[request_id]
    del backend_poll_phase_times[request_id]
    del merge_results_phase_time[request_id]
    del request_send_result_phase[request_id]

    del request_connections[request_id]
    del request_connections_response_status[request_id]


def handle_info_start_request(line_info):
    requests_start_time[line_info.request_id] = line_info.timestamp


def handle_info_backend_connect(line_info: BackendConnectLineInfo):
    request_connections[line_info.request_id][line_info.group_id] = line_info.backend_url
    request_connections_response_status[line_info.request_id][line_info.group_id] = False

    backends_replicas[line_info.backend_url] = line_info.group_id
    backends_connections[line_info.backend_url] += 1


def handle_info_backend_error(line_info: BackendErrorLineInfo):
    backend_url = request_connections[line_info.request_id][line_info.group_id]
    backends_errors[backend_url][line_info.error] += 1


def handle_info_backend_ok(line_info: BackendOkLineInfo):
    request_connections_response_status[line_info.request_id][line_info.group_id] = True


def handle_info_start_merge(line_info):
    backend_poll_phase_times[line_info.request_id] = line_info.timestamp


def handle_info_start_send_result(line_info):
    merge_results_phase_time[line_info.request_id] = line_info.timestamp


def handle_info_request_finish(line_info):
    request_send_result_phase[line_info.request_id] = line_info.timestamp

    # calculate total time of request
    request_start_at = requests_start_time[line_info.request_id]
    duration = line_info.timestamp - request_start_at

    requests_durations.append(duration)

    # compare phase duration
    if len(requests_with_too_long_send_results_phase) < MAX_REQUEST_WITH_TOO_LONG_SEND_RESULT_PHASE:

        start_at = requests_start_time[line_info.request_id]
        backend_poll_end_at = backend_poll_phase_times[line_info.request_id]
        merge_phase_end_at = merge_results_phase_time[line_info.request_id]
        send_results_end_at = request_send_result_phase[line_info.request_id]

        backend_poll_duration = backend_poll_end_at - start_at
        merge_duration = merge_phase_end_at - backend_poll_end_at
        send_results_duration = send_results_end_at - merge_phase_end_at

        if send_results_duration > merge_duration and send_results_duration > backend_poll_duration:
            requests_with_too_long_send_results_phase.append(line_info.request_id)

    if not all(request_connections_response_status[line_info.request_id].values()):
        requests_not_completed.increase()

    clean_resource(line_info.request_id)


@functools.lru_cache()
def get_mapping():
    return {
        REQUEST_TYPE_START: handle_info_start_request,
        REQUEST_TYPE_START_MERGE: handle_info_start_merge,
        REQUEST_TYPE_START_SEND_RESULT: handle_info_start_send_result,
        REQUEST_TYPE_BACKEND_CONNECT: handle_info_backend_connect,
        REQUEST_TYPE_BACKEND_ERROR: handle_info_backend_error,
        REQUEST_TYPE_BACKEND_OK: handle_info_backend_ok,
        REQUEST_TYPE_FINISH: handle_info_request_finish
    }


def handle_info(line_info: LineInfo):
    mapping = get_mapping()
    handler = mapping.get(line_info.request_type)
    if handler:
        return handler(line_info)


class ParseException(Exception):
    pass


def parse_line_backend_connect(line: str,
                               request_id: int,
                               request_type: str,
                               timestamp: int) -> BackendConnectLineInfo:
    match = backend_connect_re.match(line)
    if not match:
        raise ParseException(f"Can't parse backend arguments from line: {line}")

    group_id = int(match.group('group'))
    backend_url = match.group('url')

    return BackendConnectLineInfo(
        request_id=request_id,
        request_type=request_type,
        timestamp=timestamp,
        group_id=group_id,
        backend_url=backend_url
    )


def parse_line_backend_error(line: str, request_id: int, request_type: str, timestamp: int) -> BackendErrorLineInfo:
    match = backend_error_re.match(line)
    if not match:
        raise ParseException(f"Can't parse backend arguments from line: {line}")

    error = match.group('error')
    group_id = int(match.group('group'))

    return BackendErrorLineInfo(
        request_id=request_id,
        request_type=request_type,
        timestamp=timestamp,
        group_id=group_id,
        error=error
    )


def parse_line_backend_ok(line: str, request_id: int, request_type: str, timestamp: int) -> BackendOkLineInfo:
    match = backend_ok_re.match(line)
    if not match:
        raise ParseException(f"Can't parse backend arguments from line: {line}")

    group_id = int(match.group('group'))

    return BackendOkLineInfo(
        request_id=request_id,
        request_type=request_type,
        timestamp=timestamp,
        group_id=group_id
    )


def parse_line(line: str) -> LineInfo:
    # from ipdb import set_trace; set_trace()
    match = general_re.match(line)
    if not match:
        raise ParseException(f"Can't parse line: {line}")

    request_id: int = int(match.group('id'))
    request_type: str = match.group('type')
    timestamp: int = int(match.group('timestamp'))

    if request_type == REQUEST_TYPE_BACKEND_CONNECT:
        return parse_line_backend_connect(line, request_id, request_type, timestamp)

    elif request_type == REQUEST_TYPE_BACKEND_ERROR:
        return parse_line_backend_error(line, request_id, request_type, timestamp)

    elif request_type == REQUEST_TYPE_BACKEND_OK:
        return parse_line_backend_ok(line, request_id, request_type, timestamp)

    return LineInfo(
        request_id=request_id,
        request_type=request_type,
        timestamp=timestamp,
    )


def parse_input(file_name: str) -> None:
    with open(file_name, 'r') as source:
        for line in source:
            try:
                line_info: LineInfo = parse_line(line)
                handle_info(line_info)
            except ParseException as exception:
                print(str(exception))


def print_result(file_name: str) -> None:
    requests_time_percentil = percentil_95(array=requests_durations)

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
