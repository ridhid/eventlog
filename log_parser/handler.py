import functools
from collections import defaultdict
from typing import DefaultDict, Dict, List

from log_parser.const import (REQUEST_TYPE_BACKEND_CONNECT, REQUEST_TYPE_BACKEND_ERROR, REQUEST_TYPE_BACKEND_OK,
                              REQUEST_TYPE_FINISH, REQUEST_TYPE_START, REQUEST_TYPE_START_MERGE,
                              REQUEST_TYPE_START_SEND_RESULT)
from log_parser.parser import BackendConnectLineInfo, BackendErrorLineInfo, BackendOkLineInfo, LineInfo

MAX_REQUEST_WITH_TOO_LONG_SEND_RESULT_PHASE = 10





class RequestStats:
    start_times: Dict[int, int]

    poll_ends_at_timestamp: Dict[int, int]
    merge_ends_at_timestamp: Dict[int, int]
    send_result_timestamps: Dict[int, int]

    durations: List[int]

    connections: DefaultDict[int, Dict[int, str]]
    connections_response_status: DefaultDict[int, Dict[int, bool]]

    backends_groups: Dict[str, int] = {}
    backends_connections: DefaultDict[str, int]
    backends_errors: DefaultDict[str, Dict[str, int]]

    longest_send_results_phase: List[int]

    not_completed: int

    def __init__(self):
        self.clean()

    def clean(self, a=1):
        self.start_times = {}

        self.poll_ends_at_timestamp = {}
        self.merge_ends_at_timestamp = {}
        self.send_result_timestamps = {}

        self.durations = []

        self.connections = defaultdict(dict)
        self.connections_response_status = defaultdict(dict)

        self.backends_groups = {}
        self.backends_connections = defaultdict(lambda: 0)
        self.backends_errors = defaultdict(lambda: defaultdict(lambda: 0))

        self.longest_send_results_phase = []
        self.not_completed = 0

    def remove_request(self, request_id):
        del self.start_times[request_id]
        del self.poll_ends_at_timestamp[request_id]
        del self.merge_ends_at_timestamp[request_id]
        del self.send_result_timestamps[request_id]

        del self.connections[request_id]
        del self.connections_response_status[request_id]


stats = RequestStats()


def handle_info_start_request(line_info):
    stats.start_times[line_info.request_id] = line_info.timestamp


def handle_info_backend_connect(line_info: BackendConnectLineInfo):
    stats.connections[line_info.request_id][line_info.group_id] = line_info.backend_url
    stats.connections_response_status[line_info.request_id][line_info.group_id] = False

    stats.backends_groups[line_info.backend_url] = line_info.group_id
    stats.backends_connections[line_info.backend_url] += 1


def handle_info_backend_error(line_info: BackendErrorLineInfo):
    backend_url = stats.connections[line_info.request_id][line_info.group_id]
    stats.backends_errors[backend_url][line_info.error] += 1


def handle_info_backend_ok(line_info: BackendOkLineInfo):
    stats.connections_response_status[line_info.request_id][line_info.group_id] = True


def handle_info_start_merge(line_info):
    stats.poll_ends_at_timestamp[line_info.request_id] = line_info.timestamp


def handle_info_start_send_result(line_info):
    stats.merge_ends_at_timestamp[line_info.request_id] = line_info.timestamp


def handle_info_request_finish(line_info):
    stats.send_result_timestamps[line_info.request_id] = line_info.timestamp

    # calculate total time of request
    request_start_at: int = stats.start_times[line_info.request_id]
    duration = line_info.timestamp - request_start_at

    stats.durations.append(duration)

    # compare phase duration
    if len(stats.longest_send_results_phase) < MAX_REQUEST_WITH_TOO_LONG_SEND_RESULT_PHASE:

        start_at = stats.start_times[line_info.request_id]
        backend_poll_end_at = stats.poll_ends_at_timestamp[line_info.request_id]
        merge_phase_end_at = stats.merge_ends_at_timestamp[line_info.request_id]
        send_results_end_at = stats.send_result_timestamps[line_info.request_id]

        backend_poll_duration = backend_poll_end_at - start_at
        merge_duration = merge_phase_end_at - backend_poll_end_at
        send_results_duration = send_results_end_at - merge_phase_end_at

        if send_results_duration > merge_duration and send_results_duration > backend_poll_duration:
            stats.longest_send_results_phase.append(line_info.request_id)

    # check not completed request
    if not all(stats.connections_response_status[line_info.request_id].values()):
        stats.not_completed += 1

    stats.remove_request(line_info.request_id)


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
