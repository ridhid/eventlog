import pytest

from log_parser.const import (
    REQUEST_TYPE_BACKEND_CONNECT, REQUEST_TYPE_BACKEND_ERROR, REQUEST_TYPE_BACKEND_OK,
    REQUEST_TYPE_FINISH, REQUEST_TYPE_START, REQUEST_TYPE_START_MERGE, REQUEST_TYPE_START_SEND_RESULT,
)
from log_parser.parser import BackendConnectLineInfo, BackendErrorLineInfo, BackendOkLineInfo, LineInfo, parse_line


@pytest.mark.parametrize(
    'line, expected', (
        (
            "1390950160808136	0	StartRequest",
            LineInfo(request_id=0, request_type=REQUEST_TYPE_START, timestamp=1390950160808136)
        ),
        (
            "1390950162475798	0	StartMerge",
            LineInfo(request_id=0, request_type=REQUEST_TYPE_START_MERGE, timestamp=1390950162475798)
        ),
        (
            "1390950162536865	0	StartSendResult",
            LineInfo(request_id=0, request_type=REQUEST_TYPE_START_SEND_RESULT, timestamp=1390950162536865)
        ),
        (
            "1390950162890134	0	FinishRequest",
            LineInfo(request_id=0, request_type=REQUEST_TYPE_FINISH, timestamp=1390950162890134)
        ),
        (
            "1390950160810164	0	BackendConnect	0	http://backend0-001.yandex.ru:1963/search?",
            BackendConnectLineInfo(
                request_id=0,
                request_type=REQUEST_TYPE_BACKEND_CONNECT,
                timestamp=1390950160810164,
                group_id=0,
                backend_url="http://backend0-001.yandex.ru:1963/search?"
            )
        ),
        (
            "1390950160948308	0	BackendOk	1",
            BackendOkLineInfo(
                request_id=0,
                request_type=REQUEST_TYPE_BACKEND_OK,
                timestamp=1390950160948308,
                group_id=1
            )
        ),
        (
            "1390950161841530	0	BackendError	1	Request Timeout",
            BackendErrorLineInfo(
                request_id=0,
                request_type=REQUEST_TYPE_BACKEND_ERROR,
                timestamp=1390950161841530,
                group_id=1,
                error='Request Timeout'
            )
        ),
    )
)
def test_parse(line, expected):
    line_info: LineInfo = parse_line(line)

    assert line_info == expected
