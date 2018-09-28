import re

from log_parser.const import REQUEST_TYPE_BACKEND_CONNECT, REQUEST_TYPE_BACKEND_ERROR, REQUEST_TYPE_BACKEND_OK

general_re = re.compile(r'(?P<timestamp>\d+)\s(?P<id>\d+)\s(?P<type>\w+).*')
backend_connect_re = re.compile(rf'.*{REQUEST_TYPE_BACKEND_CONNECT}\s(?P<group>\d+)\s+(?P<url>\S+)')
backend_ok_re = re.compile(rf'.*{REQUEST_TYPE_BACKEND_OK}\s+(?P<group>\d+)')
backend_error_re = re.compile(rf'.*{REQUEST_TYPE_BACKEND_ERROR}\s(?P<group>\d+)\s+(?P<error>.*)')


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

    def __init__(self, group_id: int, aaaargs: str, *args, **kwargs):
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
