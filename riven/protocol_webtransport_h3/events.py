from dataclasses import dataclass

from aioquic.h3 import *
from rcp.events import Headers


@dataclass(slots=True,frozen=True)
class WebTransportSessionStart:
    """
    The WebTransportSessionStart event is fired when HTTP3 connection receives CONNECT request with `:protocol` pseudo-header field set to `webtransport`
    """

    method:bytes
    "value of `:method` pseudo-header field"

    protocol:bytes
    "value of `:protocol` pseudo-header field"

    scheme:bytes
    "value of `:scheme` pseudo-header field"

    authority:bytes
    "value of `:authority` pseudo-header field"

    path:bytes
    "value of `:path` pseudo-header field"

    headers:Headers
    "The headers of the CONNECT request"

    stream_id:int
    "The ID of the stream the CONNECT request was received for."