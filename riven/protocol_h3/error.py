from enum import IntEnum , StrEnum

class WebTransportErrorCode(IntEnum):
    """WebTransport error codes as defined in [draft-ietf-webtrans-http3-16](
    https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-16#section-9.2)"""
    # WebTransport protocol-defined errors
    WT_BUFFERED_STREAM_REJECTED = 0x3994bd84
    WT_SESSION_GONE = 0x170d7b68
    WT_FLOW_CONTROL_ERROR = 0x045d4487
    WT_ALPN_ERROR = 0x0817b3dd
    WT_REQUIREMENTS_NOT_MET = 0x212c0d48
    
class WebTransportApplicationErrorCode(IntEnum):
    """WebTransport application error codes by Riven."""
    
    WT_APP_NO_ERROR = 0x0 # no error in application (STOP_SENDING , RESET_STREAM_AT)
    WT_APP_INTERNAL_ERROR = 0x1 # error in application not in network (RESET_STREAM_AT , )
    WT_APP_STREAM_CLOSED_BY_APPLICATION = 0x2 # stream closed by application (STOP_SENDING)
    WT_APP_STOP_SENDING_BY_APPLICATION = 0x3 # application requesting to stop sending (STOP_SENDING)