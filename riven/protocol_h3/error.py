from enum import IntEnum

WT_ERROR_FIRST = 0x52e4a40fa8db
WT_ERROR_LAST  = 0x52e5ac983162

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
    
    WT_APP_NO_ERROR = 0x0 # no error in application (RESET_STREAM_AT, STOP_SENDING)
    WT_APP_INTERNAL_ERROR = 0x1 # error in application not in network (RESET_STREAM_AT , STOP_SENDING)
    WT_APP_STREAM_CLOSED_BY_APPLICATION = 0x2 # stream closed by application (RESET_STREAM_AT , STOP_SENDING)
    WT_APP_STOP_SENDING_BY_APPLICATION = 0x3 # application requesting to stop sending (STOP_SENDING)



def app_error_to_wire_code(code: int) -> int:
    """Riven application uint32 -> WebTransport frame error code."""
    if not 0 <= code <= 0xFFFFFFFF:
        raise ValueError("Application error code must be uint32")

    return WT_ERROR_FIRST + code + (code // 0x1E)

def wire_code_to_app_error(wire_code: int) -> int:
    """WebTransport frame error code -> Riven application uint32."""
    if not WT_ERROR_FIRST <= wire_code <= WT_ERROR_LAST:
        raise ValueError("Not in the WT_APPLICATION_ERROR range")

    if (wire_code - 0x21) % 0x1F == 0:
        raise ValueError("Reserved HTTP/3 error codepoint")

    shifted = wire_code - WT_ERROR_FIRST
    return shifted - (shifted // 0x1F)

def encode_app_error(code: int) -> int:
    """Encode Riven application error code to wire error code."""
    return app_error_to_wire_code(code)

def decode_app_error(wire_code: int) -> int:
    """Decode wire error code to Riven application error code."""
    return wire_code_to_app_error(wire_code)