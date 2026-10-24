"""
NOTE: This protocol is implemented with reference to IETF WebTransport
over HTTP/3 Draft 16. Please ensure your client-side implementation
supports this version before using the protocol.
Reference: [draft-ietf-webtrans-http3-16](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-16)

"""


from __future__ import annotations

import logging
from enum import IntEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class FlowControlCapsuleType(IntEnum):
    """A Webtransport Session can send Flow control capsules over a 
    Session's Connect stream to manage Data flow in a session 
    if flow control is not enabled an endpoint MUST ignore receipt
    of any flow control capsules [Refer to current IETF draft](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-16#section-5)"""

    WT_MAX_STREAMS_UNI = 0x190B4D40
    WT_MAX_STREAMS_BIDI = 0x190B4D3F
    WT_STREAMS_BLOCKED_UNI = 0x190B4D44
    WT_STREAMS_BLOCKED_BIDI = 0x190B4D43
    WT_MAX_DATA = 0x190B4D3D
    WT_DATA_BLOCKED = 0x190B4D41
    WT_DRAIN_SESSION = 0x78AE
    WT_CLOSE_SESSION = 0x2843

class WebTransportSettings(IntEnum):
    # Required Settings for using webtransport
    SETTINGS_ENABLE_CONNECT_PROTOCOL = 0x08
    SETTINGS_H3_DATAGRAM = 0x33
    SETTINGS_WT_ENABLED = 0x2c7cf000
    SETTINGS_WT_INITIAL_MAX_DATA = 0x2b61
    SETTINGS_WT_INITIAL_MAX_STREAMS_UNI = 0x2b64
    SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI = 0x2b65


access_logger = logging.getLogger("riven.access")
protocol_logger = logging.getLogger("riven.protocol")
