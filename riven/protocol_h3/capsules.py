from dataclasses import dataclass
from typing import Literal


@dataclass(slots=True, frozen=True)
class WT_CLOSE_SESSION:
    """Sent when an application explicitly requests the session to be shut down."""

    type: Literal[0x2843]
    "Capsule type: 0x2843"  # webtransport-draft16

    length: int
    "Total payload length"

    application_error_code: int = 0
    "Application error code, or 0 if no error occurred"

    application_error_message: str = ""
    "Application error message, or an empty string if no error occurred"


@dataclass(slots=True, frozen=True)
class WT_DRAIN_SESSION:
    """Sent when a peer signals that it intends to gracefully retire or close the session."""

    type: Literal[0x78AE]
    "Capsule type: 0x78AE"  # webtransport-draft16

    length: Literal[0]
    "Total payload length: 0"


@dataclass(slots=True, frozen=True)
class WT_MAX_STREAMS_UNI:
    """
    Sent when a peer signals the maximum number of unidirectional streams
    that the peer may initiate over the lifetime of the session,
    including streams that have already been closed.
    """

    type: Literal[0x190B4D40]
    "Capsule type: 0x190B4D40 - Unidirectional streams"

    length: int
    "Total payload length"

    maximum_streams: int
    "Maximum number of streams"


@dataclass(slots=True, frozen=True)
class WT_MAX_STREAMS_BIDI:
    """
    Sent when a peer signals the maximum number of bidirectional streams
    that the peer may initiate over the lifetime of the session,
    including streams that have already been closed.
    """

    type: Literal[0x190B4D3F]
    "Capsule type: 0x190B4D3F - Bidirectional streams"

    length: int
    "Total payload length"

    maximum_streams: int
    "Maximum number of streams"


@dataclass(slots=True, frozen=True)
class WT_STREAMS_BLOCKED_UNI:
    """
    Sent when a peer reports the unidirectional stream limit
    at which the sender became blocked.
    """

    type: Literal[0x190B4D44]
    "Capsule type: 0x190B4D44 - Unidirectional streams"

    length: int
    "Total payload length"

    maximum_streams: int
    "Maximum stream limit at which the sender became blocked"


@dataclass(slots=True, frozen=True)
class WT_STREAMS_BLOCKED_BIDI:
    """
    Sent when a peer reports the bidirectional stream limit
    at which the sender became blocked.
    """

    type: Literal[0x190B4D43]
    "Capsule type: 0x190B4D43 - Bidirectional streams"

    length: int
    "Total payload length"

    maximum_streams: int
    "Maximum stream limit at which the sender became blocked"


@dataclass(slots=True, frozen=True)
class WT_MAX_DATA:
    """
    Sent when a peer reports the maximum number of bytes of
    WebTransport stream-body data that may be sent across the session.
    """

    type: Literal[0x190B4D3D]
    "Capsule type: 0x190B4D3D"

    length: int
    "Total payload length"

    maximum_data: int
    "Maximum amount of data that can be sent across the entire session"


@dataclass(slots=True, frozen=True)
class WT_DATA_BLOCKED:
    """
    Sent when a peer is unable to send data because of
    WebTransport session-level flow control.
    """

    type: Literal[0x190B4D41]
    "Capsule type: 0x190B4D41"

    length: int
    "Total payload length"

    maximum_data: int
    "Session-level limit at which the sender became blocked"
