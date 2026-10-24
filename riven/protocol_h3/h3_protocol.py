"""
NOTE: This protocol is implemented with reference to IETF WebTransport
over HTTP/3 Draft 16. Please ensure your client-side implementation
supports this version before using the protocol.
Reference: [draft-ietf-webtrans-http3-16](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-16)

"""

from __future__ import annotations

from aioquic._buffer import Buffer , BufferReadError
from aioquic.h3.connection import ErrorCode , H3Connection , Setting , SettingsError
from .capsules import WT_CLOSE_SESSION
from ..exceptions.exceptions import CapsuleError
import logging
from enum import IntEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._session_wt import WebTransportSessionProtocol

type Capsule = (
    WT_CLOSE_SESSION
)

class FlowControlCapsuleType(IntEnum):
    """A Webtransport Session can send Flow control capsules over a
    Session's Connect stream to manage Data flow in a session
    if flow control is not enabled an endpoint MUST ignore receipt
    of any flow control capsules [Refer to current IETF draft](
    https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-16#section-5)"""

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
    SETTINGS_WT_ENABLED = 0x2C7CF000
    SETTINGS_WT_INITIAL_MAX_DATA = 0x2B61
    SETTINGS_WT_INITIAL_MAX_STREAMS_UNI = 0x2B64
    SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI = 0x2B65


access_logger = logging.getLogger("riven.access")
protocol_logger = logging.getLogger("riven.protocol")

class WebTransportProtocol:
    def __init__(self):
        pass

    

class H3Protocol(H3Connection):
    """
    A High-level HTTP/3 wrapper for aioquic's H3Connection.
    """

    def __init__(self, quic, enable_webtransport=False):
        super().__init__(quic, enable_webtransport)

    def _get_local_settings(self) -> dict[int, int]:
        """
        Override internal method to get local settings updated to latest draft version of webtransport
        """
        settings: dict[int, int] = {
            Setting.QPACK_MAX_TABLE_CAPACITY: self._max_table_capacity,
            Setting.QPACK_BLOCKED_STREAMS: self._blocked_streams,
            WebTransportSettings.SETTINGS_ENABLE_CONNECT_PROTOCOL: 1,
            Setting.DUMMY: 1,
        }
        if self._enable_webtransport:
            settings[WebTransportSettings.SETTINGS_H3_DATAGRAM] = 1
            settings[WebTransportSettings.SETTINGS_WT_ENABLED] = 1
            settings[WebTransportSettings.SETTINGS_WT_INITIAL_MAX_DATA] = 1 * 1024 * 1024
            settings[WebTransportSettings.SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI] = 5
            settings[WebTransportSettings.SETTINGS_WT_INITIAL_MAX_STREAMS_UNI] = 5
        return settings

    def _validate_settings(self, settings: dict[int, int]) -> None:
        """
        Override internal method to validate remote settings against latest draft version of webtransport
        """

        for setting in [
            WebTransportSettings.SETTINGS_ENABLE_CONNECT_PROTOCOL,
            WebTransportSettings.SETTINGS_WT_ENABLED,
            WebTransportSettings.SETTINGS_H3_DATAGRAM,
        ]:
            if setting in settings and settings[setting] not in (0, 1):
                raise SettingsError(f"{setting.name} setting must be 0 or 1")

        if (
            settings.get(WebTransportSettings.SETTINGS_H3_DATAGRAM) == 1
            and self._quic._remote_max_datagram_frame_size is None
        ):
            raise SettingsError("SETTINGS_H3_DATAGRAM requires max_datagram_frame_size transport parameter")

        if (
            settings.get(WebTransportSettings.SETTINGS_WT_ENABLED) == 1
            and settings.get(WebTransportSettings.SETTINGS_H3_DATAGRAM) != 1
        ):
            raise SettingsError("SETTINGS_WT_ENABLED requires SETTINGS_H3_DATAGRAM")

        if (
            settings.get(WebTransportSettings.SETTINGS_WT_ENABLED) == 1
            and settings.get(WebTransportSettings.SETTINGS_H3_DATAGRAM) == 1
            and settings.get(WebTransportSettings.SETTINGS_ENABLE_CONNECT_PROTOCOL) != 1
        ):
            raise SettingsError("SETTINGS_WT_ENABLED requires SETTINGS_ENABLE_CONNECT_PROTOCOL")

    def parse_webtransport_capsule(self,session:WebTransportSessionProtocol) -> list[Capsule]:
    
        buf = Buffer(data=session._buffer)
        capsules:list[Capsule] = []
        consumed = 0
 
        while not buf.eof():
 
            try:
                capsule_type = buf.pull_uint_var()
                capsule_length = buf.pull_uint_var()
            except BufferReadError:
                # Incomplete capsule wait for more data
                break
 
            if buf.capacity - buf.tell() < capsule_length:
                # Incomplete capsule payload wait for more data
                break
 
            if capsule_type == FlowControlCapsuleType.WT_CLOSE_SESSION:
                # A WT_CLOSE_SESSION can contain 32 bit application error code and 1024 byte long error message only
                # https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-16#name-session-termination
                if capsule_length < 4 or capsule_length > 1028:
                    raise CapsuleError("Invalid WT_CLOSE_SESSION length")
 
                error_code = buf.pull_uint32()
                message = buf.pull_bytes((capsule_length-4))
                try:
                    error_message = message.decode("utf-8",errors='strict')
                except UnicodeDecodeError as exc:
                    raise CapsuleError(
                        "Invalid UTF-8 Application Error Message for WT_CLOSE_SESSION"
                    ) from exc
 
                capsules.append(
                    WT_CLOSE_SESSION(
                        type=FlowControlCapsuleType.WT_CLOSE_SESSION,
                        length=capsule_length,
                        Application_Error_Code=error_code,
                        Application_Error_Message=error_message
                    )
                )
            else:
                # Skip unknown or unhandled capsule types.
                buf.pull_bytes(capsule_length)
 
            # capsule fully parsed mark consumed bytes
            consumed = buf.tell()
 
        # remove processed data from buffer
        session._buffer = session._buffer[consumed:]
        return capsules