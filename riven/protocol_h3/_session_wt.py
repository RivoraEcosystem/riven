from __future__ import annotations

import asyncio
import logging
from asyncio import Queue, QueueShutDown
from typing import TYPE_CHECKING

from rcp import (
    WEBTRANSPORT_EXTENSION,
    RCPApplication,
    Reader,
    WebtransportReceiveEvent,
    WebtransportSendEvent,
    WebTransportSessionDisconnect,
    Writer,
)

if TYPE_CHECKING:
    from ._protocol import RivenH3

access_logger = logging.getLogger("riven.access")
protocol_logger = logging.getLogger("riven.protocol")


class WebTransportSessionProtocol:
    """
    High level wrapper for WebTransport over HTTP/3 to handle WebTransport specific events and streams.
    """

    _CLOSED = 1 << 0

    def __init__(self, protocol: RivenH3, scope: dict):

        self._protocol = protocol
        self._quic = protocol._quic
        self._http = protocol._http
        self.config = protocol.config
        self.scope = scope
        self.server_state = protocol.server_state
        self._queue: Queue[WebtransportReceiveEvent] = Queue()

        self._buffer: bytes = b""

        self._flags: int = 0

        # session disconnect info
        self.disconnected: bool = False
        self.disconnect_event: WebTransportSessionDisconnect | None = None
        self.session_id: int | None = None

    def _get_flag(self, mask: int) -> bool:
        return bool(self._flags & mask)

    def _set_flag(self, mask: int, value: bool) -> None:
        if value:
            self._flags |= mask
        else:
            self._flags &= ~mask

    @property
    def _closed(self) -> bool:
        return self._get_flag(self._CLOSED)

    @_closed.setter
    def _closed(self, value: bool) -> None:
        self._set_flag(self._CLOSED, value)

    def handle_webtransport_event(self, event) -> None:
        """
        Handle WebTransport specific events.
        """
        pass

    def handle_session_start(self) -> None:
        """
        Handle the WebTransport Session establishment invoke RCP Application.
        """

        if self.config.application_interface == "asgi":
            self.scope["extensions"] = WEBTRANSPORT_EXTENSION

        app = self.config.loaded_application
        loop = asyncio.get_running_loop()

        app_task = loop.create_task(self.run_rcp(app=app))

        self.server_state.application_tasks.add(app_task)
        app_task.add_done_callback(self.server_state.application_tasks.discard)

    async def run_rcp(self, app: RCPApplication) -> None:
        try:
            result = await app(self.scope, self.receive, self.send)

        except Exception as exc:
            msg = "Exception in RCP application\n"
            protocol_logger.error(msg, exc_info=exc)

        else:
            if result is not None:
                msg = f"RCP Application should return None, but returned {result}."
                protocol_logger.error(msg)

    async def receive(self) -> None:
        try:
            if self.disconnected:
                return self.disconnect_event

            event = await self._queue.get()
            return event

        except QueueShutDown:
            if self.disconnected:
                return self.disconnect_event

    async def send(self, event: WebtransportSendEvent) -> None | tuple[Writer, Reader]:
        try:
            ...
        except Exception:
            ...
