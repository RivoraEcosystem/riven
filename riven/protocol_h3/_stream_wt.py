from __future__ import annotations

from rcp import (
    Reader,
    Writer,
    WebTransportDataEventType,
    WebTransportDataSend,
    WebTransportDataReceived,
    WebTransportStreamReadEvent,
    WebTransportStreamWriteEvent
)


class StreamReader(Reader):

    def __init__(self,stream: WebTransportStream):
        self.stream = stream
        super().__init__()

    async def read(self) -> WebTransportStreamReadEvent: ...

    async def close(self) -> None: ...

class StreamWriter(Writer):
    def __init__(self,stream:WebTransportStream):
        self.stream = stream
        super().__init__()

    async def write(self, event:WebTransportStreamWriteEvent) -> None: ...

    async def close(self) -> None: ...

class WebTransportStream:
    _DISCONNECTED = 1 << 0

    def __init__(self):
        self._flags: int = 0
        self.stream_data_buffer = b""

    def _get_flag(self, mask: int) -> bool:
            return bool(self._flags & mask)
    
    def _set_flag(self, mask: int, value: bool) -> None:
        if value:
            self._flags |= mask
        else:
            self._flags &= ~mask

    @property
    def disconnected(self):
        return self._get_flag(self._DISCONNECTED)

    @disconnected.setter
    def disconnected(self, value: bool) -> None:
        self._set_flag(self._DISCONNECTED, value)