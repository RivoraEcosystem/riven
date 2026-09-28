from __future__ import annotations

import asyncio
from collections.abc import Callable
import winloop

def winloop_loop_factory() -> Callable[[],asyncio.AbstractEventLoop]:
    return winloop.new_event_loop