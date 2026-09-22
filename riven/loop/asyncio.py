from __future__ import annotations
import asyncio
from collections.abc import Callable

def asyncio_loop_factory() -> Callable[[],asyncio.AbstractEventLoop]:
    return asyncio.SelectorEventLoop