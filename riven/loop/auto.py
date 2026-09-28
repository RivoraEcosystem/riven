from __future__ import annotations
import asyncio
from collections.abc import Callable
import sys

def auto_loop_factory() -> Callable[[],asyncio.AbstractEventLoop]:

    # Windows Platform
    if sys.platform == "win32":
        try:
            import winloop
        except ImportError: # winloop not installed fallback to asyncio
            from .asyncio import asyncio_loop_factory

            return asyncio_loop_factory()
        else:
            from .winloop import winloop_loop_factory

            return winloop_loop_factory()

    # LINUX/MACOS Platform
    else:

        try:
            import uvloop
        except ImportError: # uvloop not install fallback to asyncio
            from .asyncio import asyncio_loop_factory

            return asyncio_loop_factory()

        else:
            from .uvloop import uvloop_loop_factory

            return uvloop_loop_factory()