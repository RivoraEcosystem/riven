from dataclasses import dataclass
from .logger import LOGGING_CONFIG
from typing import Any , Literal , get_args
from rcp import RCPApplication , H3_FORBIDDEN_HEADERS
import os
import logging.config
import logging
import json
import importlib
import sys
import asyncio
from collections.abc import Callable

LOG_LEVELS: dict[str, int] = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}

APPLICATION_INTERFACE_SPEC:dict[str, dict[str, str]] = {
    'asgi' : {"version": "3.0", "spec_version": "2.4"},
    'rcp' : {"version": "1.0"}
}

LIFESPAN_CLASS:dict[str, str] = {
    "on" : "riven.lifespan.lifespan:LifeSpanOn",
    "off" : "riven.lifespan.lifespan:LifeSpanOff"
}

LOOP_FACTORIES: dict[str, str | None] = {
    "none" : None,
    "auto" : "riven.loop.auto:auto_loop_factory",
    "asyncio" : "riven.loop.asyncio:asyncio_loop_factory",
    "uvloop" : "riven.loop.uvloop:uvloop_loop_factory",
    "winloop" : "riven.loop.winloop:winloop_loop_factory"
} 

LIFESPAN = Literal["on", "off"]
APPLICATION_INTERFACE = Literal['rcp','asgi']
LOOP_FACTORY_TYPE = Literal["none", "auto", "asyncio", "uvloop" , "winloop"]
STARTUP_SHUTDOWN_FAILURE = 3

logger = logging.getLogger("riven")

class RivenConfig:
    

    @property
    def max_header_size(self) -> int:
        return self.max_header_size_kb * 1024
    

    def __init__(
        self,
        app:RCPApplication | str,
        host:str = "127.0.0.1",
        port:int = 8000,
        max_header_size_kb:int = 0,
        root_path:str = "",
        log_config:Any = LOGGING_CONFIG,
        headers: list[tuple[str, str]] | None = None,
        server_header:bool = True,
        ssl_keyfile: str | os.PathLike[str] | None = None,
        ssl_certfile: str | os.PathLike[str] | None = None,
        ssl_keyfile_password: str | None = None,
        access_log: bool = True,
        use_colors:bool = True,
        log_level:str|int|None = None,
        application_interface:APPLICATION_INTERFACE|str = 'rcp',
        app_factory:bool = False,
        lifespan:LIFESPAN = "on",
        loop:LOOP_FACTORY_TYPE|str = "auto",
        shutdown_timeout:int = 5,
    ):
        self.app = app
        self.host = host
        self.port = port
        self.max_header_size_kb = max_header_size_kb
        self.root_path = root_path
        self.log_config = log_config
        self.headers = headers or []
        self.server_header = server_header
        self.ssl_keyfile = ssl_keyfile
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile_password = ssl_keyfile_password
        self.access_log = access_log
        self.use_colors = use_colors
        self.application_interface = application_interface
        self.log_level = log_level
        self.app_factory = app_factory
        self.lifespan = lifespan
        self.loop = loop
        self.shutdown_timeout = shutdown_timeout

        self.encoded_headers: list[tuple[bytes, bytes]] = []
        self.loaded = False
        
        self.configure_logger()

    def configure_headers(self):
        self.encoded_headers = [] # reset existing headers if any
        encoded_headers = [(key.lower().encode("latin1"), value.encode("latin1")) for key, value in self.headers]
        has_server = any(name == b"server" for name,_ in encoded_headers)
        if self.server_header and not has_server:
            self.encoded_headers = [(b'server',b'riven')] + encoded_headers # add server header and concat the list

        for header in list(encoded_headers):
            if header[0] in H3_FORBIDDEN_HEADERS: # omit forbidden headers
                continue

            self.encoded_headers.append((header[0],header[1]))

    def configure_logger(self) -> None:
        if self.log_config is not None:
            if isinstance(self.log_config,os.PathLike):
                self.log_config = os.fspath(self.log_config)

            if isinstance(self.log_config,dict):
                if self.use_colors in (True,False):
                    self.log_config["formatters"]["default"]["use_colors"] = self.use_colors
                    self.log_config["formatters"]["access"]["use_colors"] = self.use_colors

                logging.config.dictConfig(self.log_config)
        
            elif isinstance(self.log_config,str) and self.log_config.endswith(".json"):
                with open(self.log_config) as config:
                    loaded_config = json.load(config)
                    logging.config.dictConfig(loaded_config)

            else: # Riven cannot process log config
                raise RuntimeError("Failed to load Log config please provide valid config using JSON/DICT")

        if self.log_level is not None:
            if isinstance(self.log_level,str):
                log_level = LOG_LEVELS.get(self.log_level.lower())
                if log_level is None:
                    raise ValueError("Invalid LOG LEVEL %s" % self.log_level)
            else:
                log_level = self.log_level

            logging.getLogger("riven").setLevel(log_level)
            logging.getLogger("riven.access").setLevel(log_level)
            logging.getLogger("riven.protocol").setLevel(log_level)
            logging.getLogger("riven.lifecycle").setLevel(log_level)

        if self.access_log is False:
            logging.getLogger("riven.access").handlers = []
            logging.getLogger("riven.access").propagate = False

    def get_loop_factory(self) -> Callable[[], asyncio.AbstractEventLoop] | None:
        if self.loop in LOOP_FACTORIES:
            loop_factory: Callable[...,Any]|None = import_with_string(LOOP_FACTORIES[self.loop])
        else:
            try:
                return import_with_string(self.loop)
            except ImporterError as e:
                logger.error("Error loading custom loop factory. %s" % e)
                sys.exit(STARTUP_SHUTDOWN_FAILURE)
        if loop_factory is None:
            return None

        return loop_factory()

    def import_app(self) -> Any:
        "Import APP using string and return it"
        try:
            return import_with_string(self.app)
        except ImporterError as e:
            logger.error("Error Loading %s Application %s" % (self.application_interface.upper(),e))
            sys.exit(STARTUP_SHUTDOWN_FAILURE)

    def load(self) -> None:
        if self.loaded:
            raise RuntimeError("Riven config already loaded")

        if (not 1 <= self.port <= 65535):
            raise RuntimeError("Invalid value for port %d" % self.port)

        if (self.ssl_keyfile is None) != (self.ssl_certfile is None):
            raise RuntimeError("SSL keyfile and certfile must be provided together")

        self.configure_headers()

        if isinstance(self.application_interface,str):
            if not self.application_interface.lower() in get_args(APPLICATION_INTERFACE):
                raise RuntimeError("Invalid Application Interface %s" % self.application_interface)

            self.application_interface = self.application_interface.lower()

        try:
            self.lifespan_class = import_with_string(LIFESPAN_CLASS[self.lifespan]) # get lifespan class
        except ImporterError as e:
            logger.error("Error Loading Lifespan class %s" % e)
            sys.exit(STARTUP_SHUTDOWN_FAILURE)

        self.loaded_application = self.import_app()

        try: # try calling application
            self.loaded_application = self.loaded_application()
        except TypeError as exc: # type error raised application is not loading through factory
            if self.app_factory:
                logger.error("Error Loading %s app factory: %s" % (self.application_interface.upper(),exc))
                sys.exit(STARTUP_SHUTDOWN_FAILURE)
        else:
            if not self.app_factory: # factory detected but app factory settings not turned on
                logger.error("App Factory Detected please turn on app factory settings")
                sys.exit(STARTUP_SHUTDOWN_FAILURE)

        self.loaded = True

class ImporterError(Exception):
    pass


def import_with_string(import_str: Any) -> Any:
    if not isinstance(import_str, str):
        return import_str

    module_str, _, attrs_str = import_str.partition(":")
    if not module_str or not attrs_str:
        message = 'Import string "{import_str}" invalid\nImport String must be in format "<module>:<attribute>"'
        raise ImporterError(message.format(import_str=import_str))

    try:
        module = importlib.import_module(module_str)
    except ModuleNotFoundError as exc:
        if exc.name != module_str:
            raise exc from None
        message = 'Failed to import module "{module_str}"'
        raise ImporterError(message.format(module_str=module_str))

    instance = module
    try:
        for attr_str in attrs_str.split("."):
            instance = getattr(instance, attr_str)
    except AttributeError:
        message = 'Attribute "{attrs_str}" not found in module "{module_str}".'
        raise ImporterError(message.format(attrs_str=attrs_str, module_str=module_str))

    return instance