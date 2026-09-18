from dataclasses import dataclass
from .logger import LOGGING_CONFIG
from typing import Any , Literal , get_args
from rcp import RCPApplication
import os
import logging.config
import logging
import json
import importlib
import sys

LOG_LEVELS: dict[str, int] = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}

APPLICATION_INTERFACE = Literal['rcp','asgi']
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
        port:str = 8000,
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
        application_interface:APPLICATION_INTERFACE|str = 'rcp'

    ):
        self.app = app,
        self.host = host,
        self.port = port,
        self.max_header_size_kb = max_header_size_kb
        self.root_path = root_path
        self.log_config = log_config
        self.headers = headers or []
        self.encoded_headers: list[tuple[bytes, bytes]] = []
        self.server_header = server_header
        self.ssl_keyfile = ssl_keyfile
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile_password = ssl_keyfile_password
        self.access_log = access_log
        self.use_colors = use_colors
        self.application_interface = application_interface
        self.log_level = log_level
        self.loaded = False
        self.configure_logger()

    def configure_headers(self):
        encoded_headers = [(key.lower().encode("latin1"), value.encode("latin1")) for key, value in self.headers]
        self.encoded_headers = (
            [(b"server", b"riven")] + encoded_headers
            if b"server" not in encoded_headers and self.server_header
            else encoded_headers
        )

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
                log_level = LOG_LEVELS[self.log_level.lower()]
            else:
                log_level = self.log_level

            logging.getLogger("riven").setLevel(log_level)
            logging.getLogger("riven.access").setLevel(log_level)
            logging.getLogger("riven.protocol").setLevel(log_level)
            logging.getLogger("riven.lifecycle").setLevel(log_level)

        if self.access_log is False:
            logging.getLogger("riven.access").handlers = []
            logging.getLogger("riven.access").propagate = False


    def import_app(self) -> None:
        "Import APP using string and return it"
        try:
            return import_with_string(self.app)
        except ImporterError as e:
            logger.error("Error Loading %s Application %s" % (self.application_interface.upper(),e))
            sys.exit(STARTUP_SHUTDOWN_FAILURE)

    def load(self) -> None:
        if self.loaded:
            raise RuntimeError("Riven config already loaded")

        if not (self.ssl_keyfile is None or self.ssl_certfile):
            raise RuntimeError("SSL keyfile/certfile Missing")

        self.configure_headers()

        if isinstance(self.application_interface,str):
            if not self.application_interface in get_args(APPLICATION_INTERFACE):
                raise RuntimeError("Invalid Application Interface %s" % self.application_interface)


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