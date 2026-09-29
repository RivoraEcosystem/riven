from riven._config import (
    LOG_LEVELS,
    APPLICATION_INTERFACE_SPEC,
    LIFESPAN_CLASS,
    LOOP_FACTORIES,
    LOOP_FACTORY_TYPE,
    APPLICATION_INTERFACE,
    RivenConfig
)
import riven
from riven.server import RivenServer
from rcp import RCPApplication
import platform
import logging
import click
from collections.abc import Callable
from typing import Any
import os

LOG_CHOICES = click.Choice(list(LOG_LEVELS.keys()))
APPLICATION_INTERFACE_CHOICES = click.Choice(list(APPLICATION_INTERFACE_SPEC.keys()))
LIFESPAN_CHOICES = click.Choice(list(LIFESPAN_CLASS.keys()))
LOOP_CHOICES = click.Choice(list(LOOP_FACTORIES.keys()))


logger = logging.getLogger('riven')

def echo_version(ctx:click.Context,param:click.Parameter,value:bool) -> None:
    if not value or ctx.resilient_parsing:
        return

    click.echo(
        "Running Riven {version} on system {system}".format(
            version = riven.__version__,
            system=platform.system(),
        )
    )
    ctx.exit()

@click.command(context_settings={"auto_envvar_prefix" : "RIVEN"})
@click.argument("app",envvar="RIVEN_APP")
@click.option(
    "--host",
    type=str,
    default="127.0.0.1",
    help="Bind socket to this host.",
    show_default=True,
)
@click.option(
    "--port",
    type=int,
    default=8000,
    help="Bind socket to this port.",
    show_default=True,
)
@click.option(
    "--max-header-size",
    type=int,
    default=0,
    help="Maximum total size of incoming headers.",
    show_default=True,
)
@click.option(
    "--root-path",
    type=str,
    default="",
    help="Set the RCP/ASGI 'root_path' for applications submounted below a given URL path.",
    show_default=True,
)
@click.option(
    "--header",
    "headers",
    multiple=True,
    help="Specify custom default HTTP response headers as a Name:Value pair",
)
@click.option(
    "--server-header/--no-server-header",
    is_flag=True,
    default=True,
    help="Enable/Disable default Server header.",
)
@click.option(
    "--ssl-keyfile", 
    type=str, 
    default=None, 
    help="SSL key file", 
    show_default=True
)
@click.option(
    "--ssl-certfile", 
    type=str, 
    default=None, 
    help="SSL certificate file", 
    show_default=True
)
@click.option(
    "--ssl-keyfile-password",
    type=str,
    default=None,
    help="SSL keyfile password",
    show_default=True,
)
@click.option(
    "--access-log/--no-access-log",
    is_flag=True,
    default=True,
    help="Enable/Disable access log.",
)
@click.option(
    "--use-colors/--no-use-colors",
    is_flag=True,
    default=None,
    help="Enable/Disable colorized logging.",
)
@click.option(
    "--log-level",
    type=LOG_CHOICES,
    default=None,
    help="Log level. [default: info]",
    show_default=True,
)
@click.option(
    "--application-interface",
    type=APPLICATION_INTERFACE_CHOICES,
    default="rcp",
    help="Select RCP or ASGI as the application interface.",
    show_default=True,
)
@click.option(
    "--factory",
    is_flag=True,
    default=False,
    help="Treat APP as an application factory, i.e. a () -> <RCP/ASGI app> callable.",
    show_default=True,
)
@click.option(
    "--lifespan",
    type=LIFESPAN_CHOICES,
    default="on",
    help="Lifespan implementation.",
    show_default=True,
)
@click.option(
    "--loop",
    type=LOOP_CHOICES,
    default="auto",
    help="Event loop factory.",
    show_default=True,
)
@click.option(
    "--shutdown-timeout",
    type=int,
    default=5,
    help="Maximum number of seconds to wait for graceful shutdown.",
)
@click.option(
    "--version",
    is_flag=True,
    callback=echo_version,
    expose_value=False,
    is_eager=True,
    help="Display the Riven version and exit.",
)


def main(
    app:str,
    host:str,
    port:int,
    max_header_size:int,
    root_path:str,
    headers:list[str],
    server_header:bool,
    ssl_keyfile:str,
    ssl_certfile:str,
    ssl_keyfile_password:str,
    access_log:bool,
    use_colors:bool,
    log_level:str,
    application_interface:str,
    factory:bool,
    lifespan:str,
    loop:str,
    shutdown_timeout:int
) -> None:
    run(
        app=app,
        host=host,
        port=port,
        max_header_size=max_header_size,
        root_path=root_path,
        headers=[header.split(":", 1) for header in headers],
        server_header=server_header,
        ssl_keyfile=ssl_keyfile,
        ssl_certfile=ssl_certfile,
        ssl_keyfile_password=ssl_keyfile_password,
        access_log=access_log,
        use_colors=use_colors,
        log_level=log_level,
        application_interface=application_interface,
        factory=factory,
        lifespan=lifespan,
        loop=loop,
        shutdown_timeout=shutdown_timeout
    )

def run(
    app: RCPApplication|Callable[...,Any]|str,
    *,
    host:str = "127.0.0.1",
    port:int = 8000,
    max_header_size:int = 0,
    root_path:str = "",
    headers:list[tuple[str,str]] | None = None,
    server_header:bool = True,
    ssl_keyfile:str | os.PathLike[str] | None = None,
    ssl_certfile:str | os.PathLike[str] | None = None,
    ssl_keyfile_password:str|None = None,
    access_log:bool = True,
    use_colors:bool = True,
    log_level:str|int|None = None,
    application_interface:APPLICATION_INTERFACE|str = "rcp",
    factory:bool = True,
    lifespan:str = "on",
    loop:LOOP_FACTORY_TYPE|str = "auto",
    shutdown_timeout:int = 5
):
    config = RivenConfig(
        app=app,
        host=host,
        port=port,
        max_header_size_kb=max_header_size,
        root_path=root_path,
        headers=headers,
        server_header=server_header,
        ssl_keyfile=ssl_keyfile,
        ssl_certfile=ssl_certfile,
        ssl_keyfile_password=ssl_keyfile_password,
        access_log=access_log,
        use_colors=use_colors,
        log_level=log_level,
        application_interface=application_interface,
        app_factory=factory,
        lifespan=lifespan,
        loop=loop,
        shutdown_timeout=shutdown_timeout
    )

    config.import_app()

    server = RivenServer(config=config)
    try:
        server.run()
    except KeyboardInterrupt:
        pass