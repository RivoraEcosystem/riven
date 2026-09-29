# Riven

Riven is an HTTP/3 server for Python applications, built around the requirements of [RFC 9114](https://www.rfc-editor.org/rfc/rfc9114.html).

Riven provides two application interfaces:

* **RCP** - Riven's native application interface, designed alongside the server to expose its HTTP/3 protocol model.
* **ASGI** - compatibility support for existing Python applications and frameworks that use the ASGI specification.

RCP is the default interface. ASGI applications can be selected with `--application-interface asgi`.

## Project Status

Riven is currently in **alpha development** and is not ready for production use.

The server can run HTTP/3 applications in development environments, but the API, behavior, and implementation may change as the project evolves. Bugs and incomplete features are expected.

ASGI support currently covers the common HTTP request and response events, as well as lifespan handling. Application compatibility may vary depending on how an application uses ASGI, so applications should be tested in a development environment before being used with Riven.

## Installation

Install Riven with pip:

```console
pip install riven
```

To install Riven with a platform-specific event loop implementation, use the `standard` extra:

```console
pip install "riven[standard]"
```

The `standard` extra installs:

* `uvloop` on Linux and macOS
* `winloop` on Windows

Riven can also run with Python's standard `asyncio` event loop without either package.

## ASGI Application

Create an ASGI application in `example.py`:

```python
async def app(scope, receive, send):
    assert scope["type"] == "http"

    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [
            (b"content-type", b"text/plain"),
        ],
    })

    await send({
        "type": "http.response.body",
        "body": b"Hello, world!",
    })
```

Start Riven with the ASGI application and a TLS certificate and private key:

```console
riven example:app --application-interface asgi --ssl-certfile cert.pem --ssl-keyfile key.pem
```

The application argument uses `module:attribute` notation.

For example:

```console
riven myapp.server:app --application-interface asgi --ssl-certfile cert.pem --ssl-keyfile key.pem
```
><small>Riven requires a TLS certificate and private key because QUIC provides built-in TLS encryption.</small>

imports `app` from `myapp/server.py`.

Riven listens on `127.0.0.1:8000` by default. Use `--host` and `--port` to change the bind address.

## Why Riven Supports Both RCP and ASGI

Riven's primary goal is to provide an HTTP/3-focused server that follows the protocol requirements defined by RFC 9114.

**RCP** is Riven's native application interface and is designed alongside Riven to represent its HTTP/3 protocol model directly.

**ASGI** is supported to make Riven usable with existing Python applications and frameworks without requiring them to be rewritten around RCP.

RCP is the default application interface. To run an ASGI application, use:

```console
riven example:app --application-interface asgi --ssl-certfile cert.pem --ssl-keyfile key.pem
```

For more information about RCP, see the [RCP project](https://github.com/RivoraEcosystem/rcp).

When using an ASGI application with Riven, keep in mind that HTTP/3 has protocol requirements that differ from older HTTP versions. In particular, some connection-specific headers and behaviors that may be valid in HTTP/1.1 or HTTP/2 are not permitted in HTTP/3.

## Standalone HTTP/3 Deployment

For standalone HTTP/3 deployment, publish an HTTPS DNS resource record as defined by [RFC 9460](https://www.rfc-editor.org/rfc/rfc9460.html).

The record should advertise the HTTP/3 endpoint using the `h3` ALPN identifier. For a service running on the default HTTPS port, the record can look like this:

```dns
example.com.  HTTPS  1  .  alpn="h3"
```

For a non-default port, add the `port` parameter:

```dns
example.com.  HTTPS  1  .  alpn="h3" port="8443"
```

Configure the corresponding TLS certificate on Riven and allow QUIC traffic over UDP on the configured port.

Riven currently supports HTTP/3 only. Until additional HTTP protocols are supported, advertise the service using the `h3` ALPN identifier.

## Contributions

Riven is currently in active development, and its architecture may continue to evolve as the project progresses.

If you are interested in contributing, feel free to reach out first so I can share the current areas where contributions would be most useful.

* [LinkedIn](https://www.linkedin.com/in/bhavya-malhotra-7265513b1/)
* [Email: contact@bhavyamalhotra.in](mailto:contact@bhavyamalhotra.in)

You can also [open an issue](https://github.com/RivoraEcosystem/riven/issues) or start a [discussion](https://github.com/RivoraEcosystem/riven/discussions) if you have an idea, question, bug report, or technical suggestion.

## Discussions

Ideas, architecture questions, and technical discussions are welcome in the repository's [Discussions](https://github.com/RivoraEcosystem/riven/discussions).

## License

Riven is licensed under the [BSD License](https://github.com/RivoraEcosystem/riven/blob/main/LICENSE).


Built for the next generation of Python web applications, beyond request and response.