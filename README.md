# Riven

> **Status:** In Development

Riven is an asynchronous Python application server built around the **Rivora Contract Protocol (RCP)** for modern web protocols.

Riven is designed to keep the server and web framework independent. Any framework that implements RCP can run on Riven without Riven needing to know the framework's internal implementation.

Development is currently focused on building the HTTP/3 server architecture and networking layer using `aioquic`, `asyncio`, and `uvloop`.

## Current Focus

The development order is intentional:

1. HTTP/3
2. WebTransport
3. Workers and server tooling
4. HTTP/2
5. HTTP/1.1

HTTP/2 and HTTP/1.1 are planned for later and are not currently under active development.

## Architecture

Riven is built around a separation between the server, protocol, and application.

* **Riven** handles networking and protocol-level work.
* **RCP** provides the interface between the server and application.
* **RCP-compatible frameworks** can run on Riven.
* The server does not need to know the internal implementation of the framework.

This allows the server and framework to evolve independently while communicating through a common protocol.

## RCP

Riven communicates with applications through the **Rivora Contract Protocol**.

RCP is developed separately as part of the Rivora Ecosystem.

→ [View RCP](https://github.com/RivoraEcosystem/RCP)

## Current Development

Currently working on:

* HTTP/3 and QUIC
* Connection management
* HTTP stream handling
* RCP application integration
* Lifespan handling
* Server logging
* Core server architecture

## Planned

* WebTransport
* Worker support
* HTTP/2
* HTTP/1.1
* Performance improvements
* Production tooling
* Monitoring and diagnostics

## Development Status

Riven is actively being developed and is **not ready for production use yet**.

The project is being built in public as part of the Rivora Ecosystem, with development logs, architecture decisions, experiments, and releases shared along the way.

## Updates

Development updates for Riven and the other Rivora Ecosystem projects are shared on:

→ [r/RivoraEcosystem](https://www.reddit.com/r/RivoraEcosystem/)

## Contributions

Riven is currently in active development, so contribution opportunities may change as the architecture evolves.

If you are interested in contributing, feel free to reach out first so I can share the current areas where contributions would be useful.

* [LinkedIn](https://www.linkedin.com/in/bhavya-malhotra-7265513b1/)
* [contact@bhavyamalhotra.in](mailto:contact@bhavyamalhotra.in)

You can also open an issue or start a discussion if you have an idea, question, or technical suggestion.

## Discussions

Ideas, architecture questions, and technical discussions are welcome in the repository's [Discussions](../../discussions).

## License

MIT
