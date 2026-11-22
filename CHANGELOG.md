# Changelog

## Current changes

- Updated After a client disconnects, HTTP/3 and WebTransport `send()` calls raise `ClientDisconnected`, an `OSError` subclass. RCP application handlers catch this exception without logging it as an application error.
- Added 
- Added WebTransport over HTTP/3 session support, including extended CONNECT validation and handshake handling, WebTransport scopes, and configuration to enable or disable the feature.
- Added CONNECT stream, session, RCP application, and teardown state tracking, with corresponding state checks in WebTransport event handlers and application send/receive paths.
- Added Capsule Protocol support for WebTransport sessions, including parsing and validation of complete, incomplete, malformed, and trailing capsule data, plus `WT_CLOSE_SESSION` and flow-control capsules.
- Added WebTransport bidirectional and unidirectional stream handling, with reader and writer APIs for reading, sending stream data and closing.
- Added stream creation events, stream reset and stop-sending support, and `RESET_STREAM_AT`.
- Added WebTransport datagram handling and connection, session, and stream events, including server-initiated stream creation.
- Updated RCP application lifecycle and exception handling for HTTP/3 streams and WebTransport sessions.
- Restructured the HTTP/3 implementation into dedicated protocol, stream, session, capsule, and error modules. 
- Updated stream ownership and cleanup, exposed stream data and control helpers, and removed custom HTTP/3 header methods now supported by `aioquic`.
- Updated the configured RCP version to 2.0 as part of WebTransport CONNECT support.
- Added CI quality checks, package compatibility and build workflows, and lint and spelling configuration. 
- Updated package manifests and added project logo assets.

## 0.1.2

- Set the package version to `0.1.2` in package metadata and `riven.__version__`.
- Updated package configuration and ignored generated files, and added `riven/loop/__init__.py`.
