from typing import Any


class RivenException(Exception):
    """Base exception for all Riven exceptions."""


class InvalidEvent(RivenException):
    """Raised when an unsupported event type is received."""

    def __init__(self, event: Any):
        self.event = event

        if isinstance(event, str):
            event_name = event
        elif isinstance(event, type):
            event_name = event.__name__
        else:
            event_name = type(event).__name__

        super().__init__(f"Unsupported event type: {event_name}")


class DuplicatePseudoHeader(RivenException):
    """Raised when a pseudo-header appears more than once."""

    def __init__(self, header: str):
        self.header = header
        super().__init__(f"Duplicate pseudo-header: {header}")


class InvalidPseudoHeader(RivenException):
    """Raised when an unknown pseudo-header is received."""

    def __init__(self, header: str):
        self.header = header
        super().__init__(f"Invalid pseudo-header: {header}")


class MethodNotAllowed(RivenException):
    """Raised when the HTTP method is missing or unsupported."""

    def __init__(self, method: str | None = None):
        self.method = method

        if method is None:
            message = "Missing required pseudo-header ':method'"
        else:
            message = f"Unsupported HTTP method: {method}"

        super().__init__(message)


class InvalidScheme(RivenException):
    """Raised when :scheme is missing or invalid."""

    def __init__(self, scheme: str | None = None):
        self.scheme = scheme

        if scheme is None:
            message = "Missing required pseudo-header ':scheme'"
        else:
            message = f"Invalid HTTP scheme: {scheme}"

        super().__init__(message)


class InvalidAuthority(RivenException):
    """Raised when :authority is missing or invalid."""

    def __init__(self, authority: bytes | str | None = None):
        self.authority = authority

        if authority is None:
            message = "Missing required pseudo-header ':authority'"
        else:
            message = f"Invalid authority: {authority!r}"

        super().__init__(message)


class InvalidPath(RivenException):
    """Raised when :path is missing or invalid."""

    def __init__(self, path: bytes | str | None = None):
        self.path = path

        if path is None:
            message = "Missing required pseudo-header ':path'"
        else:
            message = f"Invalid request path: {path!r}"

        super().__init__(message)

class InvalidStreamContext(RivenException):
    """Raised when an invalid object is stored as a stream context."""
    def __init__(self, context:Any,expected:type):
        super().__init__(f"Unexpected StreamContext expected {expected.__name__}, got {type(context).__name__}")


class InvalidLifespanState(RivenException):
    """Raised when an invalid lifespan state transition occurs."""

    def __init__(
        self,
        state: Any,
        event: Any,
    ):
        self.state = state
        self.event = event

        state_name = getattr(state, "name", str(state))
        event_name = getattr(event, "name", str(event))

        super().__init__(
            f"Invalid lifespan transition: state={state_name}, event={event_name}"
        )


class LifespanAlreadyCompleted(RivenException):
    """Raised when a lifespan phase completes more than once."""

    def __init__(self, phase: str):
        self.phase = phase
        super().__init__(f"Lifespan {phase} has already completed.")


class LifespanNotStarted(RivenException):
    """Raised when a lifespan completion event is received before the phase starts."""

    def __init__(self, phase: str):
        self.phase = phase
        super().__init__(f"Lifespan {phase} has not been started.")

class InvalidEventField(RivenException):
    """Raised when an event field has an invalid type."""

    def __init__(
        self,
        field: str,
        got: type,
        expected: type,
    ) -> None:
        super().__init__(
            f"Invalid field {field!r} in event.\n"
            f"Expected: {expected.__name__}\n"
            f"Got: {got.__name__}"
        )

class InvalidStatusCode(RivenException):
    """Raised when an event sends invalid http status code"""

    def __init__(self, *args):
        super().__init__(*args)