from dataclasses import dataclass

@dataclass(slots=True)
class ConnectionInfo:
    stream_id: int
    client: tuple[str, int] # client IP:PORT
    server: tuple[str, int] # server IP:PORT