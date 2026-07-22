from dataclasses import dataclass

# Server config
@dataclass(slots=True)
class RivenConfig:
    max_header_size = 16 * 1024
    max_queue_size = 10