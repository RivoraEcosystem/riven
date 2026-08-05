from dataclasses import dataclass

# Server config
@dataclass(slots=True)
class RivenConfig:
    max_header_size_kb: int = 0
    max_queue_size: int = 0
    root_path: str = ""

    @property
    def max_header_size(self) -> int:
        return self.max_header_size_kb * 1024