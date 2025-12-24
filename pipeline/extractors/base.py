# pipeline/extractors/base.py
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator


class BaseExtractor(ABC):
    def __init__(self, name: str, connector, config: Dict[str, Any]):
        self.name = name
        self.connector = connector
        self.config = config

    @abstractmethod
    def extract(self) -> Iterator[Dict[str, Any]]:
        pass
