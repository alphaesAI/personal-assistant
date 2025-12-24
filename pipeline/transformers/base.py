# pipeline/transformers/base.py
from abc import ABC, abstractmethod
from typing import Iterator, Tuple, Any, Dict


class BaseTransformer(ABC):
    """
    Abstract base class for data transformers.
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config

    @abstractmethod
    def transform(self) -> Iterator[Tuple[str, str, list]]:
        """
        Transform data into (id, text, tags) tuples.
        
        Returns:
            Iterator of (id, text, tags) tuples
        """
        pass
