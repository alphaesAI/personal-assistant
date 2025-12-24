# pipeline/extractors/registry.py
from typing import Dict, Type
from .base import BaseExtractor


class ExtractorRegistry:
    """Registry for extractor classes."""
    
    _extractors: Dict[str, Type[BaseExtractor]] = {}
    
    @classmethod
    def register(cls, extractor_type: str, extractor_class: Type[BaseExtractor]) -> None:
        """Register an extractor class."""
        cls._extractors[extractor_type] = extractor_class
    
    @classmethod
    def get(cls, extractor_type: str) -> Type[BaseExtractor]:
        """Get an extractor class by type."""
        if extractor_type not in cls._extractors:
            raise ValueError(f"Extractor type '{extractor_type}' not registered")
        return cls._extractors[extractor_type]
    
    @classmethod
    def list_extractors(cls) -> list[str]:
        """List all registered extractor types."""
        return list(cls._extractors.keys())
