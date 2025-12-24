# pipeline/extractors/factory.py

from typing import Any, Dict
from .registry import ExtractorRegistry
from .base import BaseExtractor


class ExtractorFactory:
    """Factory class for creating extractor instances."""
    
    @staticmethod
    def create(extractor_type: str, connector, config: Dict[str, Any]) -> BaseExtractor:
        """Create an extractor instance using registry.
        
        Args:
            extractor_type: Type of extractor to create.
            connector: Connector instance.
            config: Configuration for the extractor.
            
        Returns:
            Extractor instance.
            
        Raises:
            ValueError: If extractor type is not registered.
        """
        extractor_class = ExtractorRegistry.get(extractor_type)
        return extractor_class(name=extractor_type, connector=connector, config=config)
