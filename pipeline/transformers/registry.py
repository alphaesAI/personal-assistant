# pipeline/transformers/registry.py
from typing import Dict, Type
from .base import BaseTransformer


class TransformerRegistry:
    """Registry for transformer classes."""
    
    _transformers: Dict[str, Type[BaseTransformer]] = {}
    
    @classmethod
    def register(cls, name: str, transformer_class: Type[BaseTransformer]):
        """Register a transformer class."""
        cls._transformers[name] = transformer_class
    
    @classmethod
    def get(cls, name: str) -> Type[BaseTransformer]:
        """Get a transformer class by name."""
        if name not in cls._transformers:
            raise ValueError(f"Transformer '{name}' not registered")
        return cls._transformers[name]
    
    @classmethod
    def list_transformers(cls) -> list:
        """List all registered transformers."""
        return list(cls._transformers.keys())
