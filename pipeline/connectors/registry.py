"""Connector registration module."""

from typing import Dict, Type
from .base import BaseConnector


class ConnectorRegistry:
    """Registry for connector classes."""
    
    _connectors: Dict[str, Type[BaseConnector]] = {}
    
    @classmethod
    def register(cls, connector_type: str, connector_class: Type[BaseConnector]) -> None:
        """Register a connector class."""
        cls._connectors[connector_type] = connector_class
    
    @classmethod
    def get(cls, connector_type: str) -> Type[BaseConnector]:
        """Get a connector class by type."""
        if connector_type not in cls._connectors:
            raise ValueError(f"Connector type '{connector_type}' not registered")
        return cls._connectors[connector_type]
    
    @classmethod
    def list_connectors(cls) -> list[str]:
        """List all registered connector types."""
        return list(cls._connectors.keys())
