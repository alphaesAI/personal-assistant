"""Factory for creating connector instances."""

from typing import Any, Dict
from .base import BaseConnector
from .registry import ConnectorRegistry


class ConnectorFactory:
    """Factory class for creating connector instances."""
    
    @staticmethod
    def create(connector_type: str, name: str, config: Dict[str, Any]) -> BaseConnector:
        """Create a connector instance using registry.
        
        Args:
            connector_type: Type of connector to create.
            name: Name for the connector instance.
            config: Configuration for the connector.
            
        Returns:
            Connector instance.
            
        Raises:
            ValueError: If connector type is not registered.
        """
        connector_class = ConnectorRegistry.get(connector_type)
        return connector_class(name=name, config=config)