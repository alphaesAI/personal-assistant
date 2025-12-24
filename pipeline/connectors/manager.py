"""Manager class for handling multiple connectors."""

import os
import yaml
from typing import Any, Dict, Optional
from .base import BaseConnector
from .factory import ConnectorFactory


class ConnectorManager:
    """Manager class for handling multiple connectors."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize ConnectorManager with optional config path.
        
        Args:
            config_path: Path to YAML config file. If None, uses 'connector_config.yml'
        """
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), 'connector_config.yml')
        
        self.config_path = config_path
        self._connectors: Dict[str, BaseConnector] = {}
        self._config: Optional[Dict[str, Any]] = None
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if self._config is None:
            try:
                with open(self.config_path, 'r') as file:
                    self._config = yaml.safe_load(file) or {}
            except FileNotFoundError:
                self._config = {}
            except Exception as e:
                raise ValueError(f"Failed to load config file {self.config_path}: {e}")
        
        return self._config
    
    def get_connector(self, name: str) -> BaseConnector:
        """Get or create a connector by name."""
        if name not in self._connectors:
            self._connectors[name] = self._create_connector(name)
        return self._connectors[name]
    
    def _create_connector(self, name: str) -> BaseConnector:
        """Create a connector from configuration."""
        config = self.load_config()
        connectors = config.get('connectors', {})
        
        if name not in connectors:
            raise ValueError(f"Connector '{name}' not found in config")
        
        connector_config = connectors[name]
        connector_type = connector_config.get('type')
        
        if not connector_type:
            raise ValueError(f"Connector '{name}' missing 'type' field")
        
        return ConnectorFactory.create(connector_type, name, connector_config)
    
    def list_connectors(self) -> list[str]:
        """List all configured connector names."""
        config = self.load_config()
        return list(config.get('connectors', {}).keys())
    
    def connect_all(self) -> None:
        """Connect all configured connectors."""
        for name in self.list_connectors():
            connector = self.get_connector(name)
            if not connector.is_connected():
                connector.connect()
    
    def disconnect_all(self) -> None:
        """Disconnect all connectors."""
        for connector in self._connectors.values():
            try:
                if connector.is_connected():
                    connector.disconnect()
            except Exception:
                pass
        
        self._connectors.clear()
