"""
Factory for creating loader instances - Backend selection factory
"""

from typing import Any, Dict
from .ingestor import BaseIngestor, SingleIngestor, BulkIngestor


class LoaderFactory:
    """
    Factory class for creating loader instances.
    
    Responsibility: Choose backend based on config and return correct ingestor instance.
    """

    @staticmethod
    def create_ingestor(config: Dict[str, Any], use_bulk: bool = True) -> BaseIngestor:
        """
        Create appropriate ingestor based on backend configuration.
        
        Args:
            config: Backend configuration containing type and settings
            use_bulk: Whether to use bulk ingestor (default: True)
            
        Returns:
            Ingestor instance for the specified backend
            
        Raises:
            ValueError: If unsupported backend type is specified
        """
        backend_type = config.get('type', 'elasticsearch').lower()
        
        if backend_type == 'elasticsearch':
            # Choose between bulk and single based on config and parameter
            if use_bulk and config.get('bulk_enabled', True):
                return BulkIngestor(config)
            else:
                return SingleIngestor(config)
        elif backend_type == 'opensearch':
            # Future implementation for OpenSearch
            raise NotImplementedError("OpenSearch backend not yet implemented")
        else:
            raise ValueError(f"Unsupported backend type: {backend_type}")
    
    @staticmethod
    def get_supported_backends() -> list:
        """
        Get list of supported backend types.
        
        Returns:
            List of supported backend names
        """
        return ['elasticsearch', 'opensearch']
