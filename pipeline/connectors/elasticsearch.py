from elasticsearch import Elasticsearch
import os
from typing import Any, Dict, Optional, List
from .base import BaseConnector
from .registry import ConnectorRegistry


class ElasticsearchConnector(BaseConnector):
    """Elasticsearch connector using YAML configuration."""
    
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self._client = None
        self._connection_info = {}
    
    def connect(self) -> None:
        """Establish Elasticsearch connection using YAML configuration."""
        try:
            # Get hosts configuration from YAML
            hosts = self.config.get('connection', {}).get('hosts', [])
            if not hosts:
                # Fallback to environment variable or default
                default_hosts = os.getenv('ELASTICSEARCH_HOSTS', 'http://localhost:9200')
                hosts = default_hosts.split(',') if isinstance(default_hosts, str) else [default_hosts]
            
            conn_params = {
                'hosts': hosts if isinstance(hosts, list) else [hosts]
            }
            
            # Add any additional config
            connection_params = self.config.get('connection', {}).get('connection_params', {})
            conn_params.update(connection_params)
            
            # Remove None values
            conn_params = {k: v for k, v in conn_params.items() if v is not None}
            
            # Create client with version compatibility
            try:
                self._client = Elasticsearch(**conn_params)
            except TypeError:
                # Fallback for older elasticsearch versions
                if 'use_ssl' in conn_params:
                    conn_params.pop('use_ssl')
                if 'verify_certs' in conn_params:
                    conn_params.pop('verify_certs')
                self._client = Elasticsearch(**conn_params)
            
            # Test connection and store info
            info = self._client.info()
            self._connection_info = {
                'cluster_name': info.get('cluster_name'),
                'version': info.get('version', {}).get('number'),
                'hosts': conn_params.get('hosts', [])
            }
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Elasticsearch: {e}")
    
    def disconnect(self) -> None:
        """Close Elasticsearch connection."""
        try:
            if self._client:
                self._client.close()
                self._client = None
                self._connection_info = {}
        except Exception as e:
            raise ConnectionError(f"Error disconnecting from Elasticsearch: {e}")
    
    def test_connection(self) -> bool:
        """Test Elasticsearch connection."""
        try:
            if not self._client:
                return False
            
            # Simple cluster health check
            health = self._client.cluster.health()
            return health.get('status') in ['green', 'yellow']
        except Exception:
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get Elasticsearch connection information."""
        if not self._client:
            return {"status": "disconnected"}
        
        try:
            health = self._client.cluster.health()
            info = self._client.info()
            
            return {
                "status": "connected",
                "cluster_name": info.get('cluster_name'),
                "version": info.get('version', {}).get('number'),
                "hosts": self._connection_info.get('hosts', []),
                "cluster_health": health.get('status'),
                "number_of_nodes": health.get('number_of_nodes')
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}
    
    def search(self, index: str, query: Dict[str, Any]) -> Dict[str, Any]:
        """Search documents in Elasticsearch."""
        if not self._client:
            raise ConnectionError("Not connected to Elasticsearch")
        
        try:
            return self._client.search(
                index=index,
                body=query,
            )
        except Exception as e:
            raise e
    
    def index_document(self, index: str, doc_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """Index a document in Elasticsearch."""
        if not self._client:
            raise ConnectionError("Not connected to Elasticsearch")
        
        try:
            return self._client.index(
                index=index,
                id=doc_id,
                body=body
            )
        except Exception as e:
            raise e
    
    def get_document(self, index: str, doc_id: str) -> Dict[str, Any]:
        """Get a document by ID from Elasticsearch."""
        if not self._client:
            raise ConnectionError("Not connected to Elasticsearch")
        
        try:
            return self._client.get(
                index=index,
                id=doc_id
            )
        except Exception as e:
            raise e
    
    def delete_document(self, index: str, doc_id: str) -> Dict[str, Any]:
        """Delete a document by ID from Elasticsearch."""
        if not self._client:
            raise ConnectionError("Not connected to Elasticsearch")
        
        try:
            return self._client.delete(
                index=index,
                id=doc_id
            )
        except Exception as e:
            raise e
    
    def bulk_index(self, actions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Bulk index documents in Elasticsearch."""
        if not self._client:
            raise ConnectionError("Not connected to Elasticsearch")
        
        try:
            from elasticsearch.helpers import bulk
            success, failed = bulk(self._client, actions)
            return {
                "success_count": success,
                "failed_count": len(failed),
                "failed_items": failed
            }
        except Exception as e:
            raise e
        
    def index_exists(self, index_name: str) -> bool:
        """Check if an index exists in Elasticsearch.
        
        Args:
            index_name: Name of the index to check
            
        Returns:
            bool: True if index exists, False otherwise
        """
        try:
            return self._client.indices.exists(index=index_name)
        except Exception as e:
            print(f"Error checking if index exists: {e}")
            return False

    def create_index(self, index_name: str, body: Dict[str, Any] = None) -> bool:
        """Create an index with the specified mappings.
        
        Args:
            index_name: Name of the index to create
            body: Index configuration and mappings
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self._client.indices.create(index=index_name, body=body)
        except Exception as e:
            print(f"Error creating index: {e}")
            return False
    
    def bulk(self, actions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Bulk operations in Elasticsearch (alias for bulk_index)."""
        return self.bulk_index(actions)
    
    def count(self, index: str = None, body: Dict[str, Any] = None) -> int:
        """Count documents in an index.
        
        Args:
            index: Index name
            body: Query body
            
        Returns:
            Number of documents
        """
        try:
            if index:
                response = self._client.count(index=index, body=body) if body else self._client.count(index=index)
            else:
                response = self._client.count(body=body) if body else self._client.count()
            return response.get('count', 0)
        except Exception as e:
            print(f"Error counting documents: {e}")
            return 0
    
    def delete_by_query(self, index: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """Delete documents by query.
        
        Args:
            index: Index name
            body: Query body
            
        Returns:
            Delete response
        """
        try:
            return self._client.delete_by_query(index=index, body=body)
        except Exception as e:
            print(f"Error deleting by query: {e}")
            return {}
    
    def close(self) -> None:
        """Close the Elasticsearch client."""
        self.disconnect()


# Register the connector
ConnectorRegistry.register("elasticsearch", ElasticsearchConnector)