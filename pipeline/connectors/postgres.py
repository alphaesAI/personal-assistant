import psycopg2
import os
from psycopg2 import sql
from typing import Any, Dict, Optional
from .base import BaseConnector
from .registry import ConnectorRegistry


class PostgresConnector(BaseConnector):
    """PostgreSQL connector using YAML configuration."""
    
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self._cursor = None
    
    def connect(self) -> None:
        """Establish PostgreSQL connection using YAML configuration."""
        try:
            # Get connection configuration from YAML
            connection_config = self.config.get('connection', {})
            
            # Build connection parameters
            conn_params = {
                'host': connection_config.get('host', os.getenv('POSTGRES_HOST', 'localhost')),
                'port': connection_config.get('port', int(os.getenv('POSTGRES_PORT', '5432'))),
                'database': connection_config.get('database', os.getenv('POSTGRES_DATABASE')),
                'user': connection_config.get('user', os.getenv('POSTGRES_USER')),
                'password': connection_config.get('password', os.getenv('POSTGRES_PASSWORD'))
            }
            
            # Remove None values
            conn_params = {k: v for k, v in conn_params.items() if v is not None}
            
            # Add any additional config
            connection_params = connection_config.get('connection_params', {})
            conn_params.update(connection_params)
            
            self._connection = psycopg2.connect(**conn_params)
            self._cursor = self._connection.cursor()
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")
    
    def disconnect(self) -> None:
        """Close PostgreSQL connection."""
        try:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            if self._connection:
                self._connection.close()
                self._connection = None
        except Exception as e:
            raise ConnectionError(f"Error disconnecting from PostgreSQL: {e}")
    
    def get_connection(self):
        """Return the live database connection."""
        return self._connection
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> list:
        """Execute a query and return results."""
        if not self._connection or not self._cursor:
            raise ConnectionError("Not connected to PostgreSQL")
        
        try:
            self._cursor.execute(query, params)
            if self._cursor.description:
                # Get column names from cursor description
                columns = [desc[0] for desc in self._cursor.description]
                rows = self._cursor.fetchall()
                # Convert to list of dictionaries
                return [dict(zip(columns, row)) for row in rows]
            self._connection.commit()
            return []
        except Exception as e:
            self._connection.rollback()
            raise e


# Register the connector
ConnectorRegistry.register("postgres", PostgresConnector)