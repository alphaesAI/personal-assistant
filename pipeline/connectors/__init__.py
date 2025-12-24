"""
Connector module for various data sources and APIs.

This module provides a unified interface for connecting to different services
including PostgreSQL, Elasticsearch, and Gmail through a factory pattern.
"""

from .base import BaseConnector
from .postgres import PostgresConnector
from .elasticsearch import ElasticsearchConnector
from .gmail import GmailConnector
from .factory import ConnectorFactory
from .registry import *  # Register all connectors
from .manager import ConnectorManager

__all__ = [
    'BaseConnector',
    'PostgresConnector',
    'ElasticsearchConnector',
    'GmailConnector',
    'ConnectorFactory',
    'ConnectorManager'
]

__version__ = '1.0.0'