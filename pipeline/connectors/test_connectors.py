#!/usr/bin/env python3
"""Test script for connectors."""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from .manager import ConnectorManager

def test_connectors():
    """Test all configured connectors."""
    # Use absolute path to config file
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'connector_config.yml')
    manager = ConnectorManager(config_path)
    
    print("Available connectors:", manager.list_connectors())
    
    # Test connections
    results = manager.test_all_connections()
    for name, success in results.items():
        status = "✓" if success else "✗"
        print(f"{status} {name}: {'Connected' if success else 'Failed'}")

if __name__ == "__main__":
    test_connectors()


# from elasticsearch import Elasticsearch

# es = Elasticsearch(["http://localhost:9200"])
# print(es.info())
