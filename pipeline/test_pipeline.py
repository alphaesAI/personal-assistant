#!/usr/bin/env python3
"""
Extract actual data using existing configurations.
"""

from pipeline.connectors.manager import ConnectorManager
from pipeline.extractors.manager import ExtractorManager

def main():
    print("=== Data Extraction Demo ===\n")
    
    # Initialize managers
    connector_manager = ConnectorManager()
    extractor_manager = ExtractorManager()
    
    # 1. PostgreSQL Extraction
    print("1. PostgreSQL Extraction:")
    extractor_manager.run_extraction('postgres')
    print("   Completed")
    
    print()
    
    # 2. Gmail Extraction
    print("2. Gmail Extraction:")
    extractor_manager.run_extraction('gmail')
    print("   Completed")
    
    print()
    
    # 3. Elasticsearch Extraction
    print("3. Elasticsearch Extraction:")
    extractor_manager.run_extraction('elasticsearch')
    print("   Completed")
    
    print("\n=== Extraction Complete ===")

if __name__ == "__main__":
    main()
