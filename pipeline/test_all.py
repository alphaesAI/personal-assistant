#!/usr/bin/env python3
"""
Complete pipeline test: extract, transform, and load data sequentially.
"""

import sys
import os

# Add project root to path to ensure correct module resolution
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from pipeline.extractors.manager import ExtractorManager
from pipeline.transformers.runner import TransformerRunner
from pipeline.loaders.runner import LoaderRunner

def run_extraction():
    """Run data extraction phase."""
    print("=== Phase 1: Data Extraction ===")
    try:
        extractor_manager = ExtractorManager()
        
        print("Running all extractors...")
        extractor_manager.run_all_extractions()
        print("   Completed")
        
        print("Extraction completed successfully.")
        return True
    except Exception as e:
        print(f"Extraction failed: {str(e)}")
        raise

def run_transformation():
    """Run data transformation phase."""
    print("\n=== Phase 2: Data Transformation ===")
    try:
        transformer_runner = TransformerRunner()
        
        print("Running all transformers...")
        output_files = transformer_runner.run_all_transformers()
        for name, path in output_files.items():
            print(f"   - Transformed '{name}' to '{path}'")
            
        print("Transformation completed successfully.")
        return True
    except Exception as e:
        print(f"Transformation failed: {str(e)}")
        raise

def run_loading():
    """Run data loading phase."""
    print("\n=== Phase 3: Data Loading ===")
    try:
        loader_runner = LoaderRunner()
        
        print("Running all loaders...")
        loader_runner.run_all_loaders()
        
        print("Loading completed successfully.")
        return True
    except Exception as e:
        print(f"Loading failed: {str(e)}")
        raise

def main():
    """Run the complete pipeline."""
    print("Starting complete pipeline test")
    print("=" * 50)
    
    try:
        run_extraction()
        run_transformation()
        run_loading()
        
        print("\n" + "=" * 50)
        print("All pipeline stages completed successfully.")
        sys.exit(0)
    except Exception as e:
        print("\n" + "=" * 50)
        print(f"Pipeline failed.")
        sys.exit(1)

if __name__ == "__main__":
    # Change to the project root directory to ensure correct file path resolution
    os.chdir(project_root)
    main()