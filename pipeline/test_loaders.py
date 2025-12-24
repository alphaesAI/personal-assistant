#!/usr/bin/env python3
"""
Test loaders execution.
"""

from pipeline.loaders.runner import LoaderRunner

def main():
    print("=== Testing Loaders ===\n")
    
    # Initialize runner (reads YAML configs automatically)
    runner = LoaderRunner()
    
    # Run all loaders
    print("Running all loaders:")
    results = runner.run_all_loaders()
    
    for loader_name, success in results.items():
        status = "✓" if success else "✗"
        print(f"  {status} {loader_name}")
    
    print("\n=== Test Complete ===")

if __name__ == "__main__":
    main()
