#!/usr/bin/env python3
"""
Test transformers execution.
"""

from pipeline.transformers.runner import TransformerRunner

def main():
    print("=== Testing Transformers ===\n")
    
    # Initialize runner (reads YAML configs automatically)
    runner = TransformerRunner()
    
    # Run all transformers and write output files
    print("Running all transformers:")
    output_files = runner.run_all_transformers()
    
    for transformer_name, output_path in output_files.items():
        print(f"  {transformer_name}: {output_path}")
    
    print("\n=== Test Complete ===")

if __name__ == "__main__":
    main()
