# pipeline/transformers/runner.py
import json
from pathlib import Path
from typing import Iterator, Tuple, Dict, Any
from .factory import TransformerFactory


class TransformerRunner:
    """
    Runner that receives input, collects chunks, and writes them to files.
    """

    def __init__(self, config_path: str = None):
        self.factory = TransformerFactory(config_path)
        self.data_dir = Path(__file__).parent.parent.parent / 'data'

    def run_transformer(self, transformer_name: str, output_file: str = None) -> str:
        """
        Run a specific transformer and write chunks to file.
        
        Args:
            transformer_name: Name of the transformer to run
            output_file: Output file name (optional, defaults to transformer name)
            
        Returns:
            Path to the output file
        """
        transformer = self.factory.create(transformer_name)
        
        if output_file is None:
            output_file = f"transformed/{transformer_name}.json"
        
        # Collect all chunks as tuples
        chunks = list(transformer.transform())  # Directly collect tuples
        
        # Write to file as JSON (tuples become lists during serialization)
        output_path = self.data_dir / output_file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(chunks, f, indent=2)
        
        return str(output_path)

    def run_all_transformers(self) -> Dict[str, str]:
        """
        Run all configured transformers and write chunks to files.
        
        Returns:
            Dictionary mapping transformer names to output file paths
        """
        config = self.factory.load_config()
        output_files = {}
        
        for transformer_name in config['transformers']:
            output_path = self.run_transformer(transformer_name)
            output_files[transformer_name] = output_path
        
        return output_files

    def collect_chunks(self, transformer_name: str) -> Iterator[Tuple[str, str, list]]:
        """
        Collect chunks from a transformer without writing to file.
        
        Args:
            transformer_name: Name of the transformer
            
        Returns:
            Iterator of (id, text, tags) tuples
        """
        transformer = self.factory.create(transformer_name)
        yield from transformer.transform()
