# pipeline/transformers/manager.py
from typing import Iterator, Tuple, Dict, Any
from .factory import TransformerFactory


class TransformerManager:
    """Manager class for handling multiple transformers."""
    
    def __init__(self, config_path: str = None):
        self.factory = TransformerFactory(config_path)
        self._transformers: Dict[str, Any] = {}
    
    def get_transformer(self, name: str):
        """Get or create a transformer by name."""
        if name not in self._transformers:
            self._transformers[name] = self.factory.create(name)
        return self._transformers[name]
    
    def list_transformers(self) -> list:
        """List all configured transformer names."""
        config = self.factory.load_config()
        return list(config['transformers'].keys())
    
    def run_transformation(self, name: str) -> Iterator[Tuple[str, str, list]]:
        """Run transformation for a specific transformer."""
        transformer = self.get_transformer(name)
        yield from transformer.transform()
    
    def run_all_transformations(self) -> Iterator[Tuple[str, str, list]]:
        """Run all configured transformations."""
        for name in self.list_transformers():
            yield from self.run_transformation(name)
