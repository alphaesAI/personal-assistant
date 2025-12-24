# pipeline/transformers/factory.py
import yaml
from pathlib import Path
from typing import Dict, Any
from .registry import TransformerRegistry
from .tabular_transformer import TabularTransformer
from .document_transformer import DocumentTransformer


class TransformerFactory:
    """Factory for creating transformer instances from YAML configuration."""
    
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = Path(__file__).parent / 'transformer_config.yml'
        self.config_path = config_path
        self._config = None
    
    def load_config(self) -> Dict[str, Any]:
        """Load transformer configuration from YAML."""
        if self._config is None:
            with open(self.config_path, 'r') as f:
                self._config = yaml.safe_load(f)
        return self._config
    
    def create(self, name: str):
        """Create a transformer instance by name."""
        config = self.load_config()
        
        if name not in config['transformers']:
            raise ValueError(f"Transformer '{name}' not found in config")
        
        transformer_config = config['transformers'][name]
        transformer_type = transformer_config.get('type')
        
        if transformer_type == 'tabular':
            return TabularTransformer(name, transformer_config)
        elif transformer_type == 'textractor':
            return DocumentTransformer(name, transformer_config)
        else:
            raise ValueError(f"Unknown transformer type: {transformer_type}")
    
    def create_all(self) -> Dict[str, Any]:
        """Create all configured transformers."""
        config = self.load_config()
        transformers = {}
        
        for name in config['transformers']:
            transformers[name] = self.create(name)
        
        return transformers


# Register transformer types
TransformerRegistry.register('tabular', TabularTransformer)
TransformerRegistry.register('textractor', DocumentTransformer)
