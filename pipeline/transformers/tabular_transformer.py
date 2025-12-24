# pipeline/transformers/tabular_transformer.py
from typing import Iterator, Tuple, Any, Dict
from pathlib import Path
import json
from .base import BaseTransformer
from txtai.pipeline.data.tabular import Tabular


class TabularTransformer(BaseTransformer):
    """
    Transformer for structured/tabular data using txtai Tabular pipeline.
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.id_column = config.get('id_column', 'id')
        self.text_columns = config.get('text_columns', [])
        
        # Initialize txtai Tabular pipeline
        self.tabular = Tabular(
            idcolumn=self.id_column,
            textcolumns=self.text_columns,
            content=False
        )

    def transform(self) -> Iterator[Tuple[str, str, list]]:
        """
        Transform tabular data into (id, text, tags) tuples.
        
        Returns:
            Iterator of (id, text, tags) tuples
        """
        # Read extractor output
        data_dir = Path(__file__).parent.parent.parent / 'data'
        file_path = data_dir / 'extractors' / f'{self.config.get("source", "postgres")}.json'
        
        if not file_path.exists():
            return
        
        with open(file_path, 'r') as f:
            records = json.load(f)
        
        # Use txtai Tabular pipeline to process data and yield results directly
        results = self.tabular(records)
        
        # Tabular already returns (id, text, None) tuples, just add tags
        for result in results:
            if isinstance(result, tuple) and len(result) == 3:
                record_id, text, _ = result
                tags = [f"source:{self.config.get('source', 'postgres')}"]
                yield (record_id, text, tags)
