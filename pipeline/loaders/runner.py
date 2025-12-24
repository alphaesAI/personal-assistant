"""
Loader Runner - Orchestrator for loading process
"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import yaml

from .embeddings import EmbeddingAligner
from .ingestor import BaseIngestor
from .factory import LoaderFactory


@dataclass
class Chunk:
    """Data structure for chunk information"""
    source_id: str
    chunk_id: str
    text: str
    metadata: List[str]


class LoaderRunner:
    """
    Orchestrator for the loading process.
    
    Responsibilities:
    - Read transformed JSON files
    - Unpack data into internal Python structures
    - Read loader configuration from YAML
    - Coordinate embedding and ingestion flow
    """
    
    def __init__(self, config_path: Optional[str] = None):
        self.data_dir = Path(__file__).parent.parent.parent / 'data'
        self.config_path = config_path or (Path(__file__).parent / 'loader.yml')
        self.config = self._load_config()
        self.embedding_aligner = None
        self.ingestor = None
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _read_transformed_file(self, transformer_name: str) -> List[Chunk]:
        """Read transformed JSON file and convert to Chunk objects."""
        file_path = self.data_dir / 'transformed' / f'{transformer_name}.json'
        
        if not file_path.exists():
            raise FileNotFoundError(f"Transformed file not found: {file_path}")
        
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        chunks = []
        for item in data:
            # Convert list format back to tuple-like structure
            chunk_id, text, metadata = item
            
            # Extract source_id from chunk_id (before _chunk_ if present)
            chunk_id_str = str(chunk_id)
            if '_chunk_' in chunk_id_str:
                source_id = chunk_id_str.split('_chunk_')[0]
            else:
                source_id = chunk_id_str
            
            chunks.append(Chunk(
                source_id=source_id,
                chunk_id=str(chunk_id),
                text=text,
                metadata=metadata
            ))
        
        return chunks
    
    def _initialize_components(self):
        """Initialize embedding aligner and ingestor based on config."""
        # Initialize embedder if embedding is enabled
        if self.config.get('embeddings', {}).get('enabled', False):
            self.embedding_aligner = EmbeddingAligner(self.config['embeddings'])
        
        # Initialize ingestor
        backend_config = self.config.get('backend', {})
        self.ingestor = LoaderFactory.create_ingestor(backend_config)
    
    def _process_with_embedding(self, chunks: List[Chunk]) -> List[Dict[str, Any]]:
        """Process chunks with embedding alignment."""
        if not self.embedding_aligner:
            raise ValueError("Embedding aligner not initialized")
        
        return self.embedding_aligner.align_and_embed(chunks)
    
    def _process_without_embedding(self, chunks: List[Chunk]) -> List[Dict[str, Any]]:
        """Process chunks without embedding (direct ingestion)."""
        aligned_records = []
        for chunk in chunks:
            aligned_records.append({
                'source_id': chunk.source_id,
                'chunk_id': chunk.chunk_id,
                'text': chunk.text,
                'metadata': chunk.metadata,
                'vector': None  # No vector for direct ingestion
            })
        return aligned_records
    
    def run_loader(self, transformer_name: str) -> str:
        """
        Run the complete loading process for a transformer.
        
        Args:
            transformer_name: Name of the transformer to load
            
        Returns:
            Status message
        """
        try:
            # Read transformed data
            chunks = self._read_transformed_file(transformer_name)
            print(f"Loaded {len(chunks)} chunks from {transformer_name}")
            
            # Initialize components
            self._initialize_components()
            
            # Process based on embedding configuration
            if self.config.get('embeddings', {}).get('enabled', False):
                aligned_records = self._process_with_embedding(chunks)
                print(f"Generated embeddings for {len(aligned_records)} chunks")
            else:
                aligned_records = self._process_without_embedding(chunks)
                print(f"Prepared {len(aligned_records)} chunks for direct ingestion")
            
            # Ingest to backend
            self.ingestor.ingest(aligned_records)
            print(f"Successfully ingested {len(aligned_records)} records")
            
            return f"Successfully loaded {transformer_name}"
            
        except Exception as e:
            error_msg = f"Failed to load {transformer_name}: {str(e)}"
            print(error_msg)
            return error_msg
    
    def run_all_loaders(self) -> Dict[str, str]:
        """
        Run loaders for all available transformers.
        
        Returns:
            Dictionary mapping transformer names to status messages
        """
        results = {}
        
        # Find all transformed files
        transformed_dir = self.data_dir / 'transformed'
        if not transformed_dir.exists():
            return {"error": "No transformed directory found"}
        
        # Process each JSON file in transformed directory
        for file_path in transformed_dir.glob('*.json'):
            transformer_name = file_path.stem
            results[transformer_name] = self.run_loader(transformer_name)
        
        return results
