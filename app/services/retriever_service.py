"""
Retriever Service using existing Elasticsearch connectors
"""

import logging
import os
import sys
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class RetrieverService:
    """Service for Elasticsearch search using existing connectors"""
    
    def __init__(self):
        self.es_connector = None
        self.config = self._load_config()
        self._initialize_connector()
        self._embedding_service = None
    
    def _load_config(self):
        """Load configuration"""
        import os
        import yaml
        config_path = os.environ.get("CONFIG", "app/config/app.yml")
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def _initialize_connector(self):
        """Initialize Elasticsearch connector from existing pipeline"""
        try:
            # Import existing connector
            import sys
            # Add pipeline paths relative to project root
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            pipeline_path = os.path.join(project_root, 'pipeline')
            connectors_path = os.path.join(pipeline_path, 'connectors')
            
            if connectors_path not in sys.path:
                sys.path.insert(0, connectors_path)
            if pipeline_path not in sys.path:
                sys.path.insert(0, pipeline_path)
                
            from elasticsearch import ElasticsearchConnector
            
            # Load config
            config_path = os.environ.get("CONFIG")
            if not config_path:
                logger.warning("CONFIG environment variable not set")
                return
            
            import sys
            # Add txtai submodule path to Python path
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            txtai_path = os.path.join(project_root, 'txtai', 'src', 'python')
            if txtai_path not in sys.path:
                sys.path.insert(0, txtai_path)
            from txtai.api.application import Application
            
            config = Application.read(config_path)
            es_config = config.get("elasticsearch", {})
            
            if not es_config:
                logger.warning("No Elasticsearch configuration found")
                return
            
            # Create connector
            self.es_connector = ElasticsearchConnector("retriever", es_config)
            self.es_connector.connect()
            logger.info("Elasticsearch connector initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Elasticsearch connector: {e}")
    
    def _get_embedding_service(self):
        """Get or create embedding service using txtai's EmbeddingAligner"""
        if self._embedding_service is None:
            try:
                # Import EmbeddingAligner from pipeline
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                pipeline_path = os.path.join(project_root, 'pipeline')
                if pipeline_path not in sys.path:
                    sys.path.insert(0, pipeline_path)
                from loaders.embeddings import EmbeddingAligner
                
                # Get embeddings config
                embeddings_config = self.config.get("embeddings", {})
                if not embeddings_config:
                    # Default config
                    embeddings_config = {
                        "path": "sentence-transformers/all-MiniLM-L6-v2",
                        "content": False,
                        "scoring": None,
                        "backend": "numpy"
                    }
                
                self._embedding_service = EmbeddingAligner(embeddings_config)
                logger.info("Embedding service initialized")
            except Exception as e:
                logger.error(f"Failed to initialize embedding service: {e}")
                raise
        
        return self._embedding_service
    
    def search(self, query: str, index: str = None, size: int = 20) -> List[Dict[str, Any]]:
        """Search documents in Elasticsearch using vector similarity"""
        if not self.es_connector:
            logger.error("Elasticsearch connector not available")
            return []
        
        try:
            # Use default index if not provided
            if not index:
                index = self.config.get("elasticsearch", {}).get("index_name", "healthai_vectors")
            
            # Get the embedding for the query using txtai's EmbeddingAligner
            embedding_service = self._get_embedding_service()
            vectors = embedding_service.generate_vectors([query])
            query_embedding = vectors[0]
            logger.info(f"Generated embedding vector of dimension: {len(query_embedding)}")
            
            # Build KNN search query
            search_body = {
                "knn": {
                    "field": "vector",  # Field containing the document embeddings (corrected from 'embedding' to 'vector')
                    "query_vector": query_embedding,
                    "k": size,
                    "num_candidates": 100  # Number of candidates to consider
                },
                "_source": ["text", "metadata"]  # Fields to return
            }
            logger.info(f"Executing KNN search with vector dimension {len(query_embedding)}")
            
            # Execute search
            response = self.es_connector.search(index, search_body)
            logger.info(f"KNN search executed successfully, found {len(response.get('hits', {}).get('hits', []))} results")
            
            # Format results
            results = []
            for hit in response.get('hits', {}).get('hits', []):
                source = hit.get('_source', {})
                results.append({
                    'chunk_id': hit.get('_id'),
                    'text': source.get('text', ''),
                    'metadata': source.get('metadata', {}),
                    'score': hit.get('_score')
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            # Fall back to keyword search if vector search fails
            logger.info("Falling back to keyword search")
            return self._keyword_search(query, index, size)
    
    def _keyword_search(self, query: str, index: str, size: int) -> List[Dict[str, Any]]:
        """Fallback keyword-based search"""
        try:
            search_body = {
                "query": {
                    "match": {
                        "text": query
                    }
                },
                "size": size
            }
            response = self.es_connector.search(index, search_body)
            
            results = []
            for hit in response.get('hits', {}).get('hits', []):
                source = hit.get('_source', {})
                results.append({
                    'chunk_id': hit.get('_id'),
                    'text': source.get('text', ''),
                    'metadata': source.get('metadata', {}),
                    'score': hit.get('_score')
                })
            return results
        except Exception as e:
            logger.error(f"Keyword search also failed: {e}")
            return []
    
    def is_available(self) -> bool:
        """Check if the retriever service is available"""
        return self.es_connector is not None and self.es_connector.test_connection()

# Global instance
_retriever_service = None

def get_retriever_service() -> RetrieverService:
    """Get global retriever service instance"""
    global _retriever_service
    if _retriever_service is None:
        _retriever_service = RetrieverService()
    return _retriever_service