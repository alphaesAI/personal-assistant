"""
Simple Ingestor Module - Elasticsearch indexing logic using connectors
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
import time
import logging
from datetime import datetime
import sys
from pathlib import Path

# Add pipeline path for connector imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from pipeline.connectors.manager import ConnectorManager


class BaseIngestor(ABC):
    """Abstract base class for ingestors."""
    
    @abstractmethod
    def ingest(self, records: List[Dict[str, Any]]) -> bool:
        """Ingest records into the backend."""
        pass


class BaseElasticsearchIngestor(BaseIngestor):
    """Base class for Elasticsearch ingestors with common functionality."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize using connector."""
        self.config = config
        self.connector_name = config.get('connector_name', 'elasticsearch')
        self.index_name = config.get('index_name', 'healthai_vectors')
        self.max_retries = config.get('max_retries', 3)
        self.logger = logging.getLogger(__name__)
        
        # Get connector from manager
        connector_manager = ConnectorManager()
        self.connector = connector_manager.get_connector(self.connector_name)
        if not self.connector:
            raise ConnectionError(f"Connector '{self.connector_name}' not found")
        
        # Connect to Elasticsearch
        self.connector.connect()
    
    def _create_document(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Create Elasticsearch document from record."""
        doc = {
            'source_id': record['source_id'],
            'chunk_id': record['chunk_id'],
            'text': record['text'],
            'metadata': record['metadata'],
            'ingested_at': datetime.utcnow().isoformat()
        }
        
        # Add vector if present
        if record.get('vector') is not None:
            doc['vector'] = record['vector']
        
        return doc


class SingleIngestor(BaseElasticsearchIngestor):
    """Simple single record ingestor for Elasticsearch."""
    
    def ingest(self, records: List[Dict[str, Any]]) -> bool:
        """Ingest records one by one."""
        if not records:
            self.logger.warning("No records to ingest")
            return True
        
        success_count = 0
        
        for record in records:
            if self._ingest_single(record):
                success_count += 1
        
        self.logger.info(f"Ingested {success_count}/{len(records)} records successfully")
        return success_count == len(records)
    
    def _ingest_single(self, record: Dict[str, Any]) -> bool:
        """Ingest a single record."""
        doc = self._create_document(record)
        
        for attempt in range(self.max_retries):
            try:
                response = self.connector.index_document(
                    index=self.index_name,
                    doc_id=record['chunk_id'],
                    body=doc
                )
                
                if response.get('result') in ['created', 'updated']:
                    self.logger.info(f"Successfully ingested record: {record['chunk_id']}")
                    return True
                    
            except Exception as e:
                if attempt == self.max_retries - 1:
                    self.logger.error(f"Failed to ingest record {record['chunk_id']}: {str(e)}")
                    return False
                time.sleep(self.max_retries * (attempt + 1))
        
        return False


class BulkIngestor(BaseElasticsearchIngestor):
    """Simple bulk ingestor for Elasticsearch."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize bulk ingestor using connector."""
        super().__init__(config)
        self.batch_size = config.get('batch_size', 100)
    
    def ingest(self, records: List[Dict[str, Any]]) -> bool:
        """Ingest records in batches."""
        if not records:
            self.logger.warning("No records to ingest")
            return True
        
        success_count = 0
        total_count = len(records)
        
        # Process in batches
        for i in range(0, total_count, self.batch_size):
            batch = records[i:i + self.batch_size]
            
            if self._ingest_batch(batch):
                success_count += len(batch)
            else:
                # Fall back to single ingest for failed batch
                self.logger.warning(f"Batch failed, falling back to single ingest")
                single_ingestor = SingleIngestor(self.config)
                if single_ingestor.ingest(batch):
                    success_count += len(batch)
        
        self.logger.info(f"Ingested {success_count}/{total_count} records successfully")
        return success_count == total_count
    
    def _ingest_batch(self, records: List[Dict[str, Any]]) -> bool:
        """Ingest a batch of records."""
        actions = []
        
        for record in records:
            doc = self._create_document(record)
            action = {
                "_index": self.index_name,
                "_id": record['chunk_id'],
                "_source": doc
            }
            actions.append(action)
        
        for attempt in range(self.max_retries):
            try:
                response = self.connector.bulk(actions)
                
                if response.get('failed_count', 0) > 0:
                    self.logger.warning(f"Bulk ingest had {response['failed_count']} errors")
                    return False
                
                self.logger.info(f"Successfully bulk ingested {len(records)} records")
                return True
                
            except Exception as e:
                if attempt == self.max_retries - 1:
                    self.logger.error(f"Failed bulk ingest: {str(e)}")
                    return False
                time.sleep(self.max_retries * (attempt + 1))
        
        return False
