# pipeline/extractors/elasticsearch.py
from typing import Iterator, Dict, Any
from .base import BaseExtractor
from .registry import ExtractorRegistry


class ElasticsearchExtractor(BaseExtractor):
    def __init__(self, name: str, connector, config: Dict[str, Any]):
        super().__init__(name, connector, config)

    def extract(self) -> Iterator[Dict[str, Any]]:
        """Extract documents from Elasticsearch indices based on configuration."""
        indices = self.config.get('indices', [])
        extraction_mode = self.config.get('extraction_mode', 'full')
        date_field = self.config.get('date_field', '@timestamp')
        batch_size = self.config.get('batch_size', 1000)
        
        for index in indices:
            # Build query based on extraction mode
            query = {"query": {"match_all": {}}}
            
            if extraction_mode == 'incremental_date' and date_field:
                # For now, just extract all data - state management would be added here
                pass
            
            # Add size to query
            query['size'] = batch_size
            
            try:
                results = self.connector.search(index=index, body=query)
                
                for hit in results.get('hits', {}).get('hits', []):
                    document = hit['_source']
                    # Add index context
                    document['_source_index'] = index
                    document['_document_id'] = hit['_id']
                    yield document
                    
            except Exception as e:
                # Continue with other indices if one fails
                continue


# Register the extractor
ExtractorRegistry.register("elasticsearch", ElasticsearchExtractor)
