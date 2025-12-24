# pipeline/extractors/gmail.py
import base64
from pathlib import Path
from typing import Iterator, Dict, Any, List
from .base import BaseExtractor
from .registry import ExtractorRegistry


class GmailExtractor(BaseExtractor):
    """
    Gmail extractor that fetches messages based on configuration.

    Responsibilities:
    - Act as orchestrator for email extraction workflow
    - Delegate specific tasks to focused helper methods
    - Maintain strict separation of concerns
    """

    def __init__(self, name: str, connector, config: Dict[str, Any]):
        super().__init__(name=name, connector=connector, config=config)

    def extract(self) -> Iterator[Dict[str, Any]]:
        """
        Orchestrate Gmail message extraction workflow.

        Yields:
            Normalized document records with metadata and attachment references
        """
        message_ids = self._get_message_ids()
        all_messages = []
        
        for message_id in message_ids:
            raw_message = self.connector.get_message(message_id)
            normalized_message = self._normalize_message(raw_message)
            
            if self._has_attachments(raw_message):
                normalized_message['attachments'] = self._store_attachments(raw_message)
            
            all_messages.append(normalized_message)
            yield normalized_message
        
        # Store all messages using writer
        from .writer import write
        write(all_messages, 'extractors/gmail.json')

    def _get_message_ids(self) -> List[str]:
        """Get message IDs based on configuration.
        
        Returns:
            List of Gmail message IDs to extract
        """
        labels = self.config.get('labels', ['INBOX'])
        query = self.config.get('query', 'is:unread')
        batch_size = self.config.get('batch_size', 100)
        
        message_ids = []
        
        for label in labels:
            label_query = f"label:{label} {query}".strip()
            messages = self.connector.list_messages(query=label_query)
            
            for msg in messages[:batch_size]:
                message_id = msg.get("id")
                if message_id:
                    message_ids.append(message_id)
        
        return message_ids
    
    def _normalize_message(self, raw_message: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize raw Gmail message to standard document format.
        
        Args:
            raw_message: Raw Gmail message data
            
        Returns:
            Normalized document record
        """
        headers = raw_message.get('payload', {}).get('headers', [])
        
        # Extract metadata
        metadata = {}
        for header in headers:
            name = header.get('name', '').lower()
            value = header.get('value', '')
            if name in ['subject', 'from', 'to', 'date']:
                metadata[name] = value
        
        # Add labels
        metadata['labels'] = raw_message.get('labelIds', [])
        
        # Extract body content
        body = self._extract_body_content(raw_message.get('payload', {}))
        
        return {
            "source": "gmail",
            "type": "document",
            "id": raw_message.get('id', ''),
            "metadata": metadata,
            "body": body
        }
    
    def _extract_body_content(self, payload: Dict[str, Any]) -> str:
        """Extract body content from message payload.
        
        Args:
            payload: Gmail message payload
            
        Returns:
            Body content (HTML or text)
        """
        def extract_from_parts(parts):
            for part in parts:
                mime_type = part.get('mimeType', '')
                
                if 'text/html' in mime_type and 'data' in part.get('body', {}):
                    return base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                elif 'text/plain' in mime_type and 'data' in part.get('body', {}):
                    return base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                elif 'parts' in part:
                    content = extract_from_parts(part['parts'])
                    if content:
                        return content
            return ''
        
        # Check single part message first
        if 'data' in payload.get('body', {}):
            mime_type = payload.get('mimeType', '')
            if 'text/html' in mime_type or 'text/plain' in mime_type:
                return base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8')
        
        # Check multipart message
        if 'parts' in payload:
            return extract_from_parts(payload['parts'])
        
        return ''
    
    def _has_attachments(self, raw_message: Dict[str, Any]) -> bool:
        """Check if message has attachments.
        
        Args:
            raw_message: Raw Gmail message data
            
        Returns:
            True if message has attachments
        """
        payload = raw_message.get('payload', {})
        return self._find_attachment_parts(payload)
    
    def _find_attachment_parts(self, payload: Dict[str, Any]) -> bool:
        """Recursively find attachment parts in payload.
        
        Args:
            payload: Message payload part
            
        Returns:
            True if attachment parts found
        """
        if payload.get('filename') and payload.get('body', {}).get('attachmentId'):
            return True
        
        if 'parts' in payload:
            for part in payload['parts']:
                if self._find_attachment_parts(part):
                    return True
        
        return False
    
    def _store_attachments(self, raw_message: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Store attachments and return metadata.
        
        Args:
            raw_message: Raw Gmail message data
            
        Returns:
            List of attachment metadata
        """
        message_id = raw_message.get('id', '')
        attachments_dir = self._get_attachments_path(message_id)
        attachments_dir.mkdir(parents=True, exist_ok=True)
        
        attachments = []
        payload = raw_message.get('payload', {})
        
        self._extract_attachments_recursive(payload, message_id, attachments_dir, attachments)
        
        return attachments
    
    def _extract_attachments_recursive(self, part: Dict[str, Any], message_id: str, 
                                     attachments_dir: Path, attachments: List[Dict[str, Any]]) -> None:
        """Recursively extract and store attachments.
        
        Args:
            part: Message payload part
            message_id: Gmail message ID
            attachments_dir: Directory to store attachments
            attachments: List to append attachment metadata
        """
        if part.get('filename') and part.get('body', {}).get('attachmentId'):
            attachment_id = part['body']['attachmentId']
            filename = part['filename']
            
            try:
                attachment_data = self.connector.get_attachment(message_id, attachment_id)
                
                if 'data' in attachment_data:
                    content = base64.urlsafe_b64decode(attachment_data['data'])
                    
                    attachment_path = attachments_dir / filename
                    with open(attachment_path, 'wb') as f:
                        f.write(content)
                    
                    relative_path = str(attachment_path.relative_to(self._get_project_root()))
                    
                    attachments.append({
                        'filename': filename,
                        'path': relative_path,
                        'mime_type': part.get('mimeType', 'application/octet-stream')
                    })
            except Exception as e:
                print(f"Error extracting attachment {filename}: {e}")
        
        if 'parts' in part:
            for subpart in part['parts']:
                self._extract_attachments_recursive(subpart, message_id, attachments_dir, attachments)
    
    def _get_attachments_path(self, message_id: str) -> Path:
        """Get attachments directory path for message.
        
        Args:
            message_id: Gmail message ID
            
        Returns:
            Path to attachments directory
        """
        return self._get_project_root() / 'data' / 'attachments' / message_id
    
    def _get_project_root(self) -> Path:
        """Get project root directory path.
        
        Returns:
            Path to project root
        """
        return Path(__file__).parent.parent.parent


# Register extractor
ExtractorRegistry.register("gmail", GmailExtractor)
