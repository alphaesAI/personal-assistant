import os
import pickle
from typing import Optional
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build, Resource
from .base import BaseConnector
from .registry import ConnectorRegistry


class GmailConnector(BaseConnector):
    """Gmail API connector for fetching emails and attachments."""

    def __init__(self, name: str, config: dict):
        """
        Args:
            name: Name for the connector instance
            config: dictionary containing keys
                - credentials_path
                - token_path
                - scopes (optional, defaults to read-only scope)
                - api_version (optional, defaults to 'v1')
                - auth_port (optional, defaults to 0)
        """

        # Pass name and config to BaseConnector
        super().__init__(name, config)

        # Local config
        self.service: Optional[Resource] = None
        self.credentials_path = config.get("credentials_path")
        self.token_path = config.get("token_path")
        self.scopes = config.get("scopes", os.getenv('GMAIL_SCOPES', "https://www.googleapis.com/auth/gmail.readonly").split(','))
        self.api_version = config.get("api_version", os.getenv('GMAIL_API_VERSION', 'v1'))
        self.auth_port = int(config.get("auth_port", os.getenv('GMAIL_AUTH_PORT', '0')))

    def connect(self) -> None:
        """Authenticate and build Gmail service client."""
        creds = None

        if os.path.exists(self.token_path):
            with open(self.token_path, "rb") as token:
                creds = pickle.load(token)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, self.scopes
                )
                creds = flow.run_local_server(port=self.auth_port)

            with open(self.token_path, "wb") as token:
                pickle.dump(creds, token)

        self.service = build("gmail", self.api_version, credentials=creds)

    def disconnect(self) -> None:
        self.service = None

    def test_connection(self) -> bool:
        """Test Gmail connection."""
        try:
            if not self.service:
                return False
            profile = self.service.users().getProfile(userId="me").execute()
            return profile is not None
        except Exception:
            return False

    def get_connection_info(self):
        """Required abstract method."""
        return {
            "credentials_path": self.credentials_path,
            "token_path": self.token_path,
        }

    def list_messages(self, query: str = ""):
        """List Gmail messages with optional query filter."""
        if not self.service:
            raise Exception("Not connected to Gmail service")
        
        results = self.service.users().messages().list(userId="me", q=query).execute()
        messages = results.get("messages", [])
        
        # Handle pagination
        while "nextPageToken" in results:
            page_token = results["nextPageToken"]
            results = self.service.users().messages().list(
                userId="me", q=query, pageToken=page_token
            ).execute()
            messages.extend(results.get("messages", []))
        
        return messages

    def get_message(self, message_id: str):
        """Get full Gmail message by ID."""
        if not self.service:
            raise Exception("Not connected to Gmail service")
        
        message = self.service.users().messages().get(
            userId="me", id=message_id, format="full"
        ).execute()
        return message

    def get_attachment(self, message_id: str, attachment_id: str):
        """Get Gmail attachment by ID."""
        if not self.service:
            raise Exception("Not connected to Gmail service")
        
        attachment = self.service.users().messages().attachments().get(
            userId="me", messageId=message_id, id=attachment_id
        ).execute()
        return attachment

    def modify_labels(self, message_id: str, remove_labels: list = None, add_labels: list = None):
        """Modify labels on a Gmail message."""
        if not self.service:
            raise Exception("Not connected to Gmail service")
        
        body = {}
        if remove_labels:
            body["removeLabelIds"] = remove_labels
        if add_labels:
            body["addLabelIds"] = add_labels
        
        if body:
            self.service.users().messages().modify(
                userId="me", id=message_id, body=body
            ).execute()


# Register the connector
ConnectorRegistry.register("gmail", GmailConnector)
