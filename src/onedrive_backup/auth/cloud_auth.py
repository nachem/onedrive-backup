"""Cloud storage authentication handling."""

import os
import boto3
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

class AWSAuth:
    """Handle AWS authentication and S3 client creation."""
    
    def __init__(
        self,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        session_token: Optional[str] = None,
        region: str = "us-east-1"
    ):
        """Initialize AWS authentication.
        
        Args:
            access_key_id: AWS access key ID
            secret_access_key: AWS secret access key
            session_token: AWS session token (for temporary credentials)
            region: AWS region
        """
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.session_token = session_token
        self.region = region
        self._s3_client = None
    
    def get_s3_client(self):
        """Get authenticated S3 client.
        
        Returns:
            boto3 S3 client
        """
        if self._s3_client is None:
            # Use provided credentials or fall back to default credential chain
            if self.access_key_id and self.secret_access_key:
                self._s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.access_key_id,
                    aws_secret_access_key=self.secret_access_key,
                    aws_session_token=self.session_token,
                    region_name=self.region
                )
            else:
                # Use default credential chain (environment, instance profile, etc.)
                self._s3_client = boto3.client('s3', region_name=self.region)
        
        return self._s3_client
    
    def test_connection(self, bucket_name: str) -> bool:
        """Test S3 connection by checking if bucket is accessible.
        
        Args:
            bucket_name: Name of S3 bucket to test
            
        Returns:
            True if connection successful, False otherwise
        """
        try:
            s3_client = self.get_s3_client()
            s3_client.head_bucket(Bucket=bucket_name)
            return True
        except Exception as e:
            logger.error(f"Failed to connect to S3 bucket {bucket_name}: {e}")
            return False
    
    @classmethod
    def from_env(cls, region: str = "us-east-1") -> "AWSAuth":
        """Create AWS auth from environment variables.
        
        Args:
            region: AWS region
            
        Returns:
            AWSAuth instance
        """
        return cls(
            access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            session_token=os.getenv('AWS_SESSION_TOKEN'),
            region=region
        )

class AzureAuth:
    """Handle Azure Blob Storage authentication."""
    
    def __init__(
        self,
        account_name: str,
        account_key: Optional[str] = None,
        connection_string: Optional[str] = None,
        use_default_credential: bool = False
    ):
        """Initialize Azure Blob Storage authentication.
        
        Args:
            account_name: Storage account name
            account_key: Storage account key
            connection_string: Storage connection string or SAS URL
            use_default_credential: Use DefaultAzureCredential
        """
        self.account_name = account_name
        self.account_key = account_key
        self.connection_string = connection_string
        self.use_default_credential = use_default_credential
        self._blob_service_client = None
        self._sas_url = None
        
        # Check if connection_string is actually a SAS URL
        if connection_string and connection_string.startswith('https://'):
            self._sas_url = connection_string
            self.connection_string = None
    
    def get_blob_service_client(self) -> BlobServiceClient:
        """Get authenticated Blob Service client.
        
        Returns:
            Azure BlobServiceClient
        """
        if self._blob_service_client is None:
            if self._sas_url:
                # It's a SAS URL - the URL contains container + SAS token
                # Extract just the account URL (without container and SAS)
                account_url = self._sas_url.split('?')[0].rsplit('/', 1)[0]
                self._blob_service_client = BlobServiceClient(
                    account_url=account_url + '?' + self._sas_url.split('?', 1)[1],
                    credential=None  # SAS token in URL handles auth
                )
            elif self.connection_string:
                # Use connection string
                self._blob_service_client = BlobServiceClient.from_connection_string(
                    self.connection_string
                )
            elif self.account_key:
                # Use account name and key
                self._blob_service_client = BlobServiceClient(
                    account_url=f"https://{self.account_name}.blob.core.windows.net",
                    credential=self.account_key
                )
            elif self.use_default_credential:
                # Use DefaultAzureCredential (managed identity, service principal, etc.)
                credential = DefaultAzureCredential()
                self._blob_service_client = BlobServiceClient(
                    account_url=f"https://{self.account_name}.blob.core.windows.net",
                    credential=credential
                )
            else:
                raise ValueError(
                    "Must provide either connection_string, account_key, or set use_default_credential=True"
                )
        
        return self._blob_service_client
    
    def test_connection(self, container_name: str) -> bool:
        """Test Azure Blob Storage connection.
        
        Args:
            container_name: Name of container to test
            
        Returns:
            True if connection successful, False otherwise
        """
        try:
            blob_service_client = self.get_blob_service_client()
            container_client = blob_service_client.get_container_client(container_name)
            container_client.get_container_properties()
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Azure container {container_name}: {e}")
            return False
    
    @classmethod
    def from_env(cls, account_name: str) -> "AzureAuth":
        """Create Azure auth from environment variables.
        
        Args:
            account_name: Storage account name
            
        Returns:
            AzureAuth instance
        """
        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
        
        return cls(
            account_name=account_name,
            account_key=account_key,
            connection_string=connection_string,
            use_default_credential=not (connection_string or account_key)
        )
