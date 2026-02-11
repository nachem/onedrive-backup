"""Configuration settings and models for the backup application."""

import os
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import boto3
import yaml
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field, validator

try:
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient
except ImportError:
    DefaultAzureCredential = None
    SecretClient = None


class SourceType(str, Enum):
    """Supported source types for backup."""
    ONEDRIVE_PERSONAL = "onedrive_personal"
    ONEDRIVE_BUSINESS = "onedrive_business"
    SHAREPOINT = "sharepoint"


class DestinationType(str, Enum):
    """Supported destination types for backup."""
    AWS_S3 = "aws_s3"
    AZURE_BLOB = "azure_blob"


class ChangeDetectionType(str, Enum):
    """Change detection methods."""
    TIMESTAMP = "timestamp"
    HASH = "hash"
    BOTH = "both"


class SourceConfig(BaseModel):
    """Configuration for backup sources."""
    type: SourceType
    name: str
    folders: Union[List[str], str] = "all"
    users: Union[List[str], str] = "all"  # For OneDrive: "all" or list of email addresses
    site_url: Optional[str] = None  # For SharePoint
    libraries: Optional[List[str]] = None  # For SharePoint
    
    @validator('site_url')
    def validate_sharepoint_url(cls, v, values):
        if values.get('type') == SourceType.SHAREPOINT and not v:
            raise ValueError('site_url is required for SharePoint sources')
        return v
    
    @validator('libraries')
    def validate_sharepoint_libraries(cls, v, values):
        if values.get('type') == SourceType.SHAREPOINT and not v:
            raise ValueError('libraries is required for SharePoint sources')
        return v


class DestinationConfig(BaseModel):
    """Configuration for backup destinations."""
    type: DestinationType
    name: str
    
    # AWS S3 specific
    bucket: Optional[str] = None
    region: Optional[str] = "us-east-1"
    
    # Azure Blob specific
    account: Optional[str] = None
    container: Optional[str] = None
    
    # Common
    prefix: str = ""
    
    @validator('bucket')
    def validate_s3_bucket(cls, v, values):
        if values.get('type') == DestinationType.AWS_S3 and not v:
            raise ValueError('bucket is required for AWS S3 destinations')
        return v
    
    @validator('account')
    def validate_azure_account(cls, v, values):
        if values.get('type') == DestinationType.AZURE_BLOB and not v:
            raise ValueError('account is required for Azure Blob destinations')
        return v
    
    @validator('container')
    def validate_azure_container(cls, v, values):
        if values.get('type') == DestinationType.AZURE_BLOB and not v:
            raise ValueError('container is required for Azure Blob destinations')
        return v


class BackupJobConfig(BaseModel):
    """Configuration for individual backup jobs."""
    name: str
    sources: List[str]  # Names of source configurations
    destination: str  # Name of destination configuration
    schedule: Optional[str] = None  # Cron expression
    change_detection: ChangeDetectionType = ChangeDetectionType.TIMESTAMP
    enabled: bool = True


class SyncOptions(BaseModel):
    """Synchronization options."""
    retry_attempts: int = 3
    retry_delay: int = 5  # seconds
    parallel_uploads: int = 4
    encryption: bool = False
    chunk_size: int = 8 * 1024 * 1024  # 8MB
    verify_uploads: bool = True
    preserve_timestamps: bool = True


class BackupConfig(BaseModel):
    """Main configuration class."""
    sources: List[SourceConfig]
    destinations: List[DestinationConfig]
    backup_jobs: List[BackupJobConfig]
    sync_options: SyncOptions = Field(default_factory=SyncOptions)
    secrets: Optional[Dict[str, Any]] = None  # For secret management configuration
    @classmethod
    def from_yaml(cls, config_path: Union[str, Path]) -> "BackupConfig":
        """Load configuration from YAML file."""
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        return cls(**config_data)
    
    def to_yaml(self, config_path: Union[str, Path]) -> None:
        """Save configuration to YAML file."""
        config_path = Path(config_path)
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(self.dict(exclude_none=True), f, default_flow_style=False, indent=2)
    
    def get_source_by_name(self, name: str) -> Optional[SourceConfig]:
        """Get source configuration by name."""
        for source in self.sources:
            if source.name == name:
                return source
        return None
    
    def get_destination_by_name(self, name: str) -> Optional[DestinationConfig]:
        """Get destination configuration by name."""
        for destination in self.destinations:
            if destination.name == name:
                return destination
        return None
    
    def get_enabled_jobs(self) -> List[BackupJobConfig]:
        """Get all enabled backup jobs."""
        return [job for job in self.backup_jobs if job.enabled]


class CredentialsConfig(BaseModel):
    """Credentials configuration (stored separately for security)."""
    microsoft_app_id: Optional[str] = None
    microsoft_app_secret: Optional[str] = None
    microsoft_tenant_id: Optional[str] = None
    
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    
    azure_storage_connection_string: Optional[str] = None
    azure_storage_account_key: Optional[str] = None
    
    encryption_key: Optional[str] = None
    
    @staticmethod
    def get_aws_credentials(parameter_name, region_name=None):
        """
        Fetch credentials YAML from AWS Parameter Store and parse it.
        """
        session = boto3.Session()
        ssm = session.client('ssm', region_name=region_name)
        try:
            response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
            yaml_content = response['Parameter']['Value']
            return yaml.safe_load(yaml_content)
        except ClientError as e:
            print(f"AWS Parameter Store error: {e}")
            return None
    @staticmethod
    def get_azure_credentials(secret_name, vault_url):
        """
        Fetch credentials YAML from Azure Key Vault and parse it.
        """
        if DefaultAzureCredential is None or SecretClient is None:
            print("Azure SDK not installed.")
            return None
        try:
            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=vault_url, credential=credential)
            secret = client.get_secret(secret_name)
            yaml_content = secret.value
            return yaml.safe_load(yaml_content)
        except Exception as e:
            print(f"Azure Key Vault error: {e}")
            return None
    @staticmethod
    def read_credentials(config:BackupConfig):
        """
        Load credentials with priority:
        1. If config specifies secret_provider and secret_name, use that provider.
        2. Otherwise, fallback to local credentials.yaml file.
        """
        # If config not provided, try to load from local file
        assert config is not None, "Config must be provided to read credentials"
        
        secret_provider = config.secrets['secret_provider'] 
        secret_name = config.secrets['secret_name']
        vault_url = config.secrets['azure_vault_url'] if 'azure_vault_url' in config.secrets else None
        region_name = config.secrets['aws_region'] if 'aws_region' in config.secrets else None

        if secret_provider == 'aws' and secret_name:
            creds = CredentialsConfig.get_aws_credentials(secret_name, region_name)
            if creds:
                return CredentialsConfig(**creds)
        elif secret_provider == 'azure' and secret_name and vault_url:
            creds = CredentialsConfig.get_azure_credentials(secret_name, vault_url)
            if creds:
                return CredentialsConfig(**creds)
        config_path = Path(__file__).parent.parent.parent / 'config' / 'credentials.yaml'
        # Fallback to local file
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        else:
            print("No credentials found.")
            return None
    
    @classmethod
    def from_yaml(cls, credentials_path: Union[str, Path]) -> "CredentialsConfig":
        """Load credentials from YAML file."""
        credentials_path = Path(credentials_path)
        if not credentials_path.exists():
            return cls()  # Return empty config if file doesn't exist
        
        with open(credentials_path, 'r', encoding='utf-8') as f:
            creds_data = yaml.safe_load(f) or {}
        
        return cls(**creds_data)
    
    @classmethod
    def from_env(cls) -> "CredentialsConfig":
        """Load credentials from environment variables."""
        return cls(
            microsoft_app_id=os.getenv('MICROSOFT_APP_ID'),
            microsoft_app_secret=os.getenv('MICROSOFT_APP_SECRET'),
            microsoft_tenant_id=os.getenv('MICROSOFT_TENANT_ID'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
            azure_storage_connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
            azure_storage_account_key=os.getenv('AZURE_STORAGE_ACCOUNT_KEY'),
            encryption_key=os.getenv('BACKUP_ENCRYPTION_KEY')
        )
    
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Test credential loading from config.yaml.")
    parser.add_argument('--config', type=str, default=os.path.join(os.path.dirname(__file__), '../../../config/config.yaml'), help='Path to config.yaml')
    args = parser.parse_args()

    config_path = args.config
    print(f"Loading configuration from: {config_path}")
    if not os.path.exists(config_path):
        print(f"Config file not found: {config_path}")
        exit(1)

    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    print("Loaded config.yaml:")
    print(yaml.dump(config, default_flow_style=False, indent=2))

    print("\nLoading credentials based on config.yaml settings...")
    creds = CredentialsConfig.read_credentials(config)
    if creds:
        print("Loaded credentials:")
    else:
        print("Failed to load credentials from specified provider or local file.")
    
