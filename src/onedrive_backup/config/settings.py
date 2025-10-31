"""Configuration settings and models for the backup application."""

import os
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, Field, validator


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
