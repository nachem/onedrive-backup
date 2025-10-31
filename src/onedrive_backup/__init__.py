"""
OneDrive/SharePoint Backup Application

A comprehensive backup solution for OneDrive and SharePoint files to cloud storage.
Supports AWS S3 and Azure Blob Storage with intelligent change detection.
"""

__version__ = "1.0.0"
__author__ = "OneDrive Backup Tool"
__description__ = "Backup OneDrive and SharePoint files to cloud storage"

from .config.settings import BackupConfig
from .sync.backup_manager import BackupManager

__all__ = ["BackupConfig", "BackupManager"]
