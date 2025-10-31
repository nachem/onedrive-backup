"""Main backup manager orchestrating the backup process."""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..auth.cloud_auth import AWSAuth, AzureAuth
from ..auth.microsoft_auth import MicrosoftGraphAuth
from ..config.settings import BackupConfig, BackupJobConfig

# Module logger
logger = logging.getLogger(__name__)


class BackupManager:
    """Main backup manager that orchestrates the backup process."""
    
    def __init__(self, config: BackupConfig):
        """Initialize backup manager.
        
        Args:
            config: Backup configuration
        """
        self.config = config
        self.microsoft_auth: Optional[MicrosoftGraphAuth] = None
        self.aws_auth: Optional[AWSAuth] = None
        self.azure_auth: Optional[AzureAuth] = None
        
        # Setup logging using proper utility
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging configuration with UTF-8 support."""
        from ..utils.logging import setup_logging

        # Setup logger with UTF-8 encoding support
        log_file = Path('logs/backup.log')
        setup_logging(
            log_level="INFO",
            log_file=log_file,
            log_to_console=True,
            max_file_size=10 * 1024 * 1024,  # 10MB
            backup_count=5
        )
        
        # Reconfigure module logger
        global logger
        logger = logging.getLogger("onedrive_backup")
    
    def initialize_auth(self, credentials_config):
        """Initialize authentication for all required services.
        
        Args:
            credentials_config: Credentials configuration
        """
        # Initialize Microsoft Graph authentication if needed
        if any(source.type in ['onedrive_personal', 'onedrive_business', 'sharepoint'] 
               for source in self.config.sources):
            if credentials_config.microsoft_app_id:
                self.microsoft_auth = MicrosoftGraphAuth(
                    app_id=credentials_config.microsoft_app_id,
                    app_secret=credentials_config.microsoft_app_secret,
                    tenant_id=credentials_config.microsoft_tenant_id
                )
                logger.info("Microsoft Graph authentication initialized")
            else:
                logger.warning("Microsoft Graph credentials not found")
        
        # Initialize AWS authentication if needed
        if any(dest.type == 'aws_s3' for dest in self.config.destinations):
            self.aws_auth = AWSAuth(
                access_key_id=credentials_config.aws_access_key_id,
                secret_access_key=credentials_config.aws_secret_access_key,
                session_token=credentials_config.aws_session_token
            )
            logger.info("AWS authentication initialized")
        
        # Initialize Azure authentication if needed
        azure_destinations = [dest for dest in self.config.destinations if dest.type == 'azure_blob']
        if azure_destinations:
            # Use the first Azure destination's account name
            account_name = azure_destinations[0].account
            self.azure_auth = AzureAuth(
                account_name=account_name,
                account_key=credentials_config.azure_storage_account_key,
                connection_string=credentials_config.azure_storage_connection_string
            )
            logger.info("Azure authentication initialized")
    
    def _get_delta_token(self, source_name: str, user_id: str, destination_config) -> Optional[Dict[str, str]]:
        """Get delta token and last backup time for a specific user from S3 metadata.
        
        Args:
            source_name: Name of the source
            user_id: User ID
            destination_config: Destination configuration
            
        Returns:
            Dictionary with 'delta_token' and 'last_backup_time', or None if no previous delta
        """
        try:
            if destination_config.type != 'aws_s3':
                return None
            
            s3_client = self.aws_auth.get_s3_client()
            prefix = getattr(destination_config, 'prefix', '')
            # Store delta token per user
            token_key = f"{prefix}.backup-metadata/{source_name}_delta_tokens/{user_id}.json".lstrip('/')
            
            logger.debug(f"Checking for delta token: s3://{destination_config.bucket}/{token_key}")
            
            response = s3_client.get_object(
                Bucket=destination_config.bucket,
                Key=token_key
            )
            
            metadata = json.loads(response['Body'].read().decode('utf-8'))
            delta_token = metadata.get('delta_token')
            last_backup_time = metadata.get('last_backup_time')
            
            if delta_token:
                logger.info(f"✅ Found delta token for user {user_id}")
                if last_backup_time:
                    logger.info(f"   Last backup: {last_backup_time}")
                return {
                    'delta_token': delta_token,
                    'last_backup_time': last_backup_time
                }
            
        except s3_client.exceptions.NoSuchKey:
            logger.info(f"No delta token found for user {user_id} - will perform initial delta sync")
        except Exception as e:
            logger.warning(f"Error reading delta token: {e} - will perform initial delta sync")
        
        return None
    
    def _save_delta_token(self, source_name: str, user_id: str, delta_token: str, destination_config):
        """Save delta token and timestamp for a specific user to S3 metadata.
        
        Args:
            source_name: Name of the source
            user_id: User ID
            delta_token: Delta token URL to save
            destination_config: Destination configuration
        """
        try:
            if destination_config.type != 'aws_s3':
                return
            
            s3_client = self.aws_auth.get_s3_client()
            prefix = getattr(destination_config, 'prefix', '')
            token_key = f"{prefix}.backup-metadata/{source_name}_delta_tokens/{user_id}.json".lstrip('/')
            
            current_time = datetime.utcnow().isoformat() + 'Z'
            
            metadata = {
                'user_id': user_id,
                'delta_token': delta_token,
                'last_backup_time': current_time,  # Save timestamp for fallback
                'last_updated': current_time
            }
            
            s3_client.put_object(
                Bucket=destination_config.bucket,
                Key=token_key,
                Body=json.dumps(metadata, indent=2).encode('utf-8'),
                ContentType='application/json'
            )
            
            logger.debug(f"💾 Saved delta token and timestamp for user {user_id}")
            
        except Exception as e:
            logger.error(f"Failed to save delta token: {e}")
    
    def _get_last_backup_timestamp(self, source_name: str, destination_config) -> Optional[str]:
        """Get last successful backup timestamp from destination metadata.
        
        Args:
            source_name: Name of the source
            destination_config: Destination configuration
            
        Returns:
            ISO format timestamp string or None if no previous backup
        """
        try:
            if destination_config.type != 'aws_s3':
                logger.warning(f"Metadata timestamp only supported for AWS S3, not {destination_config.type}")
                return None
            
            s3_client = self.aws_auth.get_s3_client()
            prefix = getattr(destination_config, 'prefix', '')
            metadata_key = f"{prefix}.backup-metadata/{source_name}_last_backup.json".lstrip('/')
            
            logger.info(f"Checking for previous backup metadata: s3://{destination_config.bucket}/{metadata_key}")
            
            # Try to get metadata file
            response = s3_client.get_object(
                Bucket=destination_config.bucket,
                Key=metadata_key
            )
            
            metadata = json.loads(response['Body'].read().decode('utf-8'))
            last_backup_time = metadata.get('last_backup_time')
            
            if last_backup_time:
                logger.info(f"Found previous backup from: {last_backup_time}")
                logger.info(f"  Files backed up: {metadata.get('files_backed_up', 'unknown')}")
                logger.info(f"  Bytes transferred: {metadata.get('bytes_transferred', 'unknown'):,}")
                return last_backup_time
            
        except s3_client.exceptions.NoSuchKey:
            logger.info(f"No previous backup found for {source_name} - will perform full backup")
        except Exception as e:
            logger.warning(f"Error reading backup metadata: {e} - will perform full backup")
        
        return None
    
    def _check_s3_file_exists(self, destination_config, file_path: str, source_modified_time: str) -> bool:
        """Check if file exists in S3 with same modification time.
        
        Args:
            destination_config: Destination configuration
            file_path: File path in S3
            source_modified_time: Modification time from source (ISO format)
            
        Returns:
            True if file exists with same modification time, False otherwise
        """
        try:
            if destination_config.type != 'aws_s3':
                return False
            
            s3_client = self.aws_auth.get_s3_client()
            prefix = getattr(destination_config, 'prefix', '')
            s3_key = f"{prefix}{file_path}".lstrip('/')
            
            # Try to get object metadata
            response = s3_client.head_object(
                Bucket=destination_config.bucket,
                Key=s3_key
            )
            
            # Check if modification time matches
            existing_modified = response.get('Metadata', {}).get('source-modified-time', '')
            
            if existing_modified == source_modified_time:
                logger.debug(f"File exists with same modification time: {file_path}")
                return True
            else:
                logger.debug(f"File exists but modified time changed: {file_path}")
                logger.debug(f"  Existing: {existing_modified}")
                logger.debug(f"  New: {source_modified_time}")
                return False
                
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                # File doesn't exist
                logger.debug(f"File not found in S3: {file_path}")
                return False
            else:
                # Other error - assume file doesn't exist to be safe
                logger.warning(f"Error checking S3 file: {e}")
                return False
        except Exception as e:
            logger.warning(f"Error checking S3 file existence: {e}")
            return False
    
    def _save_backup_timestamp(self, source_name: str, destination_config, stats: Dict[str, Any]):
        """Save backup completion timestamp to destination metadata.
        
        Args:
            source_name: Name of the source
            destination_config: Destination configuration
            stats: Backup statistics (files_backed_up, bytes_transferred, etc.)
        """
        try:
            if destination_config.type != 'aws_s3':
                logger.debug(f"Metadata timestamp only supported for AWS S3, not {destination_config.type}")
                return
            
            s3_client = self.aws_auth.get_s3_client()
            prefix = getattr(destination_config, 'prefix', '')
            metadata_key = f"{prefix}.backup-metadata/{source_name}_last_backup.json".lstrip('/')
            
            metadata = {
                'source_name': source_name,
                'last_backup_time': datetime.utcnow().isoformat() + 'Z',
                'files_backed_up': stats.get('files_uploaded', 0),
                'files_skipped': stats.get('files_skipped', 0),
                'bytes_transferred': stats.get('bytes_transferred', 0),
                'backup_duration_seconds': stats.get('duration', 0)
            }
            
            # Upload metadata file
            s3_client.put_object(
                Bucket=destination_config.bucket,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2).encode('utf-8'),
                ContentType='application/json',
                Metadata={
                    'source': 'onedrive-backup',
                    'type': 'backup-metadata'
                }
            )
            
            logger.info(f"✅ Saved backup metadata to s3://{destination_config.bucket}/{metadata_key}")
            
        except Exception as e:
            logger.error(f"Failed to save backup metadata: {e}")
    
    async def run_backup_job(self, job_config: BackupJobConfig) -> Dict[str, Any]:
        """Run a single backup job.
        
        Args:
            job_config: Configuration for the backup job
            
        Returns:
            Dictionary with backup results
        """
        logger.info(f"Starting backup job: {job_config.name}")
        start_time = datetime.now()
        
        results = {
            'job_name': job_config.name,
            'start_time': start_time,
            'status': 'started',
            'files_processed': 0,
            'files_uploaded': 0,
            'files_skipped': 0,
            'bytes_transferred': 0,
            'errors': []
        }
        
        try:
            # Get destination configuration
            destination = self.config.get_destination_by_name(job_config.destination)
            if not destination:
                raise ValueError(f"Destination not found: {job_config.destination}")
            
            # Process each source
            for source_name in job_config.sources:
                source = self.config.get_source_by_name(source_name)
                if not source:
                    logger.error(f"Source not found: {source_name}")
                    results['errors'].append(f"Source not found: {source_name}")
                    continue
                
                logger.info(f"Processing source: {source_name}")
                source_results = await self._process_source(source, destination, job_config)
                
                # Aggregate results
                results['files_processed'] += source_results.get('files_processed', 0)
                results['files_uploaded'] += source_results.get('files_uploaded', 0)
                results['files_skipped'] += source_results.get('files_skipped', 0)
                results['bytes_transferred'] += source_results.get('bytes_transferred', 0)
                results['errors'].extend(source_results.get('errors', []))
            
            results['status'] = 'completed'
            
        except Exception as e:
            logger.error(f"Backup job {job_config.name} failed: {e}")
            results['status'] = 'failed'
            results['errors'].append(str(e))
        
        finally:
            results['end_time'] = datetime.now()
            results['duration'] = (results['end_time'] - start_time).total_seconds()
            logger.info(f"Backup job {job_config.name} completed in {results['duration']:.2f} seconds")
        
        return results
    
    async def _process_source(self, source_config, destination_config, job_config) -> Dict[str, Any]:
        """Process a single source configuration.
        
        Args:
            source_config: Source configuration
            destination_config: Destination configuration
            job_config: Job configuration
            
        Returns:
            Dictionary with processing results
        """
        results = {
            'files_processed': 0,
            'files_uploaded': 0,
            'files_skipped': 0,
            'bytes_transferred': 0,
            'errors': [],
            'duration': 0
        }
        
        source_start_time = datetime.now()
        
        try:
            logger.info(f"Processing {source_config.type} source: {source_config.name}")
            
            # Get last backup timestamp for incremental backup
            last_backup_time = self._get_last_backup_timestamp(source_config.name, destination_config)
            
            # Handle OneDrive sources
            if source_config.type == 'onedrive_personal':
                results = await self._process_onedrive_source(
                    source_config, destination_config, job_config, last_backup_time
                )
            elif source_config.type == 'sharepoint':
                results = await self._process_sharepoint_source(
                    source_config, destination_config, job_config, last_backup_time
                )
            else:
                logger.warning(f"Unsupported source type: {source_config.type}")
                results['errors'].append(f"Unsupported source type: {source_config.type}")
            
            # Calculate duration
            results['duration'] = (datetime.now() - source_start_time).total_seconds()
            
            # Save backup completion timestamp (only if files were uploaded and no dry-run)
            if results['files_uploaded'] > 0 and not getattr(job_config, 'dry_run', False):
                self._save_backup_timestamp(source_config.name, destination_config, results)
            
        except Exception as e:
            logger.error(f"Error processing source {source_config.name}: {e}")
            results['errors'].append(f"Source {source_config.name}: {str(e)}")
            results['duration'] = (datetime.now() - source_start_time).total_seconds()
        
        return results
    
    async def _process_onedrive_source(self, source_config, destination_config, job_config,
                                      last_backup_time: Optional[str] = None) -> Dict[str, Any]:
        """Process OneDrive personal source with incremental backup support.
        
        Args:
            source_config: OneDrive source configuration
            destination_config: Destination configuration
            job_config: Job configuration
            last_backup_time: ISO timestamp of last backup (None for full backup)
            
        Returns:
            Dictionary with processing results
        """
        import requests
        from dateutil import parser as date_parser

        from ..sources.onedrive_operations import OneDriveFileManager
        
        results = {
            'files_processed': 0,
            'files_uploaded': 0,
            'files_skipped': 0,
            'bytes_transferred': 0,
            'errors': []
        }
        
        # Parse last backup time for comparison
        last_backup_dt = None
        if last_backup_time:
            try:
                last_backup_dt = date_parser.parse(last_backup_time)
                logger.info(f"🔄 Incremental backup: Only files modified after {last_backup_time}")
            except Exception as e:
                logger.warning(f"Failed to parse last backup time: {e} - performing full backup")
        else:
            logger.info(f"📦 Full backup: No previous backup found")
        
        try:
            # Initialize OneDrive file manager
            onedrive_manager = OneDriveFileManager(self.microsoft_auth)
            
            # Get access token
            token = self.microsoft_auth.get_access_token()
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            # Get all users with OneDrive
            logger.info(f"Discovering users with OneDrive for: {source_config.name}")
            users_response = requests.get(
                'https://graph.microsoft.com/v1.0/users?$top=999',
                headers=headers
            )
            
            if users_response.status_code != 200:
                error_msg = f"Failed to list users: HTTP {users_response.status_code}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
                return results
            
            all_users = users_response.json().get('value', [])
            logger.info(f"Found {len(all_users)} total users in organization")
            
            # Filter users with OneDrive access
            users_with_onedrive = []
            for user in all_users:
                user_id = user.get('id')
                user_name = user.get('displayName', 'Unknown')
                user_email = user.get('mail') or user.get('userPrincipalName', 'N/A')
                
                # Check if user has OneDrive
                drive_info = onedrive_manager.get_user_onedrive_info(user_id)
                if drive_info:
                    users_with_onedrive.append({
                        'id': user_id,
                        'name': user_name,
                        'email': user_email,
                        'drive_id': drive_info['id']
                    })
                    logger.info(f"  ✓ {user_name} ({user_email}) has OneDrive")
                else:
                    logger.debug(f"  ✗ {user_name} ({user_email}) - no OneDrive access")
            
            logger.info(f"Found {len(users_with_onedrive)} users with accessible OneDrive")
            
            # Filter users based on configuration
            allowed_users = source_config.users
            if allowed_users != "all" and isinstance(allowed_users, list):
                # Filter to only specified users
                filtered_users = []
                for user_info in users_with_onedrive:
                    user_email = user_info['email'].lower()
                    if any(allowed.lower() == user_email for allowed in allowed_users):
                        filtered_users.append(user_info)
                
                logger.info(f"Filtered to {len(filtered_users)} users based on configuration: {allowed_users}")
                users_to_process = filtered_users
            else:
                logger.info(f"Processing all {len(users_with_onedrive)} users")
                users_to_process = users_with_onedrive
            
            # Process each user's OneDrive
            for user_info in users_to_process:
                try:
                    logger.info(f"Processing OneDrive for: {user_info['name']} ({user_info['email']})")
                    user_prefix = user_info['email'].split('@')[0]
                    
                    # Get delta token and timestamp for this user
                    delta_info = self._get_delta_token(source_config.name, user_info['id'], destination_config)
                    delta_token_url = delta_info.get('delta_token') if delta_info else None
                    fallback_timestamp = delta_info.get('last_backup_time') if delta_info else None
                    
                    # Stream files using Delta API (with hybrid fallback)
                    new_delta_token = None
                    async for file_info in self._stream_onedrive_files_delta(
                        user_info['id'], headers, user_prefix, delta_token_url, fallback_timestamp
                    ):
                        # Capture the new delta token from the last iteration
                        if isinstance(file_info, dict) and file_info.get('_delta_token'):
                            new_delta_token = file_info['_delta_token']
                            continue
                        try:
                            results['files_processed'] += 1
                            
                            file_path = file_info.get('path', file_info.get('name', ''))
                            file_size = file_info.get('size', 0)
                            modified_time = file_info.get('lastModifiedDateTime', '')
                            
                            # Check if file already exists in S3 with same modification time
                            if self._check_s3_file_exists(destination_config, file_path, modified_time):
                                logger.info(f"⏭️ Skipping (already backed up): {file_path}")
                                results['files_skipped'] += 1
                                continue
                            
                            # For dry run, just count
                            if getattr(job_config, 'dry_run', False):
                                logger.info(f"[DRY RUN] Would upload: {file_path} ({file_size:,} bytes)")
                                results['files_uploaded'] += 1
                                results['bytes_transferred'] += file_size
                                continue
                            
                            # Upload the file
                            logger.info(f"Uploading: {file_path} ({file_size:,} bytes)")
                            download_url = file_info.get('@microsoft.graph.downloadUrl', '')
                            
                            if download_url:
                                upload_result = await self._stream_upload_file(
                                    file_info, download_url, destination_config
                                )
                                
                                if upload_result.get('success', False):
                                    results['files_uploaded'] += 1
                                    results['bytes_transferred'] += file_size
                                    logger.info(f"✅ Uploaded: {file_path}")
                                else:
                                    error_msg = f"Upload failed for {file_path}: {upload_result.get('error')}"
                                    results['errors'].append(error_msg)
                                    logger.error(error_msg)
                            else:
                                error_msg = f"No download URL for {file_path}"
                                results['errors'].append(error_msg)
                                logger.error(error_msg)
                        
                        except Exception as e:
                            error_msg = f"Error processing file {file_info.get('name', 'unknown')}: {str(e)}"
                            results['errors'].append(error_msg)
                            logger.error(error_msg)
                    
                    # Save the new delta token for next backup
                    if new_delta_token and not getattr(job_config, 'dry_run', False):
                        self._save_delta_token(source_config.name, user_info['id'], new_delta_token, destination_config)
                        logger.info(f"✅ Saved delta token for next incremental backup")
                
                except Exception as e:
                    error_msg = f"Error processing OneDrive for {user_info.get('name', 'unknown')}: {str(e)}"
                    results['errors'].append(error_msg)
                    logger.error(error_msg)
            
        except Exception as e:
            logger.error(f"Error processing OneDrive source {source_config.name}: {e}")
            results['errors'].append(f"OneDrive source error: {str(e)}")
        
        return results
    
    async def _process_sharepoint_source(self, source_config, destination_config, job_config,
                                        last_backup_time: Optional[str] = None) -> Dict[str, Any]:
        """Process SharePoint source with incremental backup support.
        
        Args:
            source_config: SharePoint source configuration
            destination_config: Destination configuration
            job_config: Job configuration
            last_backup_time: ISO timestamp of last backup (None for full backup)
            
        Returns:
            Dictionary with processing results
        """
        import requests
        from dateutil import parser as date_parser
        
        results = {
            'files_processed': 0,
            'files_uploaded': 0,
            'files_skipped': 0,
            'bytes_transferred': 0,
            'errors': []
        }
        
        # Parse last backup time
        last_backup_dt = None
        if last_backup_time:
            try:
                last_backup_dt = date_parser.parse(last_backup_time)
                logger.info(f"🔄 Incremental backup: Only files modified after {last_backup_time}")
            except Exception as e:
                logger.warning(f"Failed to parse last backup time: {e} - performing full backup")
        else:
            logger.info(f"📦 Full backup: No previous backup found")
        
        try:
            # Get access token
            token = self.microsoft_auth.get_access_token()
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            # Get SharePoint drives
            logger.info(f"Fetching SharePoint drives for: {source_config.name}")
            drives_response = requests.get(
                'https://graph.microsoft.com/v1.0/sites/root/drives',
                headers=headers
            )
            
            if drives_response.status_code != 200:
                error_msg = f"Failed to get SharePoint drives: HTTP {drives_response.status_code}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
                return results
            
            drives = drives_response.json().get('value', [])
            logger.info(f"Found {len(drives)} SharePoint drives")
            
            # Process each drive
            for drive in drives:
                drive_name = drive.get('name', 'Unknown')
                drive_id = drive.get('id')
                
                logger.info(f"Processing drive: {drive_name}")
                
                # Get delta token and timestamp for this drive
                delta_info = self._get_delta_token(source_config.name, drive_id, destination_config)
                delta_token_url = delta_info.get('delta_token') if delta_info else None
                fallback_timestamp = delta_info.get('last_backup_time') if delta_info else None
                
                # Stream files using Delta API (with hybrid fallback)
                new_delta_token = None
                async for file_info in self._stream_sharepoint_files_delta(
                    drive_id, headers, drive_name, delta_token_url, fallback_timestamp
                ):
                    # Capture the new delta token from the last iteration
                    if isinstance(file_info, dict) and file_info.get('_delta_token'):
                        new_delta_token = file_info['_delta_token']
                        continue
                    try:
                        results['files_processed'] += 1
                        
                        file_path = file_info.get('path', file_info.get('name', ''))
                        file_size = file_info.get('size', 0)
                        modified_time = file_info.get('lastModifiedDateTime', '')
                        
                        # Full path including drive name for S3
                        full_s3_path = f"{drive_name}/{file_path}"
                        
                        # Check if file already exists in S3 with same modification time
                        if self._check_s3_file_exists(destination_config, full_s3_path, modified_time):
                            logger.info(f"⏭️ Skipping (already backed up): {full_s3_path}")
                            results['files_skipped'] += 1
                            continue
                        
                        # For dry run
                        if getattr(job_config, 'dry_run', False):
                            logger.info(f"[DRY RUN] Would upload: {full_s3_path} ({file_size:,} bytes)")
                            results['files_uploaded'] += 1
                            results['bytes_transferred'] += file_size
                            continue
                        
                        # Upload file
                        download_url = file_info.get('@microsoft.graph.downloadUrl', '')
                        if download_url:
                            logger.info(f"Uploading: {full_s3_path} ({file_size:,} bytes)")
                            
                            upload_result = await self._stream_upload_file(
                                {**file_info, 'path': full_s3_path},
                                download_url,
                                destination_config
                            )
                            
                            if upload_result.get('success', False):
                                results['files_uploaded'] += 1
                                results['bytes_transferred'] += file_size
                                logger.info(f"✅ Uploaded: {full_s3_path}")
                            else:
                                error_msg = f"Upload failed for {file_path}: {upload_result.get('error')}"
                                results['errors'].append(error_msg)
                                logger.error(error_msg)
                        else:
                            error_msg = f"No download URL for {file_path}"
                            results['errors'].append(error_msg)
                            logger.error(error_msg)
                    
                    except Exception as e:
                        error_msg = f"Error processing file {file_info.get('name', 'unknown')}: {str(e)}"
                        results['errors'].append(error_msg)
                        logger.error(error_msg)
                
                # Save the new delta token for next backup
                if new_delta_token and not getattr(job_config, 'dry_run', False):
                    self._save_delta_token(source_config.name, drive_id, new_delta_token, destination_config)
                    logger.info(f"✅ Saved delta token for drive {drive_name}")
            
        except Exception as e:
            error_msg = f"Error processing SharePoint source {source_config.name}: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
        
        return results
    
    async def _stream_onedrive_files_delta(self, user_id: str, headers: Dict[str, str],
                                           user_prefix: str = "", delta_token: Optional[str] = None,
                                           fallback_timestamp: Optional[str] = None):
        """Stream files from OneDrive using Delta API with timestamp fallback.
        
        Hybrid approach:
        1. Try delta token first (fast - only changed files)
        2. If delta token expired (HTTP 410), fall back to recursive scan with timestamp filtering
        
        Args:
            user_id: User ID
            headers: Authentication headers
            user_prefix: User prefix for paths
            delta_token: Delta link from previous sync (None for initial sync)
            fallback_timestamp: ISO timestamp for fallback filtering if delta token expires
            
        Yields:
            File information dictionaries, and a final dict with '_delta_token' key containing
            the new delta link for the next sync
        """
        import requests
        from dateutil import parser as date_parser
        
        try:
            # Use delta token if available, otherwise start fresh
            if delta_token:
                endpoint = delta_token
                logger.info(f"🔄 Using delta API for incremental sync (user: {user_id[:8]}...)")
            else:
                endpoint = f'https://graph.microsoft.com/v1.0/users/{user_id}/drive/root/delta'
                logger.info(f"📦 Using delta API for initial sync (user: {user_id[:8]}...)")
            
            files_found = 0
            
            while endpoint:
                response = requests.get(endpoint, headers=headers)
                
                # Handle delta token expiration
                if response.status_code == 410:
                    logger.warning(f"⚠️ Delta token expired for user {user_id[:8]}...")
                    
                    # Fall back to timestamp-based filtering if available
                    if fallback_timestamp:
                        try:
                            fallback_dt = date_parser.parse(fallback_timestamp)
                            logger.info(f"📅 Falling back to timestamp filter: files modified after {fallback_timestamp}")
                            
                            # Use recursive method with timestamp filtering
                            async for file_info in self._stream_onedrive_files_recursive(
                                user_id, headers, folder_id='root', user_prefix=user_prefix,
                                modified_after=fallback_dt
                            ):
                                yield file_info
                            
                            # Start fresh delta sync for next time
                            logger.info(f"🔄 Initiating fresh delta sync to get new token...")
                            fresh_endpoint = f'https://graph.microsoft.com/v1.0/users/{user_id}/drive/root/delta'
                            fresh_response = requests.get(fresh_endpoint, headers=headers)
                            
                            if fresh_response.status_code == 200:
                                fresh_data = fresh_response.json()
                                # Navigate through all pages to get the final delta link
                                while True:
                                    next_link = fresh_data.get('@odata.nextLink')
                                    delta_link = fresh_data.get('@odata.deltaLink')
                                    
                                    if delta_link:
                                        yield {'_delta_token': delta_link}
                                        break
                                    elif next_link:
                                        fresh_response = requests.get(next_link, headers=headers)
                                        fresh_data = fresh_response.json()
                                    else:
                                        break
                            
                            return
                            
                        except Exception as e:
                            logger.error(f"Failed to use timestamp fallback: {e}")
                            logger.info(f"📦 Starting complete fresh delta sync...")
                            # Fall through to fresh sync below
                    
                    # If no fallback timestamp or it failed, start completely fresh
                    endpoint = f'https://graph.microsoft.com/v1.0/users/{user_id}/drive/root/delta'
                    logger.info(f"📦 Restarting with fresh delta sync (no fallback available)")
                    continue
                
                elif response.status_code != 200:
                    logger.error(f"Delta API error: HTTP {response.status_code}")
                    break
                
                data = response.json()
                items = data.get('value', [])
                
                # Process items
                for item in items:
                    # Skip deleted items
                    if item.get('deleted'):
                        logger.debug(f"Skipping deleted item: {item.get('name', 'unknown')}")
                        continue
                    
                    # Skip folders (we only backup files)
                    if item.get('folder'):
                        continue
                    
                    # Only yield files (not folders)
                    if item.get('file'):
                        files_found += 1
                        name = item.get('name', '')
                        item_id = item.get('id', '')
                        
                        # Build path from parentReference
                        parent_ref = item.get('parentReference', {})
                        parent_path = parent_ref.get('path', '').replace('/drive/root:', '').strip('/')
                        
                        if parent_path:
                            full_path = f"{user_prefix}/{parent_path}/{name}"
                        else:
                            full_path = f"{user_prefix}/{name}"
                        
                        # Get download URL (delta API doesn't always include it)
                        download_url = item.get('@microsoft.graph.downloadUrl', '')
                        
                        # If no download URL, fetch it separately
                        if not download_url and item_id:
                            try:
                                item_response = requests.get(
                                    f'https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{item_id}',
                                    headers=headers
                                )
                                if item_response.status_code == 200:
                                    item_data = item_response.json()
                                    download_url = item_data.get('@microsoft.graph.downloadUrl', '')
                            except Exception as e:
                                logger.warning(f"Failed to get download URL for {name}: {e}")
                        
                        yield {
                            'id': item_id,
                            'name': name,
                            'path': full_path,
                            'size': item.get('size', 0),
                            'lastModifiedDateTime': item.get('lastModifiedDateTime', ''),
                            'mimeType': item.get('file', {}).get('mimeType', 'application/octet-stream'),
                            '@microsoft.graph.downloadUrl': download_url
                        }
                
                # Check for next page or delta link
                next_link = data.get('@odata.nextLink')
                delta_link = data.get('@odata.deltaLink')
                
                if next_link:
                    # More pages to fetch
                    endpoint = next_link
                elif delta_link:
                    # No more pages, save delta link for next sync
                    logger.info(f"✅ Delta sync complete: {files_found} files found")
                    # Yield the delta token as a special marker
                    yield {'_delta_token': delta_link}
                    break
                else:
                    # No more data
                    break
                    
        except Exception as e:
            logger.error(f"Error in delta API streaming: {e}")
    
    async def _stream_onedrive_files_recursive(self, user_id: str, headers: Dict[str, str],
                                               folder_id: str = "root", user_prefix: str = "",
                                               path: str = "", depth: int = 0, max_depth: int = 10,
                                               modified_after: Optional[datetime] = None):
        """Stream files from OneDrive with timestamp filtering.
        
        NOTE: This method is deprecated in favor of _stream_onedrive_files_delta which uses
        the Delta API for more efficient change tracking.
        
        Args:
            user_id: User ID
            headers: Authentication headers
            folder_id: Folder ID
            user_prefix: User prefix for paths
            path: Current path
            depth: Current depth
            max_depth: Maximum recursion depth
            modified_after: Only yield files modified after this datetime
            
        Yields:
            File information dictionaries
        """
        import requests
        from dateutil import parser as date_parser
        
        if depth > max_depth:
            return
        
        try:
            if folder_id == "root":
                endpoint = f'https://graph.microsoft.com/v1.0/users/{user_id}/drive/root/children'
            else:
                endpoint = f'https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{folder_id}/children'
            
            # Add filter parameter if we have a timestamp (API-level filtering)
            if modified_after:
                # Format timestamp for OData filter: 2024-01-01T00:00:00Z
                filter_time = modified_after.strftime('%Y-%m-%dT%H:%M:%SZ')
                endpoint += f"?$filter=lastModifiedDateTime gt {filter_time}"
                logger.debug(f"Using API filter: lastModifiedDateTime > {filter_time}")
            
            response = requests.get(endpoint, headers=headers)
            
            if response.status_code == 200:
                items = response.json().get('value', [])
                
                for item in items:
                    name = item.get('name', '')
                    item_id = item.get('id', '')
                    full_path = f"{path}/{name}" if path else name
                    full_path_with_user = f"{user_prefix}/{full_path}"
                    
                    if item.get('folder'):
                        # Recursively process subdirectories
                        async for file_info in self._stream_onedrive_files_recursive(
                            user_id, headers, item_id, user_prefix, full_path, depth + 1, max_depth,
                            modified_after
                        ):
                            yield file_info
                    else:
                        # Yield file (already filtered by API if modified_after was set)
                        yield {
                            'id': item_id,
                            'name': name,
                            'path': full_path_with_user,
                            'size': item.get('size', 0),
                            'lastModifiedDateTime': item.get('lastModifiedDateTime', ''),
                            'mimeType': item.get('file', {}).get('mimeType', 'application/octet-stream'),
                            '@microsoft.graph.downloadUrl': item.get('@microsoft.graph.downloadUrl', '')
                        }
            elif response.status_code == 400 and modified_after:
                # If API filter fails, fall back to client-side filtering
                logger.warning(f"API filter not supported, falling back to client-side filtering")
                endpoint_no_filter = endpoint.split('?')[0]
                response = requests.get(endpoint_no_filter, headers=headers)
                
                if response.status_code == 200:
                    items = response.json().get('value', [])
                    
                    for item in items:
                        name = item.get('name', '')
                        item_id = item.get('id', '')
                        full_path = f"{path}/{name}" if path else name
                        full_path_with_user = f"{user_prefix}/{full_path}"
                        
                        if item.get('folder'):
                            async for file_info in self._stream_onedrive_files_recursive(
                                user_id, headers, item_id, user_prefix, full_path, depth + 1, max_depth,
                                modified_after
                            ):
                                yield file_info
                        else:
                            # Client-side filtering
                            try:
                                file_modified = date_parser.parse(item.get('lastModifiedDateTime', ''))
                                if file_modified <= modified_after:
                                    continue
                            except Exception:
                                pass
                            
                            yield {
                                'id': item_id,
                                'name': name,
                                'path': full_path_with_user,
                                'size': item.get('size', 0),
                                'lastModifiedDateTime': item.get('lastModifiedDateTime', ''),
                                'mimeType': item.get('file', {}).get('mimeType', 'application/octet-stream'),
                                '@microsoft.graph.downloadUrl': item.get('@microsoft.graph.downloadUrl', '')
                            }
        
        except Exception as e:
            logger.error(f"Error listing OneDrive folder for user {user_id}: {e}")
    
    async def _stream_sharepoint_files_delta(self, drive_id: str, headers: Dict[str, str],
                                             drive_name: str = "", delta_token: Optional[str] = None,
                                             fallback_timestamp: Optional[str] = None):
        """Stream files from SharePoint using Delta API with timestamp fallback.
        
        Hybrid approach:
        1. Try delta token first (fast - only changed files)
        2. If delta token expired (HTTP 410), fall back to recursive scan with timestamp filtering
        
        Args:
            drive_id: Drive ID
            headers: Authentication headers
            drive_name: Drive name for path construction
            delta_token: Delta link from previous sync (None for initial sync)
            fallback_timestamp: ISO timestamp for fallback filtering if delta token expires
            
        Yields:
            File information dictionaries, and a final dict with '_delta_token' key containing
            the new delta link for the next sync
        """
        import requests
        from dateutil import parser as date_parser
        
        try:
            # Use delta token if available, otherwise start fresh
            if delta_token:
                endpoint = delta_token
                logger.info(f"🔄 Using delta API for incremental sync (drive: {drive_name})")
            else:
                endpoint = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/delta'
                logger.info(f"📦 Using delta API for initial sync (drive: {drive_name})")
            
            files_found = 0
            
            while endpoint:
                response = requests.get(endpoint, headers=headers)
                
                # Handle delta token expiration
                if response.status_code == 410:
                    logger.warning(f"⚠️ Delta token expired for drive {drive_name}")
                    
                    # Fall back to timestamp-based filtering if available
                    if fallback_timestamp:
                        try:
                            fallback_dt = date_parser.parse(fallback_timestamp)
                            logger.info(f"📅 Falling back to timestamp filter: files modified after {fallback_timestamp}")
                            
                            # Use recursive method with timestamp filtering
                            async for file_info in self._stream_sharepoint_files_recursive(
                                drive_id, headers, folder_id='root', path="",
                                modified_after=fallback_dt
                            ):
                                yield file_info
                            
                            # Start fresh delta sync for next time
                            logger.info(f"🔄 Initiating fresh delta sync to get new token...")
                            fresh_endpoint = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/delta'
                            fresh_response = requests.get(fresh_endpoint, headers=headers)
                            
                            if fresh_response.status_code == 200:
                                fresh_data = fresh_response.json()
                                # Navigate through all pages to get the final delta link
                                while True:
                                    next_link = fresh_data.get('@odata.nextLink')
                                    delta_link = fresh_data.get('@odata.deltaLink')
                                    
                                    if delta_link:
                                        yield {'_delta_token': delta_link}
                                        break
                                    elif next_link:
                                        fresh_response = requests.get(next_link, headers=headers)
                                        fresh_data = fresh_response.json()
                                    else:
                                        break
                            
                            return
                            
                        except Exception as e:
                            logger.error(f"Failed to use timestamp fallback: {e}")
                            logger.info(f"📦 Starting complete fresh delta sync...")
                    
                    # If no fallback timestamp or it failed, start completely fresh
                    endpoint = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/delta'
                    logger.info(f"📦 Restarting with fresh delta sync (no fallback available)")
                    continue
                
                elif response.status_code != 200:
                    logger.error(f"Delta API error: HTTP {response.status_code}")
                    break
                
                data = response.json()
                items = data.get('value', [])
                
                # Process items
                for item in items:
                    # Skip deleted items
                    if item.get('deleted'):
                        logger.debug(f"Skipping deleted item: {item.get('name', 'unknown')}")
                        continue
                    
                    # Skip folders (we only backup files)
                    if item.get('folder'):
                        continue
                    
                    # Only yield files (not folders)
                    if item.get('file'):
                        files_found += 1
                        name = item.get('name', '')
                        item_id = item.get('id', '')
                        
                        # Build path from parentReference
                        parent_ref = item.get('parentReference', {})
                        parent_path = parent_ref.get('path', '').replace('/drive/root:', '').strip('/')
                        
                        if parent_path:
                            full_path = f"{parent_path}/{name}"
                        else:
                            full_path = name
                        
                        # Get download URL (delta API doesn't always include it)
                        download_url = item.get('@microsoft.graph.downloadUrl', '')
                        
                        # If no download URL, fetch it separately
                        if not download_url and item_id:
                            try:
                                item_response = requests.get(
                                    f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}',
                                    headers=headers
                                )
                                if item_response.status_code == 200:
                                    item_data = item_response.json()
                                    download_url = item_data.get('@microsoft.graph.downloadUrl', '')
                            except Exception as e:
                                logger.warning(f"Failed to get download URL for {name}: {e}")
                        
                        yield {
                            'id': item_id,
                            'name': name,
                            'path': full_path,
                            'size': item.get('size', 0),
                            'lastModifiedDateTime': item.get('lastModifiedDateTime', ''),
                            'mimeType': item.get('file', {}).get('mimeType', 'application/octet-stream'),
                            '@microsoft.graph.downloadUrl': download_url
                        }
                
                # Check for next page or delta link
                next_link = data.get('@odata.nextLink')
                delta_link = data.get('@odata.deltaLink')
                
                if next_link:
                    # More pages to fetch
                    endpoint = next_link
                elif delta_link:
                    # No more pages, save delta link for next sync
                    logger.info(f"✅ Delta sync complete: {files_found} files found")
                    # Yield the delta token as a special marker
                    yield {'_delta_token': delta_link}
                    break
                else:
                    # No more data
                    break
                    
        except Exception as e:
            logger.error(f"Error in SharePoint delta API streaming: {e}")
    
    async def _stream_sharepoint_files_recursive(self, drive_id: str, headers: Dict[str, str],
                                                 folder_id: str = "root", path: str = "", 
                                                 depth: int = 0, max_depth: int = 10,
                                                 modified_after: Optional[datetime] = None):
        """Stream files from SharePoint with optional timestamp filtering.
        
        Uses Microsoft Graph API $filter query to retrieve only modified files when possible,
        falling back to client-side filtering for nested folders.
        
        Args:
            drive_id: Drive ID
            headers: Authentication headers
            folder_id: Folder ID
            path: Current path
            depth: Current depth
            max_depth: Maximum recursion depth
            modified_after: Only yield files modified after this datetime
            
        Yields:
            File information dictionaries
        """
        import requests
        from dateutil import parser as date_parser
        
        if depth > max_depth:
            return
        
        try:
            if folder_id == "root":
                endpoint = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children'
            else:
                endpoint = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}/children'
            
            # Add filter parameter if we have a timestamp (API-level filtering)
            if modified_after:
                filter_time = modified_after.strftime('%Y-%m-%dT%H:%M:%SZ')
                endpoint += f"?$filter=lastModifiedDateTime gt {filter_time}"
                logger.debug(f"Using API filter: lastModifiedDateTime > {filter_time}")
            
            response = requests.get(endpoint, headers=headers)
            
            if response.status_code == 200:
                items = response.json().get('value', [])
                
                for item in items:
                    name = item.get('name', '')
                    item_id = item.get('id', '')
                    full_path = f"{path}/{name}" if path else name
                    
                    if item.get('folder'):
                        # Recursively process subdirectories
                        async for file_info in self._stream_sharepoint_files_recursive(
                            drive_id, headers, item_id, full_path, depth + 1, max_depth,
                            modified_after
                        ):
                            yield file_info
                    else:
                        # Yield file (already filtered by API if modified_after was set)
                        yield {
                            'id': item_id,
                            'name': name,
                            'path': full_path,
                            'size': item.get('size', 0),
                            'lastModifiedDateTime': item.get('lastModifiedDateTime', ''),
                            'mimeType': item.get('file', {}).get('mimeType', 'application/octet-stream'),
                            '@microsoft.graph.downloadUrl': item.get('@microsoft.graph.downloadUrl', '')
                        }
            elif response.status_code == 400 and modified_after:
                # If API filter fails, fall back to client-side filtering
                logger.warning(f"SharePoint API filter not supported, falling back to client-side filtering")
                endpoint_no_filter = endpoint.split('?')[0]
                response = requests.get(endpoint_no_filter, headers=headers)
                
                if response.status_code == 200:
                    items = response.json().get('value', [])
                    
                    for item in items:
                        name = item.get('name', '')
                        item_id = item.get('id', '')
                        full_path = f"{path}/{name}" if path else name
                        
                        if item.get('folder'):
                            async for file_info in self._stream_sharepoint_files_recursive(
                                drive_id, headers, item_id, full_path, depth + 1, max_depth,
                                modified_after
                            ):
                                yield file_info
                        else:
                            # Client-side filtering
                            try:
                                file_modified = date_parser.parse(item.get('lastModifiedDateTime', ''))
                                if file_modified <= modified_after:
                                    continue
                            except Exception:
                                pass
                            
                            yield {
                                'id': item_id,
                                'name': name,
                                'path': full_path,
                                'size': item.get('size', 0),
                                'lastModifiedDateTime': item.get('lastModifiedDateTime', ''),
                                'mimeType': item.get('file', {}).get('mimeType', 'application/octet-stream'),
                                '@microsoft.graph.downloadUrl': item.get('@microsoft.graph.downloadUrl', '')
                            }
        
        except Exception as e:
            logger.error(f"Error listing SharePoint folder: {e}")
    
    async def _stream_upload_file(self, file_info: Dict[str, Any], download_url: str, 
                                 destination_config) -> Dict[str, Any]:
        """Stream upload a file to destination.
        
        Args:
            file_info: File information
            download_url: Download URL
            destination_config: Destination configuration
            
        Returns:
            Upload result dictionary
        """
        try:
            file_path = file_info.get('path', file_info.get('name', ''))
            file_size = file_info.get('size', 0)
            content_type = file_info.get('mimeType', 'application/octet-stream')
            
            if destination_config.type == 'aws_s3':
                return await self._stream_to_aws_s3(
                    file_path, download_url, file_size, content_type, destination_config, file_info
                )
            else:
                return {
                    'success': False,
                    'error': f"Unsupported destination type: {destination_config.type}"
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': f"Stream upload error: {str(e)}"
            }
    
    async def _stream_to_aws_s3(self, file_path: str, download_url: str, file_size: int, 
                               content_type: str, destination_config, 
                               file_info: Dict[str, Any] = None) -> Dict[str, Any]:
        """Stream file to AWS S3.
        
        Args:
            file_path: File path for storage
            download_url: Download URL
            file_size: File size
            content_type: Content type
            destination_config: Destination configuration
            file_info: File information
            
        Returns:
            Upload result dictionary
        """
        try:
            import base64
            import io

            import requests

            s3_client = self.aws_auth.get_s3_client()
            
            prefix = getattr(destination_config, 'prefix', '')
            s3_key = f"{prefix}{file_path}".lstrip('/')
            
            response = requests.get(download_url, stream=True)
            
            if response.status_code == 200:
                encoded_path = base64.b64encode(file_path.encode('utf-8')).decode('ascii')
                modified_time = file_info.get('lastModifiedDateTime', '') if file_info else ''
                
                s3_client.upload_fileobj(
                    Fileobj=io.BytesIO(response.content),
                    Bucket=destination_config.bucket,
                    Key=s3_key,
                    ExtraArgs={
                        'ContentType': content_type,
                        'Metadata': {
                            'original-path-encoded': encoded_path,
                            'source': 'onedrive-backup',
                            'encoding': 'base64-utf8',
                            'source-modified-time': modified_time
                        }
                    }
                )
                
                return {
                    'success': True,
                    'bucket': destination_config.bucket,
                    'key': s3_key,
                    'size': file_size
                }
            else:
                return {
                    'success': False,
                    'error': f"Failed to download: HTTP {response.status_code}"
                }
        
        except Exception as e:
            return {
                'success': False,
                'error': f"AWS S3 upload error: {str(e)}"
            }
    
    async def run_all_jobs(self) -> List[Dict[str, Any]]:
        """Run all enabled backup jobs.
        
        Returns:
            List of job results
        """
        enabled_jobs = self.config.get_enabled_jobs()
        logger.info(f"Running {len(enabled_jobs)} backup jobs")
        
        results = []
        for job in enabled_jobs:
            job_result = await self.run_backup_job(job)
            results.append(job_result)
        
        return results
    
    def test_connections(self) -> Dict[str, bool]:
        """Test all configured connections.
        
        Returns:
            Dictionary mapping service names to connection status
        """
        results = {}
        
        if self.microsoft_auth:
            results['microsoft_graph'] = self.microsoft_auth.test_connection()
        
        if self.aws_auth:
            for dest in self.config.destinations:
                if dest.type == 'aws_s3':
                    results[f'aws_s3_{dest.name}'] = self.aws_auth.test_connection(dest.bucket)
        
        if self.azure_auth:
            for dest in self.config.destinations:
                if dest.type == 'azure_blob':
                    results[f'azure_blob_{dest.name}'] = self.azure_auth.test_connection(dest.container)
        
        return results
    
    def get_backup_summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate summary of backup results.
        
        Args:
            results: List of job results
            
        Returns:
            Summary dictionary
        """
        total_files_processed = sum(r.get('files_processed', 0) for r in results)
        total_files_uploaded = sum(r.get('files_uploaded', 0) for r in results)
        total_files_skipped = sum(r.get('files_skipped', 0) for r in results)
        total_bytes_transferred = sum(r.get('bytes_transferred', 0) for r in results)
        total_errors = sum(len(r.get('errors', [])) for r in results)
        
        successful_jobs = len([r for r in results if r.get('status') == 'completed'])
        failed_jobs = len([r for r in results if r.get('status') == 'failed'])
        
        return {
            'total_jobs': len(results),
            'successful_jobs': successful_jobs,
            'failed_jobs': failed_jobs,
            'total_files_processed': total_files_processed,
            'total_files_uploaded': total_files_uploaded,
            'total_files_skipped': total_files_skipped,
            'total_bytes_transferred': total_bytes_transferred,
            'total_errors': total_errors,
            'backup_time': datetime.now().isoformat()
        }
