"""Main backup manager orchestrating the backup process."""

import asyncio
import json
import logging
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..auth.cloud_auth import AWSAuth, AzureAuth
from ..auth.microsoft_auth import MicrosoftGraphAuth
from ..config.settings import BackupConfig, BackupJobConfig

# Module logger
logger = logging.getLogger(__name__)


# Sentinel value to signal end of queue
_SENTINEL = object()
times = 0
class FileQueueManager:
    """Thread-safe manager for file download/upload queue."""
    
    def __init__(self, max_workers: int = 5):
        """Initialize queue manager.
        
        Args:
            max_workers: Maximum number of parallel workers
        """
        # Set queue size to max_workers + 1 to limit memory usage
        self.file_queue: queue.Queue = queue.Queue(maxsize=max_workers*2)
        self.results_lock = threading.Lock()
        self.max_workers = max_workers
        self.stop_event = threading.Event()
        # Statistics tracking
        self.files_processed = 0
        self.files_uploaded = 0
        self.files_skipped = 0
        self.bytes_transferred = 0
        self.errors = []
    
    def add_file(self, file_info: Dict[str, Any], timeout: Optional[float] = None) -> bool:
        """Add file to processing queue. Blocks if queue is full.
        
        Args:
            file_info: File information dictionary
            timeout: Maximum time to wait if queue is full
            
        Returns:
            True if file was added, False if timeout occurred
        """
        try:
            logger.info(f"Adding file to queue: {file_info.get('name', 'unknown')}")
            # Block with timeout to avoid deadlock
            self.file_queue.put(file_info, block=True, timeout=timeout)
            return True
        except queue.Full:
            logger.warning(f"Queue full, waiting to add: {file_info.get('name', 'unknown')}")
            # Retry with longer timeout
            try:
                self.file_queue.put(file_info, block=True, timeout=timeout * 2)
                return True
            except queue.Full:
                logger.error(f"Failed to add file to queue after {timeout * 3}s: {file_info.get('name', 'unknown')}")
                return False
    
    def get_next_file(self, timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """Get next file from queue (thread-safe).
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            File info dict, _SENTINEL to signal end, or None if queue is empty
        """
        try:
            
            item = self.file_queue.get(timeout=timeout)
            return item
        except queue.Empty:
            return None
    
    def signal_done(self):
        """Signal that no more files will be added by sending sentinel values."""
        for _ in range(self.max_workers):
            self.file_queue.put(_SENTINEL)
    
    def mark_processed(self):
        """Mark current file as processed."""
        self.file_queue.task_done()
    
    def update_stats(self, uploaded: bool = False, skipped: bool = False, 
                    bytes_transferred: int = 0, error: Optional[str] = None):
        """Update statistics (thread-safe).
        
        Args:
            uploaded: Whether file was uploaded
            skipped: Whether file was skipped
            bytes_transferred: Bytes transferred
            error: Error message if any
        """
        # with self.results_lock:
        self.files_processed += 1
        if uploaded:
            self.files_uploaded += 1
            self.bytes_transferred += bytes_transferred
        if skipped:
            self.files_skipped += 1
        if error:
            self.errors.append(error)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics (thread-safe).
        
        Returns:
            Statistics dictionary
        """
        with self.results_lock:
            return {
                'files_processed': self.files_processed,
                'files_uploaded': self.files_uploaded,
                'files_skipped': self.files_skipped,
                'bytes_transferred': self.bytes_transferred,
                'errors': self.errors.copy()
            }
    
    def stop(self):
        """Signal workers to stop."""
        self.stop_event.set()
    
    def should_stop(self) -> bool:
        """Check if workers should stop."""
        return self.stop_event.is_set()


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
        
        # Parallel processing configuration
        self.max_parallel_workers = getattr(config, 'max_parallel_workers', 20)
        
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
            
            # Try to get object metadata with retry on 401
            try:
                response = s3_client.head_object(
                    Bucket=destination_config.bucket,
                    Key=s3_key
                )
            except s3_client.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '401' or e.response['Error']['Code'] == 'ExpiredToken':
                    logger.info(f"AWS credentials expired, refreshing...")
                    s3_client = self.aws_auth.refresh_credentials()
                    response = s3_client.head_object(
                        Bucket=destination_config.bucket,
                        Key=s3_key
                    )
                else:
                    raise
            
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
    
    def run_backup_job(self, job_config: BackupJobConfig) -> Dict[str, Any]:
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
                source_results = self._process_source(source, destination, job_config)
                
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
    
    def _process_source(self, source_config, destination_config, job_config) -> Dict[str, Any]:
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

            # Handle OneDrive sources
            if source_config.type == 'onedrive_personal':
                results = self._process_onedrive_source(
                    source_config, destination_config, job_config)
            elif source_config.type == 'sharepoint':
                results = self._process_sharepoint_source(
                    source_config, destination_config, job_config)
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
    
    def _parallel_upload_worker(self, queue_manager: FileQueueManager, 
                                destination_config, job_config, worker_id: int):
        """Worker thread that processes files from queue.
        
        Args:
            queue_manager: Thread-safe queue manager
            destination_config: Destination configuration
            job_config: Job configuration
            worker_id: Worker thread ID
        """
        logger.info(f"Worker {worker_id} started")
        
        while not queue_manager.should_stop():
            file_info = queue_manager.get_next_file(timeout=40.0)
            
            if file_info is None:
                logger.info(f"Worker {worker_id} timed out waiting for file, checking again...")
                break
            
            # Check for sentinel value signaling end of queue
            if file_info is _SENTINEL:
                logger.info(f"Worker {worker_id} received sentinel, exiting")
                break
            
            try:
                file_path = file_info.get('path', file_info.get('name', ''))
                file_size = file_info.get('size', 0)
                modified_time = file_info.get('lastModifiedDateTime', '')
                
                # Check if file already exists in S3 with same modification time
                if self._check_s3_file_exists(destination_config, file_path, modified_time):
                    # logger.info(f"⏭️ [Worker {worker_id}] Skipping (already backed up): {file_path}")
                    queue_manager.update_stats(skipped=True)
                    continue
                
                # For dry run
                if getattr(job_config, 'dry_run', False):
                    logger.info(f"[DRY RUN] [Worker {worker_id}] Would upload: {file_path} ({file_size:,} bytes)")
                    queue_manager.update_stats(uploaded=True, bytes_transferred=file_size)
                    continue
                
                # Download and upload file
                download_url = file_info.get('@microsoft.graph.downloadUrl', '')
                
                if not download_url:
                    error_msg = f"No download URL for {file_path}"
                    logger.error(f"[Worker {worker_id}] {error_msg}")
                    queue_manager.update_stats(error=error_msg)
                    continue
                
                logger.info(f"[Worker {worker_id}] Uploading: {file_path} ({file_size:,} bytes)")
                
                upload_result = self._stream_upload_file(file_info, download_url, destination_config)
                
                if upload_result.get('success', False):
                    queue_manager.update_stats(uploaded=True, bytes_transferred=file_size)
                    logger.info(f"✅ [Worker {worker_id}] Uploaded: {file_path}")
                else:
                    error_msg = f"Upload failed for {file_path}: {upload_result.get('error')}"
                    logger.error(f"[Worker {worker_id}] {error_msg}")
                    queue_manager.update_stats(error=error_msg)
                
            except Exception as e:
                error_msg = f"Error processing file {file_info.get('name', 'unknown')}: {str(e)}"
                logger.error(f"[Worker {worker_id}] {error_msg}")
                queue_manager.update_stats(error=error_msg)
        
        logger.debug(f"Worker {worker_id} stopped")
    
    def _process_items_with_delta(self, items_to_process, source_config, destination_config, 
                                   job_config, stream_files_func) -> Dict[str, Any]:
        """Process items (users or drives) with parallel workers and delta sync.
        
        This is a shared method used by both OneDrive and SharePoint sources.
        
        Args:
            items_to_process: List of items (users or drives) to process
            source_config: Source configuration
            destination_config: Destination configuration
            job_config: Job configuration
            stream_files_func: Function to stream files for each item
            
        Returns:
            Dictionary with processing results
        """
        results = {
            'files_processed': 0,
            'files_uploaded': 0,
            'files_skipped': 0,
            'bytes_transferred': 0,
            'errors': []
        }
        
        # Get fresh headers
        token = self.microsoft_auth.get_access_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        # Process each item with parallel workers
        for item_info in items_to_process:
            try:
                item_id = item_info['id']
                item_name = item_info['name']
                
                logger.info(f"Processing: {item_name}")
                logger.info(f"Using {self.max_parallel_workers} parallel workers")
                
                # Get delta token and timestamp for this item
                delta_info = self._get_delta_token(source_config.name, item_id, destination_config)
                delta_token_url = delta_info.get('delta_token') if delta_info else None
                fallback_timestamp = delta_info.get('last_backup_time') if delta_info else None
                
                # Create queue manager for this item
                queue_manager = FileQueueManager(max_workers=self.max_parallel_workers)
                
                # Track final delta token
                final_delta_token = None
                
                # Start worker threads
                with ThreadPoolExecutor(max_workers=self.max_parallel_workers) as executor:
                    # Submit worker tasks
                    worker_futures = [
                        executor.submit(
                            self._parallel_upload_worker,
                            queue_manager,
                            destination_config,
                            job_config,
                            i
                        )
                        for i in range(self.max_parallel_workers)
                    ]
                    logger.info(f"Started {self.max_parallel_workers} worker threads for {item_name}")
                    
                    # Producer: Stream files from Delta API and add to queue
                    for file_info in stream_files_func(item_info, headers, delta_token_url, fallback_timestamp):
                        # Capture final delta token (arrives only at the very end)
                        if isinstance(file_info, dict) and file_info.get('_delta_token'):
                            final_delta_token = file_info['_delta_token']
                            continue
                        
                        # Add file to queue
                        queue_manager.add_file(file_info)
                    
                    # Save final delta token if we have one
                    if final_delta_token and not getattr(job_config, 'dry_run', False):
                        self._save_delta_token(source_config.name, item_id, final_delta_token, destination_config)
                        logger.info(f"✅ Delta token saved (incremental sync will resume from this point)")
                    
                    logger.info(f"Producer finished adding files for {item_name}")
                    # Producer finished - signal workers that no more files are coming
                    logger.debug(f"Producer finished for {item_name}, sending sentinel values")
                    queue_manager.signal_done()
                    
                    # Wait for all workers to complete
                    for future in as_completed(worker_futures):
                        try:
                            future.result()
                        except Exception as e:
                            logger.error(f"Worker thread error: {e}")
                
                # Aggregate results from queue manager
                item_stats = queue_manager.get_stats()
                results['files_processed'] += item_stats['files_processed']
                results['files_uploaded'] += item_stats['files_uploaded']
                results['files_skipped'] += item_stats['files_skipped']
                results['bytes_transferred'] += item_stats['bytes_transferred']
                results['errors'].extend(item_stats['errors'])
                
                logger.info(f"Completed {item_name}: {item_stats['files_uploaded']} uploaded, "
                           f"{item_stats['files_skipped']} skipped, {item_stats['files_processed']} total")
            
            except Exception as e:
                error_msg = f"Error processing {item_info.get('name', 'unknown')}: {str(e)}"
                results['errors'].append(error_msg)
                logger.error(error_msg)
        
        return results

    def _process_onedrive_source(self, source_config, destination_config, job_config) -> Dict[str, Any]:
        """Process OneDrive personal source with parallel incremental backup support.
        
        Args:
            source_config: OneDrive source configuration
            destination_config: Destination configuration
            job_config: Job configuration
            
        Returns:
            Dictionary with processing results
        """
        import requests

        from ..sources.onedrive_operations import OneDriveFileManager
        
        results = {
            'files_processed': 0,
            'files_uploaded': 0,
            'files_skipped': 0,
            'bytes_transferred': 0,
            'errors': []
        }
      
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
            
            # Define streaming function for OneDrive users
            def stream_onedrive_user_files(user_info, headers, delta_token_url, fallback_timestamp):
                user_prefix = user_info['email'].split('@')[0]
                return self._stream_onedrive_files_delta(
                    user_info['id'], headers, user_prefix, delta_token_url, fallback_timestamp
                )
            
            # Process all users with shared logic
            results = self._process_items_with_delta(
                users_to_process, source_config, destination_config, job_config,
                stream_onedrive_user_files
            )
            
        except Exception as e:
            logger.error(f"Error processing OneDrive source {source_config.name}: {e}")
            results['errors'].append(f"OneDrive source error: {str(e)}")
        
        return results
    
    def _process_sharepoint_source(self, source_config, destination_config, job_config) -> Dict[str, Any]:
        """Process SharePoint source with parallel incremental backup support.
        
        Args:
            source_config: SharePoint source configuration
            destination_config: Destination configuration
            job_config: Job configuration
            
        Returns:
            Dictionary with processing results
        """
        import requests
        
        results = {
            'files_processed': 0,
            'files_uploaded': 0,
            'files_skipped': 0,
            'bytes_transferred': 0,
            'errors': []
        }
        
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
            
            # Convert drives to common format
            drives_to_process = [
                {
                    'id': drive.get('id'),
                    'name': drive.get('name', 'Unknown')
                }
                for drive in drives
            ]
            
            # Define streaming function for SharePoint drives
            def stream_sharepoint_drive_files(drive_info, headers, delta_token_url, fallback_timestamp):
                drive_name = drive_info['name']
                # Stream files and prepend drive name to paths
                for file_info in self._stream_sharepoint_files_delta(
                    drive_info['id'], headers, drive_name, delta_token_url, fallback_timestamp
                ):
                    # Skip delta token markers (they'll be handled by the shared logic)
                    if isinstance(file_info, dict) and file_info.get('_delta_token'):
                        yield file_info
                    else:
                        # Add full S3 path including drive name
                        file_path = file_info.get('path', file_info.get('name', ''))
                        full_s3_path = f"{drive_name}/{file_path}"
                        yield {**file_info, 'path': full_s3_path}
            
            # Process all drives with shared logic
            results = self._process_items_with_delta(
                drives_to_process, source_config, destination_config, job_config,
                stream_sharepoint_drive_files
            )
            
        except Exception as e:
            error_msg = f"Error processing SharePoint source {source_config.name}: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
        
        return results
    def _stream_onedrive_files_delta(self, user_id: str, headers: Dict[str, str],
                                           user_prefix: str = "", delta_token: Optional[str] = None,
                                           fallback_timestamp: Optional[str] = None):
        """Stream files from OneDrive using Delta API (wrapper for shared implementation).
        
        Args:
            user_id: User ID
            headers: Authentication headers
            user_prefix: User prefix for path construction
            delta_token: Delta link from previous sync
            fallback_timestamp: ISO timestamp for fallback filtering
            
        Yields:
            File information dictionaries
        """
        # Create fallback function for OneDrive
        def fallback_func(modified_after):
            return self._stream_onedrive_files_recursive(
                user_id, headers, folder_id='root', user_prefix=user_prefix,
                modified_after=modified_after
            )
        
        # Call shared implementation
        return self._stream_delta_files(
            resource_id=user_id,
            resource_type='users',
            headers=headers,
            path_prefix=user_prefix,
            delta_token=delta_token,
            fallback_timestamp=fallback_timestamp,
            fallback_func=fallback_func
        )
    
    def _stream_sharepoint_files_delta(self, drive_id: str, headers: Dict[str, str],
                                             drive_name: str = "", delta_token: Optional[str] = None,
                                             fallback_timestamp: Optional[str] = None):
        """Stream files from SharePoint using Delta API (wrapper for shared implementation).
        
        Args:
            drive_id: Drive ID
            headers: Authentication headers
            drive_name: Drive name for path construction
            delta_token: Delta link from previous sync
            fallback_timestamp: ISO timestamp for fallback filtering
            
        Yields:
            File information dictionaries
        """
        # Create fallback function for SharePoint
        def fallback_func(modified_after):
            return self._stream_sharepoint_files_recursive(
                drive_id, headers, folder_id='root', path="",
                modified_after=modified_after
            )
        
        # Call shared implementation
        return self._stream_delta_files(
            resource_id=drive_id,
            resource_type='drives',
            headers=headers,
            path_prefix=drive_name,
            delta_token=delta_token,
            fallback_timestamp=fallback_timestamp,
            fallback_func=fallback_func
        )
    
    def _stream_delta_files(self, resource_id: str, resource_type: str, headers: Dict[str, str],
                            path_prefix: str = "", delta_token: Optional[str] = None,
                            fallback_timestamp: Optional[str] = None,
                            fallback_func=None):
        """Stream files using Delta API with timestamp fallback (shared by OneDrive and SharePoint).
        
        Hybrid approach:
        1. Try delta token first (fast - only changed files)
        2. If delta token expired (HTTP 410), fall back to recursive scan with timestamp filtering
        
        Args:
            resource_id: Resource ID (user_id or drive_id)
            resource_type: Type of resource ('users' or 'drives')
            headers: Authentication headers
            path_prefix: Prefix to add to file paths
            delta_token: Delta link from previous sync (None for initial sync)
            fallback_timestamp: ISO timestamp for fallback filtering if delta token expires
            fallback_func: Fallback function to call if delta expires
            
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
                logger.info(f"🔄 Using delta API for incremental sync ({resource_type}: {resource_id[:8]}...)")
            else:
                endpoint = f'https://graph.microsoft.com/v1.0/{resource_type}/{resource_id}/drive/root/delta'
                logger.info(f"📦 Using delta API for initial sync ({resource_type}: {resource_id[:8]}...)")
            
            files_found = 0
            
            while endpoint:
                # Refresh headers before each request to ensure fresh token
                token = self.microsoft_auth.get_access_token()
                headers = {
                    'Authorization': f'Bearer {token}',
                    'Content-Type': 'application/json'
                }
                response = requests.get(endpoint, headers=headers)
                
                # Handle 429 errors by implementing exponential backoff
                if response.status_code == 429:
                    logger.warning(f"⚠️ Rate limit exceeded for {resource_type} {resource_id[:8]}...")
                    retry_delay = 1  # Start with 1 second
                    max_retries = 5
                    for retry in range(max_retries):
                        logger.info(f"⏳ Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        response = requests.get(endpoint, headers=headers)
                        if response.status_code == 200:
                            break
                        retry_delay *= 2  # Exponential backoff
                    else:
                        logger.error(f"❌ Rate limit exceeded after {max_retries} retries")
                # Handle 401 errors by forcing token refresh and retrying
                if response.status_code == 401:    
                    logger.info(f"🔄 Token expired, refreshing and retrying delta request...")
                    token = self.microsoft_auth.get_access_token(force_refresh=True)
                    headers = {
                        'Authorization': f'Bearer {token}',
                        'Content-Type': 'application/json'
                    }
                    response = requests.get(endpoint, headers=headers)
                    
                # Handle delta token expiration
                if response.status_code == 410:
                    logger.warning(f"⚠️ Delta token expired for {resource_type} {resource_id[:8]}...")
                    
                    # Fall back to timestamp-based filtering if available
                    if fallback_timestamp and fallback_func:
                        try:
                            fallback_dt = date_parser.parse(fallback_timestamp)
                            logger.info(f"📅 Falling back to timestamp filter: files modified after {fallback_timestamp}")
                            
                            # Use provided fallback function with timestamp filtering
                            for file_info in fallback_func(modified_after=fallback_dt):
                                yield file_info
                            
                            # Start fresh delta sync for next time
                            logger.info(f"🔄 Initiating fresh delta sync to get new token...")
                            fresh_endpoint = f'https://graph.microsoft.com/v1.0/{resource_type}/{resource_id}/drive/root/delta'
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
                    endpoint = f'https://graph.microsoft.com/v1.0/{resource_type}/{resource_id}/drive/root/delta'
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
                        
                        if path_prefix:
                            if parent_path:
                                full_path = f"{path_prefix}/{parent_path}/{name}"
                            else:
                                full_path = f"{path_prefix}/{name}"
                        else:
                            if parent_path:
                                full_path = f"{parent_path}/{name}"
                            else:
                                full_path = name
                        
                        # Get download URL - Delta API should include it, but construct if missing
                        download_url = item.get('@microsoft.graph.downloadUrl', '')
                        
                        # If no download URL in delta response, construct the download endpoint
                        # This uses the /content endpoint which returns the file content directly
                        if not download_url and item_id:
                            # Get driveId from parentReference
                            parent_ref = item.get('parentReference', {})
                            drive_id = parent_ref.get('driveId', '')
                            
                            if drive_id:
                                # Construct download URL: /drives/{driveId}/items/{itemId}/content
                                download_url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content'
                            else:
                                # Fallback: construct based on resource type
                                download_url = f'https://graph.microsoft.com/v1.0/{resource_type}/{resource_id}/drive/items/{item_id}/content'
                            
                            logger.debug(f"Constructed download URL for {name}: {download_url}")
                        
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
                    # NOTE: nextLink URLs expire quickly (15-30 min) so they're not useful for
                    # long-term crash recovery. We'll only save the final deltaLink.
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
    
    def _stream_onedrive_files_recursive(self, user_id: str, headers: Dict[str, str],
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
                        for file_info in self._stream_onedrive_files_recursive(
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
                            for file_info in self._stream_onedrive_files_recursive(
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
    
    def _stream_sharepoint_files_recursive(self, drive_id: str, headers: Dict[str, str],
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
                        for file_info in self._stream_sharepoint_files_recursive(
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
                            for file_info in self._stream_sharepoint_files_recursive(
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
    
    def _stream_upload_file(self, file_info: Dict[str, Any], download_url: str, 
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
                return self._stream_to_aws_s3(
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
    
    def _stream_to_aws_s3(self, file_path: str, download_url: str, file_size: int, 
                               content_type: str, destination_config, 
                               file_info: Dict[str, Any] = None) -> Dict[str, Any]:
        """Stream file to AWS S3 with automatic credential refresh on expiration.
        
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
            from botocore.exceptions import ClientError

            s3_client = self.aws_auth.get_s3_client()
            
            prefix = getattr(destination_config, 'prefix', '')
            s3_key = f"{prefix}{file_path}".lstrip('/')
            
            # Check if this is a Microsoft Graph API URL that requires authentication
            # @microsoft.graph.downloadUrl URLs are pre-authenticated and don't need headers
            # But /content endpoint URLs require Bearer token
            needs_auth = 'graph.microsoft.com' in download_url and '/content' in download_url
            
            if needs_auth:
                # Get fresh token for download (handles token expiration)
                # Retry with exponential backoff for rate limiting (429) and auth errors (401)
                max_retries = 5
                retry_delay = 1  # Start with 1 second
                
                for attempt in range(max_retries):
                    token = self.microsoft_auth.get_access_token()
                    headers = {'Authorization': f'Bearer {token}'}
                    response = requests.get(download_url, headers=headers, stream=True)
                    
                    if response.status_code == 200:
                        break  # Success
                    elif response.status_code == 401:
                        logger.debug(f"Microsoft Graph token expired during download, refreshing...")
                        token = self.microsoft_auth.get_access_token(force_refresh=True)
                        headers = {'Authorization': f'Bearer {token}'}
                        response = requests.get(download_url, headers=headers, stream=True)
                        if response.status_code == 200:
                            break
                    elif response.status_code == 429:
                        # Rate limited - check Retry-After header
                        retry_after = response.headers.get('Retry-After', str(retry_delay))
                        try:
                            wait_time = int(retry_after)
                        except ValueError:
                            wait_time = retry_delay
                        
                        if attempt < max_retries - 1:
                            logger.warning(f"⚠️ Rate limited (429) downloading {file_path}, waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                            time.sleep(wait_time)
                            retry_delay = min(retry_delay * 2, 60)  # Exponential backoff, max 60s
                        else:
                            logger.error(f"❌ Rate limit exceeded after {max_retries} retries for {file_path}")
                    else:
                        break  # Other error, don't retry
            else:
                # Pre-authenticated download URL (no auth needed)
                # Still handle 429 rate limiting
                max_retries = 5
                retry_delay = 1
                
                for attempt in range(max_retries):
                    response = requests.get(download_url, stream=True)
                    
                    if response.status_code == 200:
                        break
                    elif response.status_code == 429:
                        retry_after = response.headers.get('Retry-After', str(retry_delay))
                        try:
                            wait_time = int(retry_after)
                        except ValueError:
                            wait_time = retry_delay
                        
                        if attempt < max_retries - 1:
                            logger.warning(f"⚠️ Rate limited (429) downloading {file_path}, waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                            time.sleep(wait_time)
                            retry_delay = min(retry_delay * 2, 60)
                        else:
                            logger.error(f"❌ Rate limit exceeded after {max_retries} retries for {file_path}")
                    else:
                        break
            
            if response.status_code == 200:
                encoded_path = base64.b64encode(file_path.encode('utf-8')).decode('ascii')
                modified_time = file_info.get('lastModifiedDateTime', '') if file_info else ''
                
                file_content = io.BytesIO(response.content)
                
                # Try upload with retry on credential expiration
                try:
                    s3_client.upload_fileobj(
                        Fileobj=file_content,
                        Bucket=destination_config.bucket,
                        Key=s3_key,
                        ExtraArgs={
                            'StorageClass': 'GLACIER_IR',
                            'ContentType': content_type,
                            'Metadata': {
                                'original-path-encoded': encoded_path,
                                'source': 'onedrive-backup',
                                'encoding': 'base64-utf8',
                                'source-modified-time': modified_time
                            }
                        }
                    )
                except ClientError as e:
                    error_code = e.response.get('Error', {}).get('Code', '')
                    if error_code in ['ExpiredToken', '401', 'InvalidAccessKeyId', 'SignatureDoesNotMatch']:
                        logger.info(f"AWS credentials expired during upload, refreshing and retrying...")
                        s3_client = self.aws_auth.refresh_credentials()
                        # Reset file content position for retry
                        file_content.seek(0)
                        s3_client.upload_fileobj(
                            Fileobj=file_content,
                            Bucket=destination_config.bucket,
                            Key=s3_key,
                            ExtraArgs={
                                'ContentType': content_type,
                                'StorageClass': 'GLACIER_IR',
                                'Metadata': {
                                    'original-path-encoded': encoded_path,
                                    'source': 'onedrive-backup',
                                    'encoding': 'base64-utf8',
                                    'source-modified-time': modified_time
                                }
                            }
                        )
                    else:
                        raise
                
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
    
    def run_all_jobs(self) -> List[Dict[str, Any]]:
        """Run all enabled backup jobs.
        
        Returns:
            List of job results
        """
        enabled_jobs = self.config.get_enabled_jobs()
        logger.info(f"Running {len(enabled_jobs)} backup jobs")
        
        results = []
        for job in enabled_jobs:
            job_result = self.run_backup_job(job)
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
