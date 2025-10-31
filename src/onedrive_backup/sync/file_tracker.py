"""File tracking for change detection and backup state management."""

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Set


@dataclass
class FileInfo:
    """Information about a file for tracking changes."""
    path: str
    size: int
    modified_time: str  # ISO format timestamp
    hash_md5: Optional[str] = None
    last_backup: Optional[str] = None  # ISO format timestamp
    backup_destination: Optional[str] = None

class FileTracker:
    """Track file states for change detection."""
    
    def __init__(self, tracker_file: Path):
        """Initialize file tracker.
        
        Args:
            tracker_file: Path to the tracking database file
        """
        self.tracker_file = tracker_file
        self.tracker_file.parent.mkdir(parents=True, exist_ok=True)
        self._file_states: Dict[str, FileInfo] = {}
        self._load_state()
    
    def _load_state(self):
        """Load file states from disk."""
        if self.tracker_file.exists():
            try:
                with open(self.tracker_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self._file_states = {
                        path: FileInfo(**info) for path, info in data.items()
                    }
            except Exception:
                # If we can't load the state, start fresh
                self._file_states = {}
    
    def _save_state(self):
        """Save file states to disk."""
        try:
            data = {path: asdict(info) for path, info in self._file_states.items()}
            with open(self.tracker_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            # Log error but don't fail the backup
            print(f"Warning: Could not save file tracker state: {e}")
    
    def get_file_info(self, file_path: str) -> Optional[FileInfo]:
        """Get stored information about a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            FileInfo if file is tracked, None otherwise
        """
        return self._file_states.get(file_path)
    
    def needs_backup(self, file_path: str, file_size: int, modified_time: str,
                    detection_method: str = 'timestamp', 
                    s3_client=None, bucket: Optional[str] = None, prefix: str = "") -> bool:
        """Check if a file needs backup based on change detection method.
        
        Checks against S3 (source of truth) if s3_client provided, otherwise falls back to local tracker.
        
        Args:
            file_path: Path to the file
            file_size: Current file size
            modified_time: Current modification time as string
            detection_method: Detection method ('timestamp', 'size', 'hash', 'combined')
            s3_client: Optional boto3 S3 client to check actual S3 files
            bucket: S3 bucket name (optional)
            prefix: S3 key prefix
            
        Returns:
            True if file needs backup, False otherwise
        """
        # If S3 client and bucket provided, check against actual S3 files (source of truth)
        if s3_client and bucket:
            return self._needs_backup_from_s3(file_path, file_size, modified_time, 
                                             detection_method, s3_client, bucket, prefix)
        
        # Fallback to local tracker
        stored_info = self._file_states.get(file_path)
        
        # New file always needs backup
        if stored_info is None:
            return True
        
        # Check based on detection method
        if detection_method == 'size':
            return stored_info.size != file_size
        elif detection_method == 'timestamp':
            return stored_info.modified_time != modified_time
        elif detection_method in ['hash', 'combined']:
            # For now, fall back to size and timestamp
            return (stored_info.size != file_size or 
                   stored_info.modified_time != modified_time)
        else:
            # Default to timestamp
            return stored_info.modified_time != modified_time
    
    def _needs_backup_from_s3(self, file_path: str, file_size: int, modified_time: str,
                             detection_method: str, s3_client, bucket: str, prefix: str = "") -> bool:
        """Check if file needs backup by comparing against actual S3 file (source of truth).
        
        Args:
            file_path: Path to the file
            file_size: Current file size
            modified_time: Current modification time as string
            detection_method: Detection method
            s3_client: boto3 S3 client
            bucket: S3 bucket name
            prefix: S3 key prefix
            
        Returns:
            True if file needs backup, False otherwise
        """
        import base64
        
        try:
            # Construct S3 key
            s3_key = f"{prefix}{file_path}".lstrip('/')
            
            # Try to get object metadata from S3
            response = s3_client.head_object(Bucket=bucket, Key=s3_key)
            
            # File exists in S3, check if it needs update
            s3_size = response.get('ContentLength', 0)
            s3_metadata = response.get('Metadata', {})
            
            # Get modification time from metadata (we store it there during upload)
            s3_modified = s3_metadata.get('source-modified-time', '')
            
            # Compare based on detection method
            if detection_method == 'size':
                return s3_size != file_size
            elif detection_method == 'timestamp':
                # Compare modification times
                return s3_modified != modified_time
            elif detection_method in ['hash', 'combined']:
                # Check both size and timestamp
                return s3_size != file_size or s3_modified != modified_time
            else:
                # Default to timestamp
                return s3_modified != modified_time
                
        except s3_client.exceptions.NoSuchKey:
            # File doesn't exist in S3, needs backup
            return True
        except s3_client.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == '404':
                # File doesn't exist in S3
                return True
            else:
                # Other S3 error, assume needs backup to be safe
                print(f"Warning: S3 error checking {file_path}: {e}")
                return True
        except Exception as e:
            # Any other error, assume needs backup to be safe
            print(f"Warning: Error checking S3 for {file_path}: {e}")
            return True
    
    def has_file_changed(self, file_path: str, size: int, modified_time: datetime, 
                        hash_md5: Optional[str] = None) -> bool:
        """Check if a file has changed since last backup.
        
        Args:
            file_path: Path to the file
            size: Current file size
            modified_time: Current modification time
            hash_md5: Optional MD5 hash of file content
            
        Returns:
            True if file has changed or is new, False otherwise
        """
        stored_info = self._file_states.get(file_path)
        
        # New file
        if stored_info is None:
            return True
        
        # Check size
        if stored_info.size != size:
            return True
        
        # Check modification time
        modified_time_str = modified_time.isoformat()
        if stored_info.modified_time != modified_time_str:
            return True
        
        # Check hash if provided
        if hash_md5 and stored_info.hash_md5:
            if stored_info.hash_md5 != hash_md5:
                return True
        
        return False
    
    def update_file_info(self, file_path: str, size: int, modified_time,
                        hash_md5: Optional[str] = None, destination: Optional[str] = None):
        """Update stored information about a file.
        
        Args:
            file_path: Path to the file
            size: File size
            modified_time: Modification time (datetime object or ISO string)
            hash_md5: Optional MD5 hash
            destination: Backup destination name
        """
        # Handle both datetime objects and strings
        if isinstance(modified_time, datetime):
            modified_time_str = modified_time.isoformat()
        else:
            modified_time_str = modified_time
            
        backup_time_str = datetime.now().isoformat()
        
        self._file_states[file_path] = FileInfo(
            path=file_path,
            size=size,
            modified_time=modified_time_str,
            hash_md5=hash_md5,
            last_backup=backup_time_str,
            backup_destination=destination
        )
    
    def remove_file_info(self, file_path: str):
        """Remove tracking information for a file.
        
        Args:
            file_path: Path to the file
        """
        self._file_states.pop(file_path, None)
    
    def get_tracked_files(self) -> Set[str]:
        """Get set of all tracked file paths.
        
        Returns:
            Set of file paths
        """
        return set(self._file_states.keys())
    
    def cleanup_missing_files(self, existing_files: Set[str]):
        """Remove tracking info for files that no longer exist.
        
        Args:
            existing_files: Set of file paths that currently exist
        """
        tracked_files = set(self._file_states.keys())
        missing_files = tracked_files - existing_files
        
        for file_path in missing_files:
            self.remove_file_info(file_path)
    
    def save(self):
        """Save current state to disk."""
        self._save_state()
    
    def get_stats(self) -> Dict[str, int]:
        """Get statistics about tracked files.
        
        Returns:
            Dictionary with statistics
        """
        total_files = len(self._file_states)
        backed_up_files = len([f for f in self._file_states.values() if f.last_backup])
        total_size = sum(f.size for f in self._file_states.values())
        
        return {
            'total_files': total_files,
            'backed_up_files': backed_up_files,
            'total_size': total_size
        }

def calculate_file_hash(file_path: Path, chunk_size: int = 8192) -> str:
    """Calculate MD5 hash of a file.
    
    Args:
        file_path: Path to the file
        chunk_size: Size of chunks to read
        
    Returns:
        MD5 hash as hex string
    """
    hash_md5 = hashlib.md5()
    
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            hash_md5.update(chunk)
    
    return hash_md5.hexdigest()
