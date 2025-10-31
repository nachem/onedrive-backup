"""File utility functions."""

import os
import mimetypes
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

class FileHelper:
    """Helper class for file operations."""
    
    @staticmethod
    def get_file_info(file_path: Path) -> Dict[str, Any]:
        """Get comprehensive file information.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary with file information
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        stat = file_path.stat()
        
        return {
            'name': file_path.name,
            'path': str(file_path),
            'size': stat.st_size,
            'modified_time': datetime.fromtimestamp(stat.st_mtime),
            'created_time': datetime.fromtimestamp(stat.st_ctime),
            'is_file': file_path.is_file(),
            'is_dir': file_path.is_dir(),
            'extension': file_path.suffix.lower(),
            'mime_type': mimetypes.guess_type(str(file_path))[0],
            'parent': str(file_path.parent)
        }
    
    @staticmethod
    def format_file_size(size_bytes: int) -> str:
        """Format file size in human readable format.
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            Formatted size string
        """
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB", "TB", "PB"]
        i = 0
        
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        
        return f"{size_bytes:.1f} {size_names[i]}"
    
    @staticmethod
    def is_hidden_file(file_path: Path) -> bool:
        """Check if a file is hidden.
        
        Args:
            file_path: Path to check
            
        Returns:
            True if file is hidden
        """
        # On Windows, check file attributes
        if os.name == 'nt':
            try:
                attrs = os.stat(str(file_path)).st_file_attributes
                return attrs & 0x02  # FILE_ATTRIBUTE_HIDDEN
            except (AttributeError, OSError):
                pass
        
        # On Unix-like systems, files starting with . are hidden
        return file_path.name.startswith('.')
    
    @staticmethod
    def is_system_file(file_path: Path) -> bool:
        """Check if a file is a system file.
        
        Args:
            file_path: Path to check
            
        Returns:
            True if file is a system file
        """
        # System file patterns
        system_patterns = {
            '.tmp', '.temp', '.log', '.lock', '.pid',
            'thumbs.db', 'desktop.ini', '.ds_store',
            '~$'  # Office temp files
        }
        
        file_name_lower = file_path.name.lower()
        
        # Check exact matches
        if file_name_lower in system_patterns:
            return True
        
        # Check patterns
        if file_name_lower.startswith('~$'):
            return True
        
        # Check file attributes on Windows
        if os.name == 'nt':
            try:
                attrs = os.stat(str(file_path)).st_file_attributes
                return attrs & 0x04  # FILE_ATTRIBUTE_SYSTEM
            except (AttributeError, OSError):
                pass
        
        return False
    
    @staticmethod
    def should_exclude_file(file_path: Path, include_hidden: bool = False, 
                           include_system: bool = False) -> bool:
        """Check if a file should be excluded from backup.
        
        Args:
            file_path: Path to check
            include_hidden: Whether to include hidden files
            include_system: Whether to include system files
            
        Returns:
            True if file should be excluded
        """
        if not include_hidden and FileHelper.is_hidden_file(file_path):
            return True
        
        if not include_system and FileHelper.is_system_file(file_path):
            return True
        
        return False
    
    @staticmethod
    def sanitize_filename(filename: str, replacement: str = "_") -> str:
        """Sanitize filename for safe storage.
        
        Args:
            filename: Original filename
            replacement: Character to replace invalid characters
            
        Returns:
            Sanitized filename
        """
        # Characters not allowed in filenames on various systems
        invalid_chars = '<>:"/\\|?*'
        
        # Replace invalid characters
        sanitized = filename
        for char in invalid_chars:
            sanitized = sanitized.replace(char, replacement)
        
        # Remove control characters
        sanitized = ''.join(char for char in sanitized if ord(char) >= 32)
        
        # Trim whitespace and dots from ends
        sanitized = sanitized.strip(' .')
        
        # Ensure it's not empty
        if not sanitized:
            sanitized = "unnamed_file"
        
        # Limit length
        if len(sanitized) > 255:
            name, ext = os.path.splitext(sanitized)
            max_name_len = 255 - len(ext)
            sanitized = name[:max_name_len] + ext
        
        return sanitized
    
    @staticmethod
    def create_backup_path(source_path: str, prefix: str = "", 
                          preserve_structure: bool = True) -> str:
        """Create backup path from source path.
        
        Args:
            source_path: Original source path
            prefix: Prefix to add to backup path
            preserve_structure: Whether to preserve directory structure
            
        Returns:
            Backup path
        """
        # Normalize path separators
        normalized_path = source_path.replace('\\', '/')
        
        # Remove drive letter on Windows (C:/ -> /)
        if len(normalized_path) > 1 and normalized_path[1] == ':':
            normalized_path = normalized_path[2:]
        
        # Remove leading slash
        if normalized_path.startswith('/'):
            normalized_path = normalized_path[1:]
        
        if preserve_structure:
            backup_path = normalized_path
        else:
            # Flatten structure - use just filename
            backup_path = Path(normalized_path).name
        
        # Add prefix
        if prefix:
            backup_path = f"{prefix.rstrip('/')}/{backup_path}"
        
        return backup_path
    
    @staticmethod
    def get_relative_path(file_path: Path, base_path: Path) -> str:
        """Get relative path from base path.
        
        Args:
            file_path: Full file path
            base_path: Base path to calculate relative from
            
        Returns:
            Relative path as string
        """
        try:
            return str(file_path.relative_to(base_path))
        except ValueError:
            # If paths are not related, return the full path
            return str(file_path)
