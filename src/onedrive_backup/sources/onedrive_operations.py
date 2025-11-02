"""OneDrive operations for listing and managing files."""

from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from rich import print as rprint
from rich.console import Console
from rich.table import Table
from rich.tree import Tree

from ..auth.microsoft_auth import MicrosoftGraphAuth

console = Console()


class OneDriveFileManager:
    """Manage OneDrive file operations."""
    
    def __init__(self, auth: MicrosoftGraphAuth):
        """Initialize with Microsoft Graph authentication."""
        self.auth = auth
        self.headers = None
        self.token = None
        
    def _get_headers(self) -> Dict[str, str]:
        """Get authentication headers."""
        token = self.auth.get_access_token()
        if not self.headers or token != self.token:
            self.token = token
            self.headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
        return self.headers
    
    def get_users(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get list of users in the organization."""
        headers = self._get_headers()
        
        try:
            response = requests.get(
                f'https://graph.microsoft.com/v1.0/users?$top={limit}',
                headers=headers
            )
            
            if response.status_code == 200:
                users_data = response.json()
                users = []
                
                for user in users_data.get('value', []):
                    users.append({
                        'id': user.get('id'),
                        'name': user.get('displayName', 'N/A'),
                        'email': user.get('mail') or user.get('userPrincipalName', 'N/A'),
                        'enabled': user.get('accountEnabled', False)
                    })
                
                return users
            else:
                console.print(f"❌ Cannot list users: {response.status_code}", style="red")
                if response.status_code == 403:
                    console.print("   Need User.Read.All permission", style="yellow")
                return []
                
        except Exception as e:
            console.print(f"❌ Error getting users: {e}", style="red")
            return []
    
    def get_user_onedrive_info(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get OneDrive information for a specific user."""
        headers = self._get_headers()
        
        try:
            response = requests.get(
                f'https://graph.microsoft.com/v1.0/users/{user_id}/drive',
                headers=headers
            )
            
            if response.status_code == 200:
                drive_info = response.json()
                
                quota = drive_info.get('quota', {})
                return {
                    'id': drive_info.get('id'),
                    'name': drive_info.get('name', 'OneDrive'),
                    'type': drive_info.get('driveType', 'business'),
                    'web_url': drive_info.get('webUrl', ''),
                    'quota': {
                        'total': quota.get('total', 0),
                        'used': quota.get('used', 0),
                        'remaining': quota.get('remaining', 0)
                    }
                }
            else:
                return None
                
        except Exception as e:
            console.print(f"❌ Error getting OneDrive info: {e}", style="red")
            return None
    
    def list_files(self, user_id: str, folder_id: str = "root", 
                   recursive: bool = False, max_depth: int = 3) -> List[Dict[str, Any]]:
        """List files in a user's OneDrive folder."""
        headers = self._get_headers()
        
        def _list_folder_recursive(folder_id: str, path: str = "", depth: int = 0) -> List[Dict[str, Any]]:
            if depth > max_depth:
                return []
            
            all_items = []
            
            if folder_id == "root":
                endpoint = f'https://graph.microsoft.com/v1.0/users/{user_id}/drive/root/children'
            else:
                endpoint = f'https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{folder_id}/children'
            
            try:
                response = requests.get(endpoint, headers=headers)
                
                if response.status_code == 200:
                    items = response.json()
                    
                    for item in items.get('value', []):
                        name = item.get('name', 'N/A')
                        item_id = item.get('id', 'N/A')
                        size = item.get('size', 0)
                        modified = item.get('lastModifiedDateTime', 'N/A')
                        created = item.get('createdDateTime', 'N/A')
                        web_url = item.get('webUrl', '')
                        
                        # Format dates
                        if modified != 'N/A':
                            modified = modified[:19].replace('T', ' ')
                        if created != 'N/A':
                            created = created[:19].replace('T', ' ')
                        
                        full_path = f"{path}/{name}" if path else name
                        
                        file_info = {
                            'id': item_id,
                            'name': name,
                            'path': full_path,
                            'size': size,
                            'created': created,
                            'modified': modified,
                            'web_url': web_url,
                            'is_folder': item.get('folder') is not None,
                            'depth': depth
                        }
                        
                        if item.get('folder'):
                            # It's a folder
                            file_info['child_count'] = item.get('folder', {}).get('childCount', 0)
                            file_info['type'] = 'folder'
                        else:
                            # It's a file
                            file_info['type'] = 'file'
                            file_info['mime_type'] = item.get('file', {}).get('mimeType', 'unknown')
                            file_info['download_url'] = item.get('@microsoft.graph.downloadUrl', '')
                        
                        all_items.append(file_info)
                        
                        # Recursively process folders if requested
                        if recursive and item.get('folder') and depth < max_depth:
                            child_count = item.get('folder', {}).get('childCount', 0)
                            if child_count > 0:
                                sub_items = _list_folder_recursive(item_id, full_path, depth + 1)
                                all_items.extend(sub_items)
                
                return all_items
                
            except Exception as e:
                console.print(f"❌ Error listing folder: {e}", style="red")
                return []
        
        return _list_folder_recursive(folder_id, "", 0)
    
    def format_file_size(self, bytes_size: int) -> str:
        """Format file size in human readable format."""
        if bytes_size == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        while bytes_size >= 1024 and i < len(size_names) - 1:
            bytes_size /= 1024.0
            i += 1
        return f"{bytes_size:.1f} {size_names[i]}"
    
    def get_file_icon(self, file_info: Dict[str, Any]) -> str:
        """Get appropriate emoji icon for file type."""
        if file_info['is_folder']:
            return "📁"
        
        name = file_info['name']
        if not name or '.' not in name:
            return "📄"
        
        ext = name.split('.')[-1].lower()
        
        icons = {
            # Documents
            'doc': '📝', 'docx': '📝', 'txt': '📝', 'rtf': '📝',
            'pdf': '📑',
            
            # Spreadsheets
            'xls': '📊', 'xlsx': '📊', 'csv': '📊',
            
            # Presentations
            'ppt': '📽️', 'pptx': '📽️',
            
            # Images
            'jpg': '🖼️', 'jpeg': '🖼️', 'png': '🖼️', 'gif': '🖼️', 'bmp': '🖼️',
            'svg': '🖼️', 'tiff': '🖼️', 'webp': '🖼️',
            
            # Videos
            'mp4': '🎥', 'avi': '🎥', 'mkv': '🎥', 'mov': '🎥', 'wmv': '🎥',
            
            # Audio
            'mp3': '🎵', 'wav': '🎵', 'flac': '🎵', 'aac': '🎵',
            
            # Archives
            'zip': '📦', 'rar': '📦', '7z': '📦', 'tar': '📦', 'gz': '📦',
            
            # Code
            'py': '💻', 'js': '💻', 'html': '💻', 'css': '💻', 'json': '💻',
            'xml': '💻', 'yaml': '💻', 'yml': '💻',
            
            # Other
            'exe': '⚙️', 'msi': '⚙️'
        }
        
        return icons.get(ext, '📄')
    
    def display_users_table(self, users: List[Dict[str, Any]]):
        """Display users in a formatted table."""
        if not users:
            console.print("❌ No users found", style="red")
            return
        
        table = Table(title="Organization Users", show_header=True, header_style="bold blue")
        table.add_column("Name", style="cyan")
        table.add_column("Email", style="magenta")
        table.add_column("User ID", style="dim")
        table.add_column("Status", justify="center")
        
        for user in users:
            status = "✅ Active" if user['enabled'] else "❌ Disabled"
            status_style = "green" if user['enabled'] else "red"
            
            table.add_row(
                user['name'],
                user['email'],
                user['id'],
                f"[{status_style}]{status}[/{status_style}]"
            )
        
        console.print(table)
    
    def display_onedrive_info(self, user_info: Dict[str, Any], drive_info: Dict[str, Any]):
        """Display OneDrive information."""
        console.print(f"\n👤 [bold]User:[/bold] {user_info['name']}")
        console.print(f"📧 [bold]Email:[/bold] {user_info['email']}")
        console.print(f"🆔 [bold]User ID:[/bold] {user_info['id']}")
        
        if drive_info:
            console.print(f"\n💾 [bold]OneDrive:[/bold] {drive_info['name']}")
            console.print(f"🏷️  [bold]Type:[/bold] {drive_info['type']}")
            
            quota = drive_info['quota']
            if quota['total'] > 0:
                used_pct = (quota['used'] / quota['total']) * 100
                console.print(f"📊 [bold]Storage:[/bold] {self.format_file_size(quota['used'])} used of {self.format_file_size(quota['total'])} ({used_pct:.1f}%)")
                console.print(f"💿 [bold]Available:[/bold] {self.format_file_size(quota['remaining'])}")
        else:
            console.print("\n❌ [red]OneDrive not found or not accessible[/red]")
    
    def display_files_table(self, files: List[Dict[str, Any]], show_details: bool = True):
        """Display files in a formatted table."""
        if not files:
            console.print("📁 No files found", style="yellow")
            return
        
        # Separate files and folders
        folders = [f for f in files if f['is_folder']]
        file_items = [f for f in files if not f['is_folder']]
        
        console.print(f"\n📋 [bold]Found {len(files)} items ({len(folders)} folders, {len(file_items)} files)[/bold]")
        
        if show_details:
            table = Table(show_header=True, header_style="bold blue")
            table.add_column("Type", justify="center", width=4)
            table.add_column("Name", style="cyan")
            table.add_column("Size", justify="right")
            table.add_column("Modified", style="dim")
            
            # Sort items: folders first, then files, both alphabetically
            sorted_items = sorted(folders, key=lambda x: x['name'].lower()) + \
                          sorted(file_items, key=lambda x: x['name'].lower())
            
            for item in sorted_items:
                icon = self.get_file_icon(item)
                size_str = f"({item['child_count']} items)" if item['is_folder'] else self.format_file_size(item['size'])
                
                table.add_row(
                    icon,
                    item['name'],
                    size_str,
                    item['modified']
                )
            
            console.print(table)
        
        # Statistics
        if file_items:
            total_size = sum(f['size'] for f in file_items)
            console.print(f"\n📊 [bold]Statistics:[/bold]")
            console.print(f"   📄 Files: {len(file_items)}")
            console.print(f"   📁 Folders: {len(folders)}")
            console.print(f"   📏 Total size: {self.format_file_size(total_size)}")
            
            # File type breakdown
            type_stats = {}
            for file_item in file_items:
                ext = file_item['name'].split('.')[-1].lower() if '.' in file_item['name'] else 'no_ext'
                if ext not in type_stats:
                    type_stats[ext] = {'count': 0, 'size': 0}
                type_stats[ext]['count'] += 1
                type_stats[ext]['size'] += file_item['size']
            
            if type_stats:
                console.print(f"\n📈 [bold]File Types:[/bold]")
                for ext, stats in sorted(type_stats.items(), key=lambda x: x[1]['count'], reverse=True)[:10]:
                    console.print(f"   .{ext}: {stats['count']} files ({self.format_file_size(stats['size'])})")
    
    def display_files_tree(self, files: List[Dict[str, Any]]):
        """Display files in a tree structure."""
        if not files:
            console.print("📁 No files found", style="yellow")
            return
        
        tree = Tree("📂 OneDrive")
        
        # Group items by depth and parent
        depth_groups = {}
        for item in files:
            depth = item['depth']
            if depth not in depth_groups:
                depth_groups[depth] = []
            depth_groups[depth].append(item)
        
        # Build tree structure (simplified - showing only root level and one level deep)
        root_items = depth_groups.get(0, [])
        
        for item in sorted(root_items, key=lambda x: (not x['is_folder'], x['name'].lower())):
            icon = self.get_file_icon(item)
            
            if item['is_folder']:
                size_info = f" ({item['child_count']} items)"
                folder_node = tree.add(f"{icon} {item['name']}{size_info}")
                
                # Add some children if they exist
                children = [f for f in files if f['depth'] == 1 and f['path'].startswith(f"{item['name']}/")]
                for child in sorted(children[:5], key=lambda x: (not x['is_folder'], x['name'].lower())):
                    child_icon = self.get_file_icon(child)
                    if child['is_folder']:
                        child_size = f" ({child['child_count']} items)"
                    else:
                        child_size = f" ({self.format_file_size(child['size'])})"
                    folder_node.add(f"{child_icon} {child['name']}{child_size}")
                
                if len(children) > 5:
                    folder_node.add(f"... and {len(children) - 5} more items")
            else:
                size_info = f" ({self.format_file_size(item['size'])})"
                tree.add(f"{icon} {item['name']}{size_info}")
        
        console.print(tree)


# Alias for backward compatibility and different use cases
class OneDriveOperations(OneDriveFileManager):
    """OneDrive operations class - alias for OneDriveFileManager with additional async methods."""
    
    async def list_files(self, folder_path: str = "", recursive: bool = True) -> List[Dict[str, Any]]:
        """List files from personal OneDrive asynchronously.
        
        Args:
            folder_path: Path to folder (empty for root)
            recursive: Whether to list recursively
            
        Returns:
            List of file information dictionaries
        """
        # Get user's OneDrive using app-only authentication
        headers = self._get_headers()
        
        try:
            # For app-only auth, we need to get a specific user
            # Try /me first (works for delegated auth), fall back to listing users (app-only)
            user_response = requests.get(
                'https://graph.microsoft.com/v1.0/me',
                headers=headers
            )
            
            if user_response.status_code == 200:
                user_id = user_response.json().get('id')
            else:
                # /me doesn't work (app-only auth), get first user
                users_response = requests.get(
                    'https://graph.microsoft.com/v1.0/users?$top=1',
                    headers=headers
                )
                
                if users_response.status_code == 200:
                    users = users_response.json().get('value', [])
                    if users:
                        user_id = users[0].get('id')
                        console.print(f"Using OneDrive for user: {users[0].get('displayName', 'Unknown')}")
                    else:
                        console.print("❌ No users found in organization")
                        return []
                else:
                    console.print(f"❌ Could not list users: {users_response.status_code}")
                    return []
            
            if user_id:
                
                # Now list files using the parent class method
                files = super().list_files(
                    user_id=user_id,
                    folder_id="root",
                    recursive=recursive,
                    max_depth=10 if recursive else 1
                )
                
                # Convert to format expected by backup manager
                result_files = []
                for file in files:
                    if not file.get('is_folder', False):  # Only include actual files
                        result_files.append({
                            'id': file.get('id'),
                            'name': file.get('name'),
                            'path': file.get('path', file.get('name')),
                            'size': file.get('size', 0),
                            'lastModifiedDateTime': file.get('modified', ''),
                            'mimeType': file.get('mime_type', 'application/octet-stream'),
                            '@microsoft.graph.downloadUrl': file.get('download_url', '')
                        })
                
                return result_files
            else:
                console.print(f"❌ Could not get current user: {user_response.status_code}")
                return []
                
        except Exception as e:
            console.print(f"❌ Error listing OneDrive files: {e}")
            return []
    
    async def get_download_url(self, file_id: str) -> Optional[str]:
        """Get download URL for a file.
        
        Args:
            file_id: File ID
            
        Returns:
            Download URL or None
        """
        headers = self._get_headers()
        
        try:
            response = requests.get(
                f'https://graph.microsoft.com/v1.0/me/drive/items/{file_id}',
                headers=headers
            )
            
            if response.status_code == 200:
                item = response.json()
                return item.get('@microsoft.graph.downloadUrl')
            else:
                return None
                
        except Exception as e:
            console.print(f"❌ Error getting download URL: {e}")
            return None
