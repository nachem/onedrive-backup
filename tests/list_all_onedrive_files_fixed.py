#!/usr/bin/env python3
"""
Fixed OneDrive Files Listing

This script properly lists ALL OneDrive files with complete details,
organized output, and proper file counting.
"""

import sys
import asyncio
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from onedrive_backup.auth.microsoft_auth import MicrosoftGraphAuth
from onedrive_backup.config.settings import CredentialsConfig
import requests
import json

def format_file_size(size_bytes):
    """Format file size in human readable format."""
    if size_bytes == 0:
        return "0 B"
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    return f"{size_bytes:.1f} {size_names[i]}"

def get_file_icon(name):
    """Get appropriate emoji for file type."""
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
        'flv': '🎥', 'webm': '🎥',
        
        # Audio
        'mp3': '🎵', 'wav': '🎵', 'flac': '🎵', 'aac': '🎵', 'wma': '🎵',
        
        # Archives
        'zip': '📦', 'rar': '📦', '7z': '📦', 'tar': '📦', 'gz': '📦',
        
        # Code
        'py': '💻', 'js': '💻', 'html': '💻', 'css': '💻', 'json': '💻',
        'xml': '💻', 'yaml': '💻', 'yml': '💻',
        
        # Other
        'exe': '⚙️', 'msi': '⚙️', 'app': '⚙️',
    }
    
    return icons.get(ext, '📄')

class OneDriveFileCollector:
    def __init__(self, headers):
        self.headers = headers
        self.all_files = []
        self.all_folders = []
        self.total_size = 0
        
    def collect_items_recursively(self, drive_id, folder_id="root", folder_path="", level=0, max_level=10):
        """Recursively collect all files and folders."""
        if level > max_level:
            return
        
        # Get items in current folder
        if folder_id == "root":
            endpoint = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children'
        else:
            endpoint = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}/children'
        
        try:
            response = requests.get(endpoint, headers=self.headers)
            
            if response.status_code == 200:
                items = response.json()
                
                for item in items.get('value', []):
                    name = item.get('name', 'N/A')
                    size = item.get('size', 0)
                    modified = item.get('lastModifiedDateTime', 'N/A')
                    created = item.get('createdDateTime', 'N/A')
                    item_id = item.get('id', 'N/A')
                    
                    # Format dates
                    if modified != 'N/A':
                        modified = modified[:19].replace('T', ' ')
                    if created != 'N/A':
                        created = created[:19].replace('T', ' ')
                    
                    full_path = f"{folder_path}/{name}" if folder_path else name
                    
                    if item.get('folder'):
                        # It's a folder
                        folder_info = {
                            'name': name,
                            'path': full_path,
                            'id': item_id,
                            'created': created,
                            'modified': modified,
                            'child_count': item.get('folder', {}).get('childCount', 0),
                            'level': level
                        }
                        self.all_folders.append(folder_info)
                        
                        # Recursively process folder contents
                        self.collect_items_recursively(drive_id, item_id, full_path, level + 1, max_level)
                    else:
                        # It's a file
                        file_info = {
                            'name': name,
                            'path': full_path,
                            'id': item_id,
                            'size': size,
                            'created': created,
                            'modified': modified,
                            'mime_type': item.get('file', {}).get('mimeType', 'N/A'),
                            'download_url': item.get('@microsoft.graph.downloadUrl', 'N/A'),
                            'level': level,
                            'icon': get_file_icon(name)
                        }
                        self.all_files.append(file_info)
                        self.total_size += size
            
            else:
                print(f"❌ Cannot access folder at level {level}: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Error collecting items at level {level}: {e}")

async def list_all_onedrive_files_fixed():
    """List ALL OneDrive files with proper organization."""
    print("🚀 Complete OneDrive Files Listing (FIXED)")
    print("=" * 70)
    
    try:
        # Load credentials
        config_path = Path(__file__).parent.parent / "config" / "credentials.yaml"
        creds = CredentialsConfig.from_yaml(config_path)
        
        print(f'✅ Credentials loaded successfully')
        
        # Get authentication
        auth = MicrosoftGraphAuth(
            app_id=creds.microsoft_app_id,
            app_secret=creds.microsoft_app_secret,
            tenant_id=creds.microsoft_tenant_id
        )
        
        token = auth.get_access_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        print(f'✅ Access token obtained')
        
        # Get all accessible drives
        print("\n🔍 Discovering OneDrive drives...")
        response = requests.get('https://graph.microsoft.com/v1.0/drives', headers=headers)
        
        if response.status_code == 200:
            drives = response.json()
            drive_count = len(drives.get('value', []))
            print(f'✅ Found {drive_count} accessible drives')
            
            grand_total_files = 0
            grand_total_folders = 0
            grand_total_size = 0
            
            for i, drive in enumerate(drives.get('value', [])):
                drive_name = drive.get('name', 'N/A')
                drive_type = drive.get('driveType', 'N/A')
                drive_id = drive.get('id', 'N/A')
                owner = drive.get('owner', {}).get('user', {}).get('displayName', 'System')
                
                print(f"\n{'='*70}")
                print(f"📂 DRIVE {i+1}: {drive_name}")
                print(f"{'='*70}")
                print(f"🏷️  Type: {drive_type}")
                print(f"👤 Owner: {owner}")
                print(f"🆔 ID: {drive_id}")
                
                # Get drive quota information
                quota = drive.get('quota', {})
                if quota:
                    total = quota.get('total', 0)
                    used = quota.get('used', 0)
                    remaining = quota.get('remaining', 0)
                    
                    if total > 0:
                        print(f"💾 Storage: {format_file_size(used)} used of {format_file_size(total)}")
                        print(f"📊 Usage: {(used / total) * 100:.1f}%")
                        print(f"💿 Available: {format_file_size(remaining)}")
                
                print(f"\n🔍 Collecting ALL files and folders...")
                
                # Collect all items recursively
                collector = OneDriveFileCollector(headers)
                collector.collect_items_recursively(drive_id, "root", "", 0, 10)
                
                print(f"✅ Collection completed!")
                print(f"📊 Found: {len(collector.all_files)} files, {len(collector.all_folders)} folders")
                print(f"📏 Total size: {format_file_size(collector.total_size)}")
                
                # Display folder structure
                if collector.all_folders:
                    print(f"\n📁 FOLDER STRUCTURE:")
                    print("-" * 50)
                    for folder in sorted(collector.all_folders, key=lambda x: x['path']):
                        indent = "  " * folder['level']
                        print(f"{indent}📁 {folder['path']}/ ({folder['child_count']} items)")
                
                # Display all files
                if collector.all_files:
                    print(f"\n📄 ALL FILES ({len(collector.all_files)} total):")
                    print("-" * 50)
                    
                    # Group files by type
                    files_by_type = {}
                    for file_info in collector.all_files:
                        ext = file_info['name'].split('.')[-1].lower() if '.' in file_info['name'] else 'no_ext'
                        if ext not in files_by_type:
                            files_by_type[ext] = []
                        files_by_type[ext].append(file_info)
                    
                    # Display by type
                    for ext, files in sorted(files_by_type.items()):
                        print(f"\n📋 {ext.upper()} files ({len(files)}):")
                        for file_info in sorted(files, key=lambda x: x['path']):
                            icon = file_info['icon']
                            name = file_info['name']
                            path = file_info['path']
                            size = format_file_size(file_info['size'])
                            modified = file_info['modified']
                            
                            print(f"   {icon} {path}")
                            print(f"      Size: {size} | Modified: {modified}")
                
                # Drive statistics
                print(f"\n📊 DRIVE STATISTICS:")
                print(f"   📄 Files: {len(collector.all_files)}")
                print(f"   📁 Folders: {len(collector.all_folders)}")
                print(f"   📏 Total size: {format_file_size(collector.total_size)}")
                
                # File type breakdown
                if collector.all_files:
                    type_stats = {}
                    for file_info in collector.all_files:
                        ext = file_info['name'].split('.')[-1].lower() if '.' in file_info['name'] else 'no_ext'
                        if ext not in type_stats:
                            type_stats[ext] = {'count': 0, 'size': 0}
                        type_stats[ext]['count'] += 1
                        type_stats[ext]['size'] += file_info['size']
                    
                    print(f"\n📈 FILE TYPE BREAKDOWN:")
                    for ext, stats in sorted(type_stats.items(), key=lambda x: x[1]['count'], reverse=True):
                        print(f"   .{ext}: {stats['count']} files ({format_file_size(stats['size'])})")
                
                grand_total_files += len(collector.all_files)
                grand_total_folders += len(collector.all_folders)
                grand_total_size += collector.total_size
            
            # Grand total summary
            print(f"\n{'='*70}")
            print(f"🎯 GRAND TOTAL SUMMARY")
            print(f"{'='*70}")
            print(f"📂 Total drives: {drive_count}")
            print(f"📄 Total files: {grand_total_files}")
            print(f"📁 Total folders: {grand_total_folders}")
            print(f"📏 Total size: {format_file_size(grand_total_size)}")
            
            if grand_total_files > 0:
                print(f"📊 Average file size: {format_file_size(grand_total_size // grand_total_files)}")
            
        else:
            print(f'❌ Cannot access drives: {response.status_code}')
            print(f'Error: {response.text}')
            return False
        
        print(f"\n✅ Complete OneDrive listing finished!")
        return True
        
    except Exception as e:
        print(f'\n❌ Listing failed with error: {e}')
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function."""
    try:
        result = asyncio.run(list_all_onedrive_files_fixed())
        
        if result:
            print("\n🎉 OneDrive files listed successfully!")
            return 0
        else:
            print("\n💥 Failed to list files. Check the output above for details.")
            return 1
            
    except KeyboardInterrupt:
        print("\n⚠️ Listing interrupted by user")
        return 1
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
