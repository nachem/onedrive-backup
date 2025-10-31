#!/usr/bin/env python3
"""
List OneDrive Files

This test lists all accessible OneDrive files in detail using the working method
(SharePoint document libraries that contain OneDrive content).
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

def list_folder_recursively(headers, drive_id, folder_id="root", folder_path="", level=0, max_level=3):
    """Recursively list files in a folder."""
    if level > max_level:
        return
    
    indent = "  " * level
    
    # Get items in current folder
    if folder_id == "root":
        endpoint = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children'
    else:
        endpoint = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}/children'
    
    response = requests.get(endpoint, headers=headers)
    
    if response.status_code == 200:
        items = response.json()
        
        for item in items.get('value', []):
            name = item.get('name', 'N/A')
            size = item.get('size', 0)
            modified = item.get('lastModifiedDateTime', 'N/A')
            created = item.get('createdDateTime', 'N/A')
            item_id = item.get('id', 'N/A')
            
            if modified != 'N/A':
                modified = modified[:19].replace('T', ' ')
            if created != 'N/A':
                created = created[:19].replace('T', ' ')
            
            if item.get('folder'):
                # It's a folder
                print(f"{indent}📁 {name}/")
                print(f"{indent}   ID: {item_id}")
                print(f"{indent}   Created: {created}")
                print(f"{indent}   Modified: {modified}")
                print(f"{indent}   Items: {item.get('folder', {}).get('childCount', 'N/A')}")
                
                # Recursively list folder contents
                if level < max_level:
                    print(f"{indent}   Contents:")
                    list_folder_recursively(headers, drive_id, item_id, f"{folder_path}/{name}", level + 1, max_level)
                else:
                    print(f"{indent}   (Max depth reached - not listing contents)")
                print()
            else:
                # It's a file
                file_type = "📄"
                file_ext = name.split('.')[-1].lower() if '.' in name else ""
                
                # Use different emojis for different file types
                if file_ext in ['jpg', 'jpeg', 'png', 'gif', 'bmp']:
                    file_type = "🖼️"
                elif file_ext in ['doc', 'docx']:
                    file_type = "📝"
                elif file_ext in ['xls', 'xlsx']:
                    file_type = "📊"
                elif file_ext in ['ppt', 'pptx']:
                    file_type = "📽️"
                elif file_ext in ['pdf']:
                    file_type = "📑"
                elif file_ext in ['mp4', 'avi', 'mkv', 'mov']:
                    file_type = "🎥"
                elif file_ext in ['mp3', 'wav', 'flac']:
                    file_type = "🎵"
                elif file_ext in ['zip', 'rar', '7z']:
                    file_type = "📦"
                
                print(f"{indent}{file_type} {name}")
                print(f"{indent}   ID: {item_id}")
                print(f"{indent}   Size: {format_file_size(size)}")
                print(f"{indent}   Created: {created}")
                print(f"{indent}   Modified: {modified}")
                
                # Show additional file properties
                if item.get('file'):
                    mime_type = item.get('file', {}).get('mimeType', 'N/A')
                    print(f"{indent}   Type: {mime_type}")
                
                # Show download URL if available
                download_url = item.get('@microsoft.graph.downloadUrl')
                if download_url:
                    print(f"{indent}   Download: Available")
                
                print()
    else:
        print(f"{indent}❌ Cannot access folder contents: {response.status_code}")

async def list_onedrive_files():
    """List all accessible OneDrive files."""
    print("🚀 OneDrive Files Listing")
    print("=" * 50)
    
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
        
        # Get all accessible drives (this worked in previous test)
        print("\n🔍 Getting all accessible drives...")
        response = requests.get('https://graph.microsoft.com/v1.0/drives', headers=headers)
        
        if response.status_code == 200:
            drives = response.json()
            drive_count = len(drives.get('value', []))
            print(f'✅ Found {drive_count} accessible drives')
            
            total_files = 0
            total_folders = 0
            total_size = 0
            
            for i, drive in enumerate(drives.get('value', [])):
                drive_name = drive.get('name', 'N/A')
                drive_type = drive.get('driveType', 'N/A')
                drive_id = drive.get('id', 'N/A')
                owner = drive.get('owner', {}).get('user', {}).get('displayName', 'N/A')
                
                print(f"\n{'='*60}")
                print(f"📂 DRIVE {i+1}: {drive_name}")
                print(f"{'='*60}")
                print(f"Type: {drive_type}")
                print(f"ID: {drive_id}")
                print(f"Owner: {owner}")
                
                # Get drive quota information
                quota = drive.get('quota', {})
                if quota:
                    total = quota.get('total', 0)
                    used = quota.get('used', 0)
                    remaining = quota.get('remaining', 0)
                    
                    if total > 0:
                        print(f"Storage: {format_file_size(used)} used of {format_file_size(total)} ({format_file_size(remaining)} remaining)")
                        usage_percent = (used / total) * 100 if total > 0 else 0
                        print(f"Usage: {usage_percent:.1f}%")
                
                print(f"\n📋 CONTENTS:")
                print("-" * 40)
                
                # List all files and folders in this drive
                list_folder_recursively(headers, drive_id, "root", "", 0, max_level=2)
                
                # Get statistics for this drive
                stats_endpoint = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children?$count=true'
                stats_response = requests.get(stats_endpoint, headers=headers)
                
                if stats_response.status_code == 200:
                    stats = stats_response.json()
                    item_count = len(stats.get('value', []))
                    
                    # Count files vs folders
                    files_in_drive = 0
                    folders_in_drive = 0
                    size_in_drive = 0
                    
                    for item in stats.get('value', []):
                        if item.get('folder'):
                            folders_in_drive += 1
                        else:
                            files_in_drive += 1
                            size_in_drive += item.get('size', 0)
                    
                    print(f"📊 DRIVE SUMMARY:")
                    print(f"   Files: {files_in_drive}")
                    print(f"   Folders: {folders_in_drive}")
                    print(f"   Total size: {format_file_size(size_in_drive)}")
                    
                    total_files += files_in_drive
                    total_folders += folders_in_drive
                    total_size += size_in_drive
            
            # Overall summary
            print(f"\n{'='*60}")
            print(f"📊 OVERALL SUMMARY")
            print(f"{'='*60}")
            print(f"Total drives: {drive_count}")
            print(f"Total files: {total_files}")
            print(f"Total folders: {total_folders}")
            print(f"Total size: {format_file_size(total_size)}")
            
        else:
            print(f'❌ Cannot access drives: {response.status_code}')
            print(f'Error: {response.text}')
        
        print("\n" + "=" * 50)
        print("✅ OneDrive files listing completed!")
        return True
        
    except Exception as e:
        print(f'\n❌ Test failed with error: {e}')
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function."""
    try:
        # Run the async test
        result = asyncio.run(list_onedrive_files())
        
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
