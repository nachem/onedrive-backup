#!/usr/bin/env python3
"""
Personal OneDrive Access with User ID

This test demonstrates accessing personal OneDrive using a specific user ID/email,
similar to the PowerShell approach that worked for you.
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
        'doc': '📝', 'docx': '📝', 'txt': '📝', 'rtf': '📝',
        'pdf': '📑',
        'xls': '📊', 'xlsx': '📊', 'csv': '📊',
        'ppt': '📽️', 'pptx': '📽️',
        'jpg': '🖼️', 'jpeg': '🖼️', 'png': '🖼️', 'gif': '🖼️', 'bmp': '🖼️',
        'mp4': '🎥', 'avi': '🎥', 'mkv': '🎥', 'mov': '🎥',
        'mp3': '🎵', 'wav': '🎵', 'flac': '🎵',
        'zip': '📦', 'rar': '📦', '7z': '📦',
        'py': '💻', 'js': '💻', 'html': '💻', 'css': '💻',
    }
    
    return icons.get(ext, '📄')

def list_folder_contents(headers, user_id, folder_id="root", level=0, max_level=2):
    """List contents of a folder in user's OneDrive."""
    if level > max_level:
        return []
    
    indent = "  " * level
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
                size = item.get('size', 0)
                modified = item.get('lastModifiedDateTime', 'N/A')
                created = item.get('createdDateTime', 'N/A')
                item_id = item.get('id', 'N/A')
                web_url = item.get('webUrl', 'N/A')
                
                # Format dates
                if modified != 'N/A':
                    modified = modified[:19].replace('T', ' ')
                if created != 'N/A':
                    created = created[:19].replace('T', ' ')
                
                item_info = {
                    'name': name,
                    'id': item_id,
                    'size': size,
                    'created': created,
                    'modified': modified,
                    'web_url': web_url,
                    'level': level,
                    'is_folder': item.get('folder') is not None
                }
                
                if item.get('folder'):
                    # It's a folder
                    child_count = item.get('folder', {}).get('childCount', 0)
                    item_info['child_count'] = child_count
                    
                    print(f"{indent}📁 {name}/ ({child_count} items)")
                    print(f"{indent}   Created: {created}")
                    print(f"{indent}   Modified: {modified}")
                    print(f"{indent}   Web URL: {web_url}")
                    
                    all_items.append(item_info)
                    
                    # Recursively list folder contents if not too deep
                    if level < max_level and child_count > 0:
                        print(f"{indent}   Contents:")
                        sub_items = list_folder_contents(headers, user_id, item_id, level + 1, max_level)
                        all_items.extend(sub_items)
                    
                    print()
                else:
                    # It's a file
                    file_icon = get_file_icon(name)
                    item_info['icon'] = file_icon
                    item_info['mime_type'] = item.get('file', {}).get('mimeType', 'N/A')
                    item_info['download_url'] = item.get('@microsoft.graph.downloadUrl', 'N/A')
                    
                    print(f"{indent}{file_icon} {name}")
                    print(f"{indent}   Size: {format_file_size(size)}")
                    print(f"{indent}   Created: {created}")
                    print(f"{indent}   Modified: {modified}")
                    print(f"{indent}   Web URL: {web_url}")
                    
                    if item_info['download_url'] != 'N/A':
                        print(f"{indent}   Download: Available")
                    
                    all_items.append(item_info)
                    print()
        else:
            print(f"{indent}❌ Cannot access folder: {response.status_code}")
            if response.status_code == 404:
                print(f"{indent}   Folder may be empty or not exist")
            else:
                try:
                    error_info = response.json()
                    print(f"{indent}   Error: {error_info.get('error', {}).get('message', 'Unknown')}")
                except:
                    pass
    
    except Exception as e:
        print(f"{indent}❌ Error accessing folder: {e}")
    
    return all_items

async def test_personal_onedrive_with_userid():
    """Test accessing personal OneDrive using specific user ID/email."""
    print("🚀 Personal OneDrive Access with User ID")
    print("=" * 60)
    
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
        
        # Method 1: Try to get users first to find available user IDs
        print("\n🔍 Step 1: Looking for available users...")
        
        users_response = requests.get('https://graph.microsoft.com/v1.0/users?$top=10', headers=headers)
        
        available_users = []
        
        if users_response.status_code == 200:
            users_data = users_response.json()
            users = users_data.get('value', [])
            print(f'✅ Found {len(users)} users in organization')
            
            for i, user in enumerate(users):
                user_id = user.get('id')
                display_name = user.get('displayName', 'N/A')
                email = user.get('mail') or user.get('userPrincipalName', 'N/A')
                
                available_users.append({
                    'id': user_id,
                    'name': display_name,
                    'email': email
                })
                
                print(f'   {i+1}. {display_name}')
                print(f'      Email: {email}')
                print(f'      User ID: {user_id}')
                print()
        else:
            print(f'❌ Cannot list users: {users_response.status_code}')
            # Provide instructions for manual user ID entry
            print("\n💡 To access your personal OneDrive, you need your User ID or email.")
            print("   You can find this by:")
            print("   1. Going to Azure Portal → Azure Active Directory → Users")
            print("   2. Finding your user and copying the Object ID")
            print("   3. Or using your work email address")
            
            # For demonstration, we'll try a common pattern
            print("\n🔍 Trying common user email patterns...")
            tenant_domain = creds.microsoft_tenant_id  # Could also extract from email
            
            # You would replace this with your actual email
            example_emails = [
                "your-email@yourdomain.com",  # Replace with actual email
                f"user@{tenant_domain}",
                "admin@yourdomain.com"
            ]
            
            for email in example_emails:
                print(f"\n   Trying user: {email}")
                user_response = requests.get(f'https://graph.microsoft.com/v1.0/users/{email}', headers=headers)
                if user_response.status_code == 200:
                    user_info = user_response.json()
                    available_users.append({
                        'id': user_info.get('id'),
                        'name': user_info.get('displayName', 'N/A'),
                        'email': email
                    })
                    print(f"   ✅ Found user: {user_info.get('displayName', 'N/A')}")
                else:
                    print(f"   ❌ User not found: {user_response.status_code}")
        
        # Method 2: Access OneDrive for each found user
        if available_users:
            print(f"\n🔍 Step 2: Accessing OneDrive for found users...")
            
            for i, user in enumerate(available_users[:3]):  # Test first 3 users
                user_id = user['id']
                user_name = user['name']
                user_email = user['email']
                
                print(f"\n{'='*60}")
                print(f"👤 USER {i+1}: {user_name}")
                print(f"{'='*60}")
                print(f"📧 Email: {user_email}")
                print(f"🆔 User ID: {user_id}")
                
                # Try to access their OneDrive
                print(f"\n🔍 Accessing OneDrive...")
                drive_response = requests.get(f'https://graph.microsoft.com/v1.0/users/{user_id}/drive', headers=headers)
                
                if drive_response.status_code == 200:
                    drive_info = drive_response.json()
                    drive_name = drive_info.get('name', 'N/A')
                    drive_type = drive_info.get('driveType', 'N/A')
                    drive_id = drive_info.get('id', 'N/A')
                    
                    print(f"✅ OneDrive found: {drive_name}")
                    print(f"🏷️  Type: {drive_type}")
                    print(f"🆔 Drive ID: {drive_id}")
                    
                    # Get quota information
                    quota = drive_info.get('quota', {})
                    if quota:
                        total = quota.get('total', 0)
                        used = quota.get('used', 0)
                        remaining = quota.get('remaining', 0)
                        
                        if total > 0:
                            print(f"💾 Storage: {format_file_size(used)} used of {format_file_size(total)}")
                            print(f"📊 Usage: {(used / total) * 100:.1f}%")
                            print(f"💿 Available: {format_file_size(remaining)}")
                    
                    # List files in OneDrive root (similar to PowerShell command)
                    print(f"\n📋 OneDrive Contents:")
                    print("-" * 50)
                    
                    all_items = list_folder_contents(headers, user_id, "root", 0, 2)
                    
                    # Statistics
                    files = [item for item in all_items if not item['is_folder']]
                    folders = [item for item in all_items if item['is_folder']]
                    total_size = sum(item['size'] for item in files)
                    
                    print(f"\n📊 OneDrive Statistics:")
                    print(f"   📄 Files: {len(files)}")
                    print(f"   📁 Folders: {len(folders)}")
                    print(f"   📏 Total size: {format_file_size(total_size)}")
                    
                    # File type breakdown
                    if files:
                        type_stats = {}
                        for file_item in files:
                            ext = file_item['name'].split('.')[-1].lower() if '.' in file_item['name'] else 'no_ext'
                            if ext not in type_stats:
                                type_stats[ext] = {'count': 0, 'size': 0}
                            type_stats[ext]['count'] += 1
                            type_stats[ext]['size'] += file_item['size']
                        
                        print(f"\n📈 File Type Breakdown:")
                        for ext, stats in sorted(type_stats.items(), key=lambda x: x[1]['count'], reverse=True):
                            print(f"   .{ext}: {stats['count']} files ({format_file_size(stats['size'])})")
                
                elif drive_response.status_code == 403:
                    print(f"❌ Access denied to OneDrive")
                    print("   May need Files.Read.All permission or user may not have OneDrive")
                elif drive_response.status_code == 404:
                    print(f"❌ OneDrive not found for this user")
                    print("   User may not have OneDrive provisioned")
                else:
                    print(f"❌ Cannot access OneDrive: {drive_response.status_code}")
                    try:
                        error_details = drive_response.json()
                        print(f"   Error: {error_details.get('error', {}).get('message', 'Unknown')}")
                    except:
                        pass
        else:
            print("\n❌ No users found to test OneDrive access")
            print("\n💡 Instructions for manual testing:")
            print("   1. Replace 'your-email@yourdomain.com' with your actual work email")
            print("   2. Or get your User ID from Azure Portal")
            print("   3. Re-run this test")
        
        print(f"\n✅ Personal OneDrive test completed!")
        return True
        
    except Exception as e:
        print(f'\n❌ Test failed with error: {e}')
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function."""
    try:
        result = asyncio.run(test_personal_onedrive_with_userid())
        
        if result:
            print("\n🎉 Personal OneDrive test completed!")
            print("\nℹ️  If you want to access a specific user's OneDrive:")
            print("   Edit this script and replace 'your-email@yourdomain.com'")
            print("   with the actual user email you want to access.")
            return 0
        else:
            print("\n💥 Test failed. Check the output above for details.")
            return 1
            
    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted by user")
        return 1
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
