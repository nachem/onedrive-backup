#!/usr/bin/env python3
"""
List Personal OneDrive Files

This test attempts to access your personal OneDrive for Business account
using different methods to find your specific user account.
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

def list_folder_contents(headers, drive_id, folder_id="root", level=0, max_level=2):
    """List contents of a folder."""
    if level > max_level:
        return 0, 0  # files, folders
    
    indent = "  " * level
    
    if folder_id == "root":
        endpoint = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children'
    else:
        endpoint = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}/children'
    
    response = requests.get(endpoint, headers=headers)
    
    file_count = 0
    folder_count = 0
    
    if response.status_code == 200:
        items = response.json()
        
        for item in items.get('value', []):
            name = item.get('name', 'N/A')
            size = item.get('size', 0)
            modified = item.get('lastModifiedDateTime', 'N/A')
            
            if modified != 'N/A':
                modified = modified[:19].replace('T', ' ')
            
            if item.get('folder'):
                folder_count += 1
                child_count = item.get('folder', {}).get('childCount', 0)
                print(f"{indent}📁 {name}/ ({child_count} items)")
                print(f"{indent}   Modified: {modified}")
                
                # Recursively count contents
                if level < max_level and child_count > 0:
                    sub_files, sub_folders = list_folder_contents(headers, drive_id, item.get('id'), level + 1, max_level)
                    file_count += sub_files
                    folder_count += sub_folders
            else:
                file_count += 1
                file_type = "📄"
                file_ext = name.split('.')[-1].lower() if '.' in name else ""
                
                if file_ext in ['jpg', 'jpeg', 'png', 'gif']:
                    file_type = "🖼️"
                elif file_ext in ['doc', 'docx']:
                    file_type = "📝"
                elif file_ext in ['xls', 'xlsx']:
                    file_type = "📊"
                elif file_ext in ['ppt', 'pptx']:
                    file_type = "📽️"
                elif file_ext in ['pdf']:
                    file_type = "📑"
                
                print(f"{indent}{file_type} {name}")
                print(f"{indent}   Size: {format_file_size(size)}")
                print(f"{indent}   Modified: {modified}")
    else:
        print(f"{indent}❌ Cannot access folder: {response.status_code}")
    
    return file_count, folder_count

async def list_personal_onedrive():
    """Find and list personal OneDrive for Business files."""
    print("🚀 Personal OneDrive for Business File Listing")
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
        
        # Method 1: Try to find your personal OneDrive through drives
        print("\n🔍 Method 1: Looking for personal OneDrive drives...")
        response = requests.get('https://graph.microsoft.com/v1.0/drives', headers=headers)
        
        personal_onedrive_found = False
        
        if response.status_code == 200:
            drives = response.json()
            print(f'Found {len(drives.get("value", []))} drives total')
            
            for drive in drives.get('value', []):
                drive_name = drive.get('name', 'N/A')
                drive_type = drive.get('driveType', 'N/A')
                drive_id = drive.get('id', 'N/A')
                owner = drive.get('owner', {})
                
                # Look for personal OneDrive indicators
                if (drive_type == 'business' or 
                    'onedrive' in drive_name.lower() or
                    owner.get('user', {}).get('displayName')):
                    
                    print(f"\n📂 Potential Personal OneDrive Found:")
                    print(f"   Name: {drive_name}")
                    print(f"   Type: {drive_type}")
                    print(f"   ID: {drive_id}")
                    
                    owner_info = owner.get('user', {})
                    if owner_info:
                        print(f"   Owner: {owner_info.get('displayName', 'N/A')}")
                        print(f"   Email: {owner_info.get('email', 'N/A')}")
                    
                    # Get quota info
                    quota = drive.get('quota', {})
                    if quota:
                        total = quota.get('total', 0)
                        used = quota.get('used', 0)
                        if total > 0:
                            print(f"   Storage: {format_file_size(used)} used of {format_file_size(total)}")
                    
                    # List files in this drive
                    print(f"\n   📋 Contents:")
                    print(f"   {'-' * 40}")
                    
                    file_count, folder_count = list_folder_contents(headers, drive_id, "root", 0, 2)
                    
                    print(f"\n   📊 Summary:")
                    print(f"   Files: {file_count}")
                    print(f"   Folders: {folder_count}")
                    
                    personal_onedrive_found = True
        
        # Method 2: Try to get drives through SharePoint but filter for OneDrive
        if not personal_onedrive_found:
            print("\n🔍 Method 2: Looking through SharePoint for OneDrive...")
            
            sites_response = requests.get('https://graph.microsoft.com/v1.0/sites?search=*', headers=headers)
            if sites_response.status_code == 200:
                sites = sites_response.json()
                
                for site in sites.get('value', []):
                    site_name = site.get('displayName', '')
                    site_url = site.get('webUrl', '')
                    
                    # Look for personal site indicators
                    if ('-my.sharepoint.com' in site_url or 
                        'personal' in site_name.lower() or
                        'onedrive' in site_name.lower()):
                        
                        print(f"\n📂 Personal Site Found: {site_name}")
                        print(f"   URL: {site_url}")
                        
                        site_id = site.get('id')
                        if site_id:
                            drives_response = requests.get(f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives', headers=headers)
                            
                            if drives_response.status_code == 200:
                                site_drives = drives_response.json()
                                
                                for drive in site_drives.get('value', []):
                                    drive_name = drive.get('name', 'N/A')
                                    drive_type = drive.get('driveType', 'N/A')
                                    drive_id = drive.get('id', 'N/A')
                                    
                                    print(f"\n   📁 Drive: {drive_name} (Type: {drive_type})")
                                    
                                    # List files
                                    file_count, folder_count = list_folder_contents(headers, drive_id, "root", 1, 2)
                                    
                                    print(f"\n   📊 Drive Summary:")
                                    print(f"   Files: {file_count}")
                                    print(f"   Folders: {folder_count}")
                                    
                                    personal_onedrive_found = True
        
        # Method 3: Try to find your user and access their OneDrive
        if not personal_onedrive_found:
            print("\n🔍 Method 3: Trying to find your user account...")
            
            # Try to get the app service principal to find the user
            me_response = requests.get('https://graph.microsoft.com/v1.0/me', headers=headers)
            if me_response.status_code != 200:
                print("   Cannot use /me endpoint with app-only auth (expected)")
            
            # Try to get users (may fail due to permissions)
            users_response = requests.get('https://graph.microsoft.com/v1.0/users?$top=5', headers=headers)
            if users_response.status_code == 200:
                users = users_response.json()
                print(f"   Found {len(users.get('value', []))} users")
                
                for user in users.get('value', [])[:3]:  # Check first 3 users
                    user_id = user.get('id')
                    user_name = user.get('displayName', 'N/A')
                    user_email = user.get('mail') or user.get('userPrincipalName', 'N/A')
                    
                    print(f"\n   👤 User: {user_name} ({user_email})")
                    
                    # Try to access their OneDrive
                    user_drive_response = requests.get(f'https://graph.microsoft.com/v1.0/users/{user_id}/drive', headers=headers)
                    
                    if user_drive_response.status_code == 200:
                        drive_info = user_drive_response.json()
                        drive_name = drive_info.get('name', 'N/A')
                        drive_type = drive_info.get('driveType', 'N/A')
                        drive_id = drive_info.get('id', 'N/A')
                        
                        print(f"      ✅ OneDrive: {drive_name} (Type: {drive_type})")
                        
                        # List files
                        file_count, folder_count = list_folder_contents(headers, drive_id, "root", 2, 3)
                        
                        print(f"\n      📊 OneDrive Summary:")
                        print(f"      Files: {file_count}")
                        print(f"      Folders: {folder_count}")
                        
                        personal_onedrive_found = True
                    else:
                        print(f"      ❌ Cannot access OneDrive: {user_drive_response.status_code}")
            else:
                print(f"   ❌ Cannot list users: {users_response.status_code}")
                print("   Need User.Read.All permission for this method")
        
        if not personal_onedrive_found:
            print("\n❌ Could not find personal OneDrive for Business")
            print("\n💡 Possible reasons:")
            print("   • Need User.Read.All permission to enumerate users")
            print("   • OneDrive may be accessed through SharePoint (what we found earlier)")
            print("   • May need delegated authentication for personal access")
            
            print("\n📋 What we found earlier were SharePoint document libraries")
            print("   These may actually BE your OneDrive content, just accessed via SharePoint")
        
        print("\n" + "=" * 60)
        print("✅ Personal OneDrive search completed!")
        return True
        
    except Exception as e:
        print(f'\n❌ Test failed with error: {e}')
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function."""
    try:
        result = asyncio.run(list_personal_onedrive())
        
        if result:
            print("\n🎉 Personal OneDrive search completed!")
            return 0
        else:
            print("\n💥 Search failed. Check the output above for details.")
            return 1
            
    except KeyboardInterrupt:
        print("\n⚠️ Search interrupted by user")
        return 1
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
