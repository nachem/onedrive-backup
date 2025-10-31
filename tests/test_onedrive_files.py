#!/usr/bin/env python3
"""
OneDrive Files Test

This test attempts to list OneDrive files using different approaches:
1. Direct OneDrive access (will fail with app-only auth)
2. OneDrive for Business through SharePoint sites
3. User drives accessible through Graph API
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

async def test_onedrive_files():
    """Test different methods to access OneDrive files."""
    print("🚀 OneDrive Files Discovery Test")
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
        
        # Method 1: Try direct personal OneDrive access (expected to fail)
        print("\n🔍 Method 1: Direct personal OneDrive access...")
        response = requests.get('https://graph.microsoft.com/v1.0/me/drive', headers=headers)
        print(f'   Personal OneDrive API: {response.status_code}')
        if response.status_code == 200:
            drive_info = response.json()
            print(f'   ✅ OneDrive Name: {drive_info.get("name", "N/A")}')
        else:
            print(f'   ❌ Expected failure: {response.json().get("error", {}).get("message", "Unknown")}')
        
        # Method 2: Get all drives accessible to the app
        print("\n🔍 Method 2: All accessible drives...")
        response = requests.get('https://graph.microsoft.com/v1.0/drives', headers=headers)
        print(f'   All drives API: {response.status_code}')
        
        if response.status_code == 200:
            drives = response.json()
            drive_count = len(drives.get('value', []))
            print(f'   ✅ Found {drive_count} accessible drives')
            
            for i, drive in enumerate(drives.get('value', [])[:10]):  # Show first 10
                print(f'      {i+1}. {drive.get("name", "N/A")} (Type: {drive.get("driveType", "N/A")})')
                print(f'         ID: {drive.get("id", "N/A")}')
                print(f'         Owner: {drive.get("owner", {}).get("user", {}).get("displayName", "N/A")}')
                
                # Try to list files in this drive
                drive_id = drive.get('id')
                if drive_id:
                    files_response = requests.get(
                        f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children',
                        headers=headers
                    )
                    if files_response.status_code == 200:
                        files = files_response.json()
                        file_count = len(files.get('value', []))
                        print(f'         📁 Files/Folders: {file_count}')
                        
                        # Show first few files
                        for j, file_item in enumerate(files.get('value', [])[:3]):
                            file_type = "📁" if file_item.get('folder') else "📄"
                            print(f'            {file_type} {file_item.get("name", "N/A")}')
                    else:
                        print(f'         ❌ Cannot access files: {files_response.status_code}')
                print()
        else:
            print(f'   ❌ Error: {response.text}')
        
        # Method 3: Get drives through SharePoint sites
        print("\n🔍 Method 3: OneDrive through SharePoint sites...")
        
        # First get all sites
        sites_response = requests.get('https://graph.microsoft.com/v1.0/sites?search=*', headers=headers)
        if sites_response.status_code == 200:
            sites = sites_response.json()
            print(f'   Checking {len(sites.get("value", []))} sites for OneDrive content...')
            
            onedrive_found = False
            for site in sites.get('value', []):
                site_id = site.get('id')
                site_name = site.get('displayName', 'N/A')
                
                if site_id:
                    # Get drives for this site
                    drives_response = requests.get(
                        f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives',
                        headers=headers
                    )
                    
                    if drives_response.status_code == 200:
                        site_drives = drives_response.json()
                        for drive in site_drives.get('value', []):
                            drive_type = drive.get('driveType', '')
                            drive_name = drive.get('name', 'N/A')
                            
                            # Look for OneDrive-type drives
                            if 'onedrive' in drive_type.lower() or 'personal' in drive_name.lower():
                                onedrive_found = True
                                print(f'   ✅ Found OneDrive in site "{site_name}":')
                                print(f'      Drive: {drive_name} (Type: {drive_type})')
                                print(f'      ID: {drive.get("id", "N/A")}')
                                
                                # Try to list files
                                drive_id = drive.get('id')
                                if drive_id:
                                    files_response = requests.get(
                                        f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children',
                                        headers=headers
                                    )
                                    if files_response.status_code == 200:
                                        files = files_response.json()
                                        file_count = len(files.get('value', []))
                                        print(f'      📁 Total items: {file_count}')
                                        
                                        print(f'      📋 Files and folders:')
                                        for file_item in files.get('value', [])[:10]:  # Show first 10
                                            file_type = "📁" if file_item.get('folder') else "📄"
                                            size = file_item.get('size', 0)
                                            modified = file_item.get('lastModifiedDateTime', 'N/A')
                                            print(f'         {file_type} {file_item.get("name", "N/A")} ({size} bytes, {modified[:10]})')
                                    else:
                                        print(f'      ❌ Cannot access files: {files_response.status_code}')
                                print()
            
            if not onedrive_found:
                print('   ℹ️  No OneDrive-specific drives found in SharePoint sites')
        
        # Method 4: Try to find user-specific drives
        print("\n🔍 Method 4: Search for user drives...")
        
        # Try to get users and their drives
        users_response = requests.get('https://graph.microsoft.com/v1.0/users', headers=headers)
        if users_response.status_code == 200:
            users = users_response.json()
            user_count = len(users.get('value', []))
            print(f'   Found {user_count} users in directory')
            
            for user in users.get('value', [])[:5]:  # Check first 5 users
                user_id = user.get('id')
                user_name = user.get('displayName', 'N/A')
                user_email = user.get('mail') or user.get('userPrincipalName', 'N/A')
                
                print(f'   👤 Checking user: {user_name} ({user_email})')
                
                # Try to get their drive
                user_drive_response = requests.get(
                    f'https://graph.microsoft.com/v1.0/users/{user_id}/drive',
                    headers=headers
                )
                
                if user_drive_response.status_code == 200:
                    user_drive = user_drive_response.json()
                    print(f'      ✅ OneDrive found: {user_drive.get("name", "N/A")}')
                    print(f'      Drive Type: {user_drive.get("driveType", "N/A")}')
                    
                    # Try to list files
                    files_response = requests.get(
                        f'https://graph.microsoft.com/v1.0/users/{user_id}/drive/root/children',
                        headers=headers
                    )
                    if files_response.status_code == 200:
                        files = files_response.json()
                        file_count = len(files.get('value', []))
                        print(f'      📁 Files: {file_count}')
                        
                        for file_item in files.get('value', [])[:3]:  # Show first 3
                            file_type = "📁" if file_item.get('folder') else "📄"
                            print(f'         {file_type} {file_item.get("name", "N/A")}')
                    else:
                        print(f'      ❌ Cannot access files: {files_response.status_code}')
                else:
                    print(f'      ❌ Cannot access drive: {user_drive_response.status_code}')
                print()
        else:
            print(f'   ❌ Cannot list users: {users_response.status_code}')
        
        print("\n" + "=" * 50)
        print("✅ OneDrive discovery test completed!")
        print("📄 Check the results above to see what OneDrive content is accessible")
        return True
        
    except Exception as e:
        print(f'\n❌ Test failed with error: {e}')
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test function."""
    try:
        # Run the async test
        result = asyncio.run(test_onedrive_files())
        
        if result:
            print("\n🎉 OneDrive discovery test completed!")
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
