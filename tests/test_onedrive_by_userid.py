#!/usr/bin/env python3
"""
OneDrive Access by User ID Test

This test shows how to:
1. Get all users in the organization
2. Access each user's OneDrive using their User ID
3. List files in each user's OneDrive
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

async def test_onedrive_by_userid():
    """Test accessing OneDrive using specific User IDs."""
    print("🚀 OneDrive Access by User ID Test")
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
        
        # Step 1: Get all users in the organization
        print("\n🔍 Step 1: Getting all users in organization...")
        
        # Try different approaches to get users
        user_endpoints = [
            ('https://graph.microsoft.com/v1.0/users', 'Standard users endpoint'),
            ('https://graph.microsoft.com/v1.0/users?$top=10', 'Users with limit'),
            ('https://graph.microsoft.com/v1.0/users?$select=id,displayName,mail,userPrincipalName', 'Users with specific fields'),
        ]
        
        users_found = []
        
        for endpoint, description in user_endpoints:
            print(f"\n   Trying: {description}")
            response = requests.get(endpoint, headers=headers)
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                users_data = response.json()
                users = users_data.get('value', [])
                print(f"   ✅ Found {len(users)} users")
                
                for i, user in enumerate(users[:5]):  # Show first 5 users
                    user_id = user.get('id')
                    display_name = user.get('displayName', 'N/A')
                    email = user.get('mail') or user.get('userPrincipalName', 'N/A')
                    print(f"      {i+1}. {display_name} ({email})")
                    print(f"         User ID: {user_id}")
                    
                    users_found.append({
                        'id': user_id,
                        'name': display_name,
                        'email': email
                    })
                
                if users:
                    break  # Use the first successful endpoint
                    
            elif response.status_code == 403:
                print(f"   ❌ Access denied: Need User.Read.All permission")
            else:
                print(f"   ❌ Error: {response.status_code}")
                try:
                    error_details = response.json()
                    print(f"   Error details: {error_details.get('error', {}).get('message', 'Unknown')}")
                except:
                    print(f"   Error text: {response.text[:200]}...")
        
        # Step 2: If we couldn't get users, try alternative approaches
        if not users_found:
            print("\n🔍 Alternative: Try to find users through SharePoint sites...")
            
            # Get site owners/members through SharePoint
            sites_response = requests.get('https://graph.microsoft.com/v1.0/sites?search=*', headers=headers)
            if sites_response.status_code == 200:
                sites = sites_response.json()
                
                for site in sites.get('value', [])[:3]:  # Check first 3 sites
                    site_id = site.get('id')
                    site_name = site.get('displayName', 'N/A')
                    
                    print(f"\n   Checking site: {site_name}")
                    
                    # Try to get site members
                    members_endpoint = f'https://graph.microsoft.com/v1.0/sites/{site_id}/members'
                    members_response = requests.get(members_endpoint, headers=headers)
                    
                    if members_response.status_code == 200:
                        members = members_response.json()
                        for member in members.get('value', [])[:3]:
                            if member.get('@odata.type') == '#microsoft.graph.user':
                                user_id = member.get('id')
                                display_name = member.get('displayName', 'N/A')
                                email = member.get('mail') or member.get('userPrincipalName', 'N/A')
                                
                                users_found.append({
                                    'id': user_id,
                                    'name': display_name,
                                    'email': email
                                })
                                
                                print(f"      Found user: {display_name} ({email})")
                                print(f"      User ID: {user_id}")
        
        # Step 3: Access OneDrive for each found user
        if users_found:
            print(f"\n🔍 Step 2: Accessing OneDrive for {len(users_found)} users...")
            
            for i, user in enumerate(users_found[:3]):  # Test first 3 users
                user_id = user['id']
                user_name = user['name']
                user_email = user['email']
                
                print(f"\n   👤 User {i+1}: {user_name} ({user_email})")
                print(f"      User ID: {user_id}")
                
                # Method 1: Access user's OneDrive directly
                onedrive_endpoint = f'https://graph.microsoft.com/v1.0/users/{user_id}/drive'
                onedrive_response = requests.get(onedrive_endpoint, headers=headers)
                
                print(f"      OneDrive access: {onedrive_response.status_code}")
                
                if onedrive_response.status_code == 200:
                    drive_info = onedrive_response.json()
                    drive_name = drive_info.get('name', 'N/A')
                    drive_type = drive_info.get('driveType', 'N/A')
                    drive_id = drive_info.get('id', 'N/A')
                    
                    print(f"      ✅ OneDrive found: {drive_name}")
                    print(f"         Drive Type: {drive_type}")
                    print(f"         Drive ID: {drive_id}")
                    
                    # List files in OneDrive root
                    files_endpoint = f'https://graph.microsoft.com/v1.0/users/{user_id}/drive/root/children'
                    files_response = requests.get(files_endpoint, headers=headers)
                    
                    print(f"      Files access: {files_response.status_code}")
                    
                    if files_response.status_code == 200:
                        files = files_response.json()
                        file_count = len(files.get('value', []))
                        print(f"      📁 Total files/folders: {file_count}")
                        
                        if file_count > 0:
                            print(f"      📋 Files and folders:")
                            for file_item in files.get('value', [])[:5]:  # Show first 5 items
                                file_type = "📁" if file_item.get('folder') else "📄"
                                name = file_item.get('name', 'N/A')
                                size = file_item.get('size', 0)
                                modified = file_item.get('lastModifiedDateTime', 'N/A')[:10]
                                print(f"         {file_type} {name} ({size} bytes, modified: {modified})")
                        else:
                            print(f"      📁 OneDrive is empty or no files accessible")
                    
                    elif files_response.status_code == 403:
                        print(f"      ❌ Access denied to files: Need Files.Read.All permission")
                    else:
                        print(f"      ❌ Cannot access files: {files_response.status_code}")
                        try:
                            error_details = files_response.json()
                            print(f"      Error: {error_details.get('error', {}).get('message', 'Unknown')}")
                        except:
                            pass
                
                elif onedrive_response.status_code == 403:
                    print(f"      ❌ Access denied to OneDrive: Need Files.Read.All permission")
                elif onedrive_response.status_code == 404:
                    print(f"      ❌ OneDrive not found for this user")
                else:
                    print(f"      ❌ Cannot access OneDrive: {onedrive_response.status_code}")
                    try:
                        error_details = onedrive_response.json()
                        print(f"      Error: {error_details.get('error', {}).get('message', 'Unknown')}")
                    except:
                        pass
                
                # Method 2: Try to access via drives endpoint
                user_drives_endpoint = f'https://graph.microsoft.com/v1.0/users/{user_id}/drives'
                drives_response = requests.get(user_drives_endpoint, headers=headers)
                
                if drives_response.status_code == 200:
                    drives = drives_response.json()
                    drive_count = len(drives.get('value', []))
                    if drive_count > 0:
                        print(f"      📊 Alternative access: Found {drive_count} drives for user")
                        for drive in drives.get('value', []):
                            print(f"         • {drive.get('name', 'N/A')} (Type: {drive.get('driveType', 'N/A')})")
        
        else:
            print("\n❌ No users found. Cannot test OneDrive access by User ID.")
            print("   This might be due to insufficient permissions (User.Read.All needed)")
        
        # Step 4: Show required permissions
        print(f"\n📋 Required Azure AD App Permissions for OneDrive access:")
        print(f"   Application Permissions:")
        print(f"   • Files.Read.All - Read files in all site collections") 
        print(f"   • Sites.Read.All - Read items in all site collections")
        print(f"   • User.Read.All - Read all users' profiles")
        print(f"   ")
        print(f"   Delegated Permissions (for interactive access):")
        print(f"   • Files.Read.All - Read user files")
        print(f"   • User.Read - Read user profile")
        
        print("\n" + "=" * 50)
        print("✅ OneDrive User ID test completed!")
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
        result = asyncio.run(test_onedrive_by_userid())
        
        if result:
            print("\n🎉 User ID OneDrive test completed!")
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
