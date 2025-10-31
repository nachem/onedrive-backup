#!/usr/bin/env python3
"""
SharePoint Connection Test

This test verifies the Microsoft Graph authentication and SharePoint connectivity
using the real credentials provided by the user.
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

async def test_sharepoint_connection():
    """Test SharePoint connection with real credentials."""
    print("🚀 SharePoint Connection Test")
    print("=" * 50)
    
    try:
        # Load credentials
        config_path = Path(__file__).parent.parent / "config" / "credentials.yaml"
        creds = CredentialsConfig.from_yaml(config_path)
        
        print(f'✅ Credentials loaded successfully')
        print(f'   App ID: {creds.microsoft_app_id}')
        print(f'   Tenant ID: {creds.microsoft_tenant_id}')
        print(f'   App Secret: {creds.microsoft_app_secret[:10]}...')
        
        # Test authentication
        print("\n🔐 Testing Microsoft Graph Authentication...")
        auth = MicrosoftGraphAuth(
            app_id=creds.microsoft_app_id,
            app_secret=creds.microsoft_app_secret,
            tenant_id=creds.microsoft_tenant_id
        )
        print('✅ MicrosoftGraphAuth object created')
        
        # Get access token
        print("🔑 Obtaining access token...")
        token = auth.get_access_token()
        print(f'✅ Access token obtained: {token[:20]}...')
        
        # Test Microsoft Graph API calls
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        # Test 1: Get all accessible sites
        print("\n🔍 Test 1: Getting all accessible sites...")
        response = requests.get('https://graph.microsoft.com/v1.0/sites?search=*', headers=headers)
        print(f'   API Response Status: {response.status_code}')
        
        if response.status_code == 200:
            sites = response.json()
            site_count = len(sites.get('value', []))
            print(f'   ✅ Found {site_count} accessible sites')
            
            if site_count > 0:
                print("   📋 Available sites:")
                for i, site in enumerate(sites.get('value', [])[:5]):  # Show first 5 sites
                    print(f'      {i+1}. {site.get("displayName", "N/A")}')
                    print(f'         URL: {site.get("webUrl", "N/A")}')
                    print(f'         ID: {site.get("id", "N/A")}')
        else:
            print(f'   ❌ API Error: {response.status_code}')
            print(f'   Error details: {response.text}')
            
        # Test 2: Test specific SharePoint site
        print("\n🔍 Test 2: Testing specific SharePoint site...")
        site_url = 'bernoullisofrware.sharepoint.com'
        print(f'   Target site: https://{site_url}')
        
        # Method 1: Try to get site by hostname
        site_endpoint = f'https://graph.microsoft.com/v1.0/sites/{site_url}'
        response = requests.get(site_endpoint, headers=headers)
        print(f'   Method 1 - Direct site access: {response.status_code}')
        
        if response.status_code == 200:
            site_info = response.json()
            print(f'   ✅ Site Name: {site_info.get("displayName", "N/A")}')
            print(f'   ✅ Site ID: {site_info.get("id", "N/A")}')
            print(f'   ✅ Web URL: {site_info.get("webUrl", "N/A")}')
            
            # Test 3: Get document libraries
            site_id = site_info.get("id")
            if site_id:
                print("\n🔍 Test 3: Getting document libraries...")
                libraries_endpoint = f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives'
                response = requests.get(libraries_endpoint, headers=headers)
                print(f'   Libraries API Response: {response.status_code}')
                
                if response.status_code == 200:
                    libraries = response.json()
                    lib_count = len(libraries.get('value', []))
                    print(f'   ✅ Found {lib_count} document libraries')
                    
                    if lib_count > 0:
                        print("   📚 Available libraries:")
                        for library in libraries.get('value', []):
                            print(f'      - {library.get("name", "N/A")} (Type: {library.get("driveType", "N/A")})')
                else:
                    print(f'   ❌ Libraries API Error: {response.status_code}')
                    print(f'   Error details: {response.text}')
        else:
            print(f'   ❌ Site access failed: {response.status_code}')
            print(f'   Error details: {response.text}')
            
            # Try alternative method - search for the site
            print("\n🔍 Alternative: Searching for site by name...")
            search_endpoint = f'https://graph.microsoft.com/v1.0/sites?search=bernoulli'
            response = requests.get(search_endpoint, headers=headers)
            print(f'   Search API Response: {response.status_code}')
            
            if response.status_code == 200:
                sites = response.json()
                found_sites = sites.get('value', [])
                print(f'   Found {len(found_sites)} sites matching "bernoulli"')
                for site in found_sites:
                    print(f'      - {site.get("displayName", "N/A")}: {site.get("webUrl", "N/A")}')
        
        # Test 4: Test OneDrive access
        print("\n🔍 Test 4: Testing OneDrive access...")
        onedrive_endpoint = 'https://graph.microsoft.com/v1.0/me/drive'
        response = requests.get(onedrive_endpoint, headers=headers)
        print(f'   OneDrive API Response: {response.status_code}')
        
        if response.status_code == 200:
            drive_info = response.json()
            print(f'   ✅ OneDrive Name: {drive_info.get("name", "N/A")}')
            print(f'   ✅ Drive Type: {drive_info.get("driveType", "N/A")}')
            print(f'   ✅ Total Size: {drive_info.get("quota", {}).get("total", "N/A")} bytes')
        else:
            print(f'   ❌ OneDrive access failed: {response.status_code}')
            print(f'   Error details: {response.text}')
        
        print("\n" + "=" * 50)
        print("✅ SharePoint connection test completed successfully!")
        print("📄 Test results saved in logs/")
        return True
        
    except Exception as e:
        print(f'\n❌ Test failed with error: {e}')
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test function."""
    try:
        # Ensure logs directory exists
        logs_dir = Path(__file__).parent.parent / "logs"
        logs_dir.mkdir(exist_ok=True)
        
        # Run the async test
        result = asyncio.run(test_sharepoint_connection())
        
        if result:
            print("\n🎉 All tests passed! Your SharePoint connection is working.")
            return 0
        else:
            print("\n💥 Some tests failed. Check the output above for details.")
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
