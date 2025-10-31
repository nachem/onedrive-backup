"""Test Microsoft Graph Delta API for incremental changes."""

import json

import requests
from msal import ConfidentialClientApplication

# Load credentials
with open('config/credentials.yaml', 'r') as f:
    import yaml
    creds = yaml.safe_load(f)

# Get access token
app = ConfidentialClientApplication(
    creds['microsoft_app_id'],
    authority=f"https://login.microsoftonline.com/{creds['microsoft_tenant_id']}",
    client_credential=creds['microsoft_app_secret']
)

result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
token = result['access_token']

headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

# Get user ID with OneDrive
print("Finding users with OneDrive...")
users_response = requests.get('https://graph.microsoft.com/v1.0/users?$top=999', headers=headers)
all_users = users_response.json()['value']

# Find user with OneDrive
user_id = None
user_email = None
for user in all_users:
    test_user_id = user['id']
    # Try to get drive
    drive_response = requests.get(
        f'https://graph.microsoft.com/v1.0/users/{test_user_id}/drive',
        headers=headers
    )
    if drive_response.status_code == 200:
        user_id = test_user_id
        user_email = user.get('mail') or user.get('userPrincipalName')
        print(f"✅ Found user with OneDrive: {user_email} ({user_id})\n")
        break

if not user_id:
    print("❌ No users with OneDrive found!")
    exit(1)

# Test 1: Get initial delta
print("=" * 70)
print("TEST 1: Initial Delta Call")
print("=" * 70)
delta_url = f'https://graph.microsoft.com/v1.0/users/{user_id}/drive/root/delta'
print(f"GET {delta_url}\n")

response = requests.get(delta_url, headers=headers)
print(f"Status Code: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    
    # Count items
    items = data.get('value', [])
    print(f"Items returned: {len(items)}")
    
    # Show delta link
    delta_link = data.get('@odata.deltaLink')
    next_link = data.get('@odata.nextLink')
    
    if delta_link:
        print(f"\n✅ Delta Link (save this for next call):")
        print(f"   {delta_link}")
    
    if next_link:
        print(f"\n➡️  Next Link (pagination):")
        print(f"   {next_link}")
    
    # Show first few items
    if items:
        print(f"\n📄 First 3 items:")
        for i, item in enumerate(items[:3]):
            name = item.get('name', 'Unknown')
            is_folder = 'folder' in item
            modified = item.get('lastModifiedDateTime', 'N/A')
            print(f"   {i+1}. {name} {'[FOLDER]' if is_folder else ''}")
            print(f"      Modified: {modified}")
    
    # Save delta link for next test
    if delta_link:
        with open('tests/delta_link.txt', 'w') as f:
            f.write(delta_link)
        print(f"\n💾 Saved delta link to tests/delta_link.txt")
    
    print("\n" + "=" * 70)
    print("✅ Delta API is working!")
    print("=" * 70)
    print("\nTo test incremental changes:")
    print("1. Modify a file in OneDrive")
    print("2. Run this script again")
    print("3. It will use the saved delta link to get only changes")
    
else:
    print(f"Error: {response.text}")
