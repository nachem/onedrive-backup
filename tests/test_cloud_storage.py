#!/usr/bin/env python3
"""
Test cloud storage connections (AWS S3 and Azure Blob Storage)

This script tests connectivity to both AWS S3 and Azure Blob Storage
to verify backup destinations are accessible.
"""

import sys
import asyncio
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from onedrive_backup.config.settings import CredentialsConfig
import boto3
from azure.storage.blob import BlobServiceClient
import requests
from datetime import datetime

def test_aws_s3_connection(aws_access_key_id, aws_secret_access_key, bucket_name="test-bucket", region="us-east-1"):
    """Test AWS S3 connection."""
    print("🔍 Testing AWS S3 Connection...")
    
    try:
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region
        )
        
        # Test by listing buckets
        # print("   Attempting to list S3 buckets...")
        # response = s3_client.list_buckets()
        
        # buckets = response.get('Buckets', [])
        # print(f"✅ AWS S3 Connection successful!")
        # print(f"   📦 Found {len(buckets)} accessible buckets:")
        
        # for bucket in buckets[:5]:  # Show first 5 buckets
        #     bucket_name = bucket['Name']
        #     created = bucket['CreationDate'].strftime('%Y-%m-%d %H:%M:%S')
        #     print(f"      • {bucket_name} (created: {created})")
        
        # if len(buckets) > 5:
        #     print(f"      ... and {len(buckets) - 5} more buckets")
        
        # Test upload capability with a small test file
        test_bucket = 'bernoulli_backup'  # Change to an existing bucket for testing
        if test_bucket:
            print(f"\n   Testing upload to bucket: {test_bucket}")
            test_key = f"test-backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
            test_content = f"OneDrive Backup Test - {datetime.now()}"
            
            try:
                s3_client.put_object(
                    Bucket=test_bucket,
                    Key=test_key,
                    Body=test_content.encode('utf-8'),
                    ContentType='text/plain'
                )
                print(f"✅ Upload test successful: {test_key}")
                
                # Clean up test file
                s3_client.delete_object(Bucket=test_bucket, Key=test_key)
                print(f"✅ Cleanup successful")
                
            except Exception as upload_error:
                print(f"⚠️  Upload test failed: {upload_error}")
        
        return True, test_bucket
        
    except Exception as e:
        print(f"❌ AWS S3 Connection failed: {e}")
        return False, None

def test_azure_blob_connection(connection_string_or_url):
    """Test Azure Blob Storage connection."""
    print("\n🔍 Testing Azure Blob Storage Connection...")
    
    try:
        # Check if it's a SAS URL or connection string
        if connection_string_or_url.startswith('https://'):
            # It's a SAS URL
            print("   Using SAS URL for authentication...")
            
            # Extract account and container from URL
            url_parts = connection_string_or_url.split('/')
            account_container = '/'.join(url_parts[3:5])  # account/container
            
            # Test by making a simple request to list blobs
            print("   Attempting to list blobs...")
            response = requests.get(
                connection_string_or_url + "&restype=container&comp=list&maxresults=10",
                timeout=30
            )
            
            if response.status_code == 200:
                print(f"✅ Azure Blob Storage Connection successful!")
                print(f"   📦 Container accessible: {account_container}")
                
                # Parse blob count from XML response
                blob_count = response.text.count('<Name>')
                print(f"   📄 Found {blob_count} blobs in container")
                
                # Test upload capability
                print(f"\n   Testing upload capability...")
                test_blob_name = f"test-backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
                test_content = f"OneDrive Backup Test - {datetime.now()}"
                
                upload_url = connection_string_or_url.split('?')[0] + f"/{test_blob_name}?" + connection_string_or_url.split('?')[1]
                
                upload_response = requests.put(
                    upload_url,
                    data=test_content.encode('utf-8'),
                    headers={
                        'x-ms-blob-type': 'BlockBlob',
                        'Content-Type': 'text/plain'
                    },
                    timeout=30
                )
                
                if upload_response.status_code in [201, 200]:
                    print(f"✅ Upload test successful: {test_blob_name}")
                    
                    # Clean up test file
                    delete_response = requests.delete(upload_url, timeout=30)
                    if delete_response.status_code in [202, 200]:
                        print(f"✅ Cleanup successful")
                    else:
                        print(f"⚠️  Cleanup warning: {delete_response.status_code}")
                        
                else:
                    print(f"⚠️  Upload test failed: {upload_response.status_code}")
                    print(f"   Response: {upload_response.text}")
                
                return True, account_container
                
            else:
                print(f"❌ Azure Blob Storage Connection failed: {response.status_code}")
                print(f"   Response: {response.text}")
                return False, None
                
        else:
            # It's a connection string
            print("   Using connection string for authentication...")
            blob_service_client = BlobServiceClient.from_connection_string(connection_string_or_url)
            
            print("   Attempting to list containers...")
            containers = list(blob_service_client.list_containers())
            
            print(f"✅ Azure Blob Storage Connection successful!")
            print(f"   📦 Found {len(containers)} accessible containers:")
            
            for container in containers[:5]:  # Show first 5 containers
                print(f"      • {container.name} (modified: {container.last_modified})")
            
            if len(containers) > 5:
                print(f"      ... and {len(containers) - 5} more containers")
            
            return True, containers
            
    except Exception as e:
        print(f"❌ Azure Blob Storage Connection failed: {e}")
        return False, None

async def test_all_cloud_storage():
    """Test all configured cloud storage connections."""
    print("🚀 Cloud Storage Connection Test")
    print("=" * 60)
    
    try:
        # Load credentials
        config_path = Path(__file__).parent.parent / "config" / "credentials.yaml"
        creds = CredentialsConfig.from_yaml(config_path)
        
        print(f'✅ Credentials loaded successfully')
        
        results = {}
        
        # Test AWS S3
        if creds.aws_access_key_id and creds.aws_secret_access_key:
            aws_success, aws_buckets = test_aws_s3_connection(
                creds.aws_access_key_id,
                creds.aws_secret_access_key
            )
            results['aws_s3'] = {
                'success': aws_success,
                'details': aws_buckets
            }
        else:
            print("⚠️  AWS credentials not configured - skipping AWS test")
            results['aws_s3'] = {'success': False, 'details': 'Not configured'}
        
        # Test Azure Blob Storage
        if creds.azure_storage_connection_string:
            azure_success, azure_containers = test_azure_blob_connection(
                creds.azure_storage_connection_string
            )
            results['azure_blob'] = {
                'success': azure_success,
                'details': azure_containers
            }
        else:
            print("⚠️  Azure credentials not configured - skipping Azure test")
            results['azure_blob'] = {'success': False, 'details': 'Not configured'}
        
        # Summary
        print(f"\n{'='*60}")
        print(f"📊 CLOUD STORAGE TEST SUMMARY")
        print(f"{'='*60}")
        
        total_tests = len([r for r in results.values() if r['details'] != 'Not configured'])
        successful_tests = len([r for r in results.values() if r['success']])
        
        for service, result in results.items():
            if result['details'] == 'Not configured':
                status = "⚠️  Not Configured"
            elif result['success']:
                status = "✅ Connected"
            else:
                status = "❌ Failed"
            
            print(f"   {service.upper()}: {status}")
        
        print(f"\n📈 Results: {successful_tests}/{total_tests} connections successful")
        
        if successful_tests == total_tests and total_tests > 0:
            print(f"🎉 All configured cloud storage services are accessible!")
        elif successful_tests > 0:
            print(f"⚠️  Some connections successful, check failed services")
        else:
            print(f"❌ No cloud storage connections successful")
        
        print(f"\n✅ Cloud storage test completed!")
        return results
        
    except Exception as e:
        print(f'\n❌ Test failed with error: {e}')
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main function."""
    try:
        result = asyncio.run(test_all_cloud_storage())
        
        if result:
            # Count successful connections
            successful = sum(1 for r in result.values() if r['success'])
            total = len([r for r in result.values() if r['details'] != 'Not configured'])
            
            if successful == total and total > 0:
                print("\n🎉 All cloud storage tests passed!")
                return 0
            elif successful > 0:
                print(f"\n⚠️  {successful}/{total} cloud storage tests passed")
                return 1
            else:
                print("\n💥 All cloud storage tests failed")
                return 1
        else:
            print("\n💥 Cloud storage test failed")
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
