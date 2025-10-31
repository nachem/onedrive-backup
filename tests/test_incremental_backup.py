"""Test incremental backup by setting fake last backup timestamp."""

import json
from datetime import datetime, timedelta

import boto3

# AWS S3 Configuration (from your config)
BUCKET_NAME = "bernoulli-backup"
PREFIX = "backups/onedrive/"

# Sources to test
SOURCES = [
    "My Personal OneDrive",
    "Work OneDrive"
]

def upload_fake_metadata():
    """Upload fake metadata files with timestamp from 3 days ago."""
    s3_client = boto3.client('s3')
    
    # Calculate timestamp from 3 days ago
    three_days_ago = datetime.utcnow() - timedelta(days=3)
    timestamp = three_days_ago.isoformat() + 'Z'
    
    print(f"Setting last backup timestamp to: {timestamp}")
    print(f"This will retrieve files modified after: {three_days_ago.strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
    
    for source_name in SOURCES:
        metadata_key = f"{PREFIX}.backup-metadata/{source_name}_last_backup.json".lstrip('/')
        
        metadata = {
            'source_name': source_name,
            'last_backup_time': timestamp,
            'files_backed_up': 0,
            'files_skipped': 0,
            'bytes_transferred': 0,
            'backup_duration_seconds': 0,
            'note': 'Test metadata - simulating backup from 3 days ago'
        }
        
        print(f"Uploading metadata for: {source_name}")
        print(f"  S3 Key: s3://{BUCKET_NAME}/{metadata_key}")
        
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2).encode('utf-8'),
                ContentType='application/json',
                Metadata={
                    'source': 'onedrive-backup-test',
                    'type': 'backup-metadata'
                }
            )
            print(f"  ✅ Successfully uploaded\n")
        except Exception as e:
            print(f"  ❌ Error: {e}\n")

if __name__ == "__main__":
    print("=" * 70)
    print("INCREMENTAL BACKUP TEST - Setting Last Backup Timestamp")
    print("=" * 70)
    print()
    
    upload_fake_metadata()
    
    print("=" * 70)
    print("Next backup will use incremental mode!")
    print("Run: python -m onedrive_backup.cli backup")
    print("=" * 70)
