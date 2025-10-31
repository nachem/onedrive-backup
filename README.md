# OneDrive/SharePoint Backup Tool

A comprehensive Python application for backing up OneDrive and SharePoint files to cloud storage (AWS S3 and Azure Blob Storage) with intelligent change detection.

## Features

✅ **Multi-source support**: Personal OneDrive, OneDrive for Business, and SharePoint sites  
✅ **Dual cloud support**: AWS S3 and Azure Blob Storage destinations  
✅ **Smart change detection**: Timestamp-based, hash-based, or both  
✅ **Incremental backups**: Only upload changed/new files  
✅ **Flexible scheduling**: Manual execution or cron-style scheduling  
✅ **Robust error handling**: Retry logic and comprehensive logging  
✅ **Rich CLI interface**: Beautiful console output with progress tracking  
✅ **Configurable**: YAML-based configuration with validation  

## Quick Start

### 1. Installation

```bash
# Clone or download the project
cd onedrive-backup

# Install dependencies
pip install -r src/requirements.txt
```

### 2. Configuration

```bash
# Initialize configuration
python -m onedrive_backup.cli init

# Copy and edit credentials template
cp config/credentials.yaml.template config/credentials.yaml
# Edit credentials.yaml with your authentication details
```

### 3. Setup Authentication

#### Microsoft Graph (OneDrive/SharePoint)
1. Register an application in [Azure Portal](https://portal.azure.com)
2. Add these API permissions:
   - `Files.Read.All` (for OneDrive)
   - `Sites.Read.All` (for SharePoint)
   - `User.Read`
3. Copy the Application ID, Secret, and Tenant ID to `credentials.yaml`

#### AWS S3
- Use AWS CLI: `aws configure`
- Or set environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- Or use IAM roles (recommended for EC2/Lambda)

#### Azure Blob Storage
- Get connection string from Azure Portal
- Or use managed identity (recommended for Azure VMs)

### 4. Run Backup

```bash
# Test connections
python -m onedrive_backup.cli test

# Run all enabled backup jobs
python -m onedrive_backup.cli backup

# Run specific job
python -m onedrive_backup.cli backup --job "daily_personal_backup"

# Dry run (see what would be backed up)
python -m onedrive_backup.cli backup --dry-run
```

## Configuration

### Main Configuration (`config/config.yaml`)

```yaml
sources:
  - type: onedrive_personal
    name: "My Personal OneDrive"
    folders: "all"  # or ["Documents", "Pictures"]
  
  - type: sharepoint
    name: "Company SharePoint"
    site_url: "https://company.sharepoint.com/sites/team"
    libraries: ["Documents", "Shared Documents"]

destinations:
  - type: aws_s3
    name: "aws_backup"
    bucket: "my-backup-bucket"
    region: "us-east-1"
    prefix: "onedrive/"

backup_jobs:
  - name: "daily_backup"
    sources: ["My Personal OneDrive"]
    destination: "aws_backup"
    schedule: "0 2 * * *"  # Daily at 2 AM
    change_detection: "timestamp"
    enabled: true
```

### Source Types

- **`onedrive_personal`**: Personal OneDrive account
- **`onedrive_business`**: OneDrive for Business account
- **`sharepoint`**: SharePoint site document libraries

### Destination Types

- **`aws_s3`**: Amazon S3 bucket
- **`azure_blob`**: Azure Blob Storage container

### Change Detection Methods

- **`timestamp`**: Compare file modification times (fastest)
- **`hash`**: Compare file content hashes (most accurate)
- **`both`**: Use timestamp first, then hash for verification

## CLI Commands

### `backup`
Run backup jobs with various options:
```bash
python -m onedrive_backup.cli backup [OPTIONS]

Options:
  -c, --config PATH      Configuration file path
  --credentials PATH     Credentials file path  
  -j, --job TEXT         Run specific job by name
  -d, --dry-run          Show what would be backed up
```

### `test`
Test all configured connections:
```bash
python -m onedrive_backup.cli test
```

### `status`
Show configuration and job status:
```bash
python -m onedrive_backup.cli status
```

### `init`
Create initial configuration:
```bash
python -m onedrive_backup.cli init
```

## Scheduling

### Manual Execution
Remove the `schedule` field from backup jobs or set `enabled: false`.

### Cron Scheduling (Linux/macOS)
```bash
# Edit crontab
crontab -e

# Add daily backup at 2 AM
0 2 * * * cd /path/to/onedrive-backup && python -m onedrive_backup.cli backup
```

### Windows Task Scheduler
1. Open Task Scheduler
2. Create Basic Task
3. Set trigger (daily, weekly, etc.)
4. Set action: `python -m onedrive_backup.cli backup`
5. Set start directory: `/path/to/onedrive-backup`

### Systemd Service (Linux)
Create `/etc/systemd/system/onedrive-backup.service`:
```ini
[Unit]
Description=OneDrive Backup Service
After=network.target

[Service]
Type=oneshot
User=your-user
WorkingDirectory=/path/to/onedrive-backup
ExecStart=/usr/bin/python -m onedrive_backup.cli backup
Environment=PYTHONPATH=/path/to/onedrive-backup/src

[Install]
WantedBy=multi-user.target
```

Then create a timer `/etc/systemd/system/onedrive-backup.timer`:
```ini
[Unit]
Description=Run OneDrive Backup Daily
Requires=onedrive-backup.service

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

Enable and start:
```bash
sudo systemctl enable onedrive-backup.timer
sudo systemctl start onedrive-backup.timer
```

## Security

- **Credentials**: Store in `credentials.yaml` (not in version control) or environment variables
- **Token caching**: Microsoft Graph tokens are cached in `~/.onedrive_backup/`
- **Encryption**: Optional client-side encryption before upload
- **Permissions**: Use least-privilege access (read-only for sources)

## Troubleshooting

### Authentication Issues
```bash
# Clear cached tokens
rm -rf ~/.onedrive_backup/token_cache.json

# Test connections
python -m onedrive_backup.cli test
```

### Permission Errors
- Ensure Azure app has required Graph API permissions
- Check AWS/Azure credentials and bucket/container permissions
- Verify SharePoint site URLs are accessible

### Large Files
- Increase `chunk_size` in sync options for better performance
- Consider using `parallel_uploads` for multiple small files
- Monitor memory usage with very large files

## Advanced Usage

### Environment Variables
Instead of `credentials.yaml`, use environment variables:
```bash
export MICROSOFT_APP_ID="your-app-id"
export MICROSOFT_APP_SECRET="your-app-secret"
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
```

### Custom Folder Structure
Use the `prefix` setting to organize backups:
```yaml
destinations:
  - type: aws_s3
    bucket: "my-backup"
    prefix: "backups/{{ date }}/{{ source_name }}/"
```

### Multiple Tenants
Create separate configuration files for different organizations:
```bash
python -m onedrive_backup.cli backup --config config/company1.yaml
python -m onedrive_backup.cli backup --config config/company2.yaml
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Check the troubleshooting section
- Review logs in the `logs/` directory
- Create an issue on GitHub
