#!/usr/bin/env python3
"""
Example script demonstrating how to use the OneDrive Backup Tool.

This script shows how to:
1. Set up the configuration
2. Initialize the backup manager
3. Run backup jobs
4. Handle errors and logging

Run this script to see the backup tool in action (dry-run mode by default).
"""

import asyncio
import sys
from pathlib import Path

# Add src to Python path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent / "src"))

from onedrive_backup.config.settings import BackupConfig, CredentialsConfig
from onedrive_backup.sync.backup_manager import BackupManager
from onedrive_backup.utils.logging import setup_logging


async def main():
    """Main example function."""
    print("🚀 OneDrive/SharePoint Backup Tool - Example Run")
    print("=" * 60)
    
    # Setup logging
    log_file = Path("logs") / "example_run.log"
    logger = setup_logging(log_level="INFO", log_file=log_file)
    
    try:
        # Load configuration
        config_path = Path("config/config.yaml")
        credentials_path = Path("config/credentials.yaml")
        
        print(f"📝 Loading configuration from {config_path}")
        
        if not config_path.exists():
            print("❌ Configuration file not found!")
            print("💡 Run the following command to create one:")
            print("   python -m onedrive_backup.cli init")
            return
        
        # Load main configuration
        backup_config = BackupConfig.from_yaml(config_path)
        print(f"✅ Loaded configuration with {len(backup_config.sources)} sources and {len(backup_config.destinations)} destinations")
        
        # Load credentials (may not exist yet)
        if credentials_path.exists():
            creds_config = CredentialsConfig.from_yaml(credentials_path)
            print("✅ Loaded credentials configuration")
        else:
            print("⚠️  Credentials file not found - using environment variables")
            creds_config = CredentialsConfig.from_env()
        
        # Initialize backup manager
        print("\n🔧 Initializing backup manager...")
        backup_manager = BackupManager(backup_config)
        backup_manager.initialize_auth(creds_config)
        
        # Show configuration summary
        print("\n📊 Configuration Summary:")
        print(f"   Sources: {len(backup_config.sources)}")
        for source in backup_config.sources:
            print(f"     • {source.name} ({source.type})")
        
        print(f"   Destinations: {len(backup_config.destinations)}")
        for dest in backup_config.destinations:
            print(f"     • {dest.name} ({dest.type})")
        
        enabled_jobs = backup_config.get_enabled_jobs()
        print(f"   Enabled Jobs: {len(enabled_jobs)}")
        for job in enabled_jobs:
            schedule = job.schedule or "Manual"
            print(f"     • {job.name} -> {job.destination} ({schedule})")
        
        # Test connections (if credentials are available)
        print("\n🔍 Testing connections...")
        try:
            connection_results = backup_manager.test_connections()
            if connection_results:
                print("Connection test results:")
                for service, status in connection_results.items():
                    status_icon = "✅" if status else "❌"
                    print(f"   {status_icon} {service}")
            else:
                print("⚠️  No connections to test (missing credentials)")
        except Exception as e:
            print(f"⚠️  Connection test failed: {e}")
        
        # Run backup jobs (dry run mode)
        print("\n🏃 Running backup jobs (DRY RUN MODE)...")
        print("ℹ️  This is a demonstration - no files will actually be uploaded")
        
        if enabled_jobs:
            for job in enabled_jobs[:1]:  # Run only first job for demo
                print(f"\n▶️  Processing job: {job.name}")
                try:
                    result = backup_manager.run_backup_job(job)
                    
                    print(f"   Status: {result['status']}")
                    print(f"   Duration: {result.get('duration', 0):.2f}s")
                    print(f"   Files processed: {result.get('files_processed', 0)}")
                    print(f"   Files uploaded: {result.get('files_uploaded', 0)}")
                    print(f"   Errors: {len(result.get('errors', []))}")
                    
                    if result.get('errors'):
                        print("   Error details:")
                        for error in result['errors'][:3]:  # Show first 3 errors
                            print(f"     • {error}")
                
                except Exception as e:
                    print(f"   ❌ Job failed: {e}")
        else:
            print("   ℹ️  No enabled jobs found")
        
        print("\n🎉 Example run completed!")
        print("\n📖 Next steps:")
        print("1. Edit config/config.yaml to match your setup")
        print("2. Create config/credentials.yaml with your authentication details")
        print("3. Run: python -m onedrive_backup.cli test")
        print("4. Run: python -m onedrive_backup.cli backup")
        print("5. Schedule regular backups using cron or Windows Task Scheduler")
        
    except Exception as e:
        print(f"❌ Error during example run: {e}")
        logger.exception("Example run failed")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
