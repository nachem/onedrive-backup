"""Command-line interface for the OneDrive backup application."""

import asyncio
import os
import sys
from pathlib import Path

import click
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from .config.settings import BackupConfig, CredentialsConfig
from .sync.backup_manager import BackupManager

# Force UTF-8 encoding for Windows console to handle Unicode characters
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')

console = Console()

@click.group()
@click.version_option(version="1.0.0")
def cli():
    """OneDrive/SharePoint Backup Tool
    
    A comprehensive backup solution for OneDrive and SharePoint files to cloud storage.
    Supports AWS S3 and Azure Blob Storage with intelligent change detection.
    """
    pass

@cli.command()
@click.option('--config', '-c', 
              type=click.Path(exists=True, path_type=Path),
              default=Path('config/config.yaml'),
              help='Path to configuration file')
@click.option('--credentials',
              default='config/credentials.yaml',
              help='Path to credentials file')
@click.option('--job', '-j',
              help='Run specific job by name (default: run all enabled jobs)')
@click.option('--dry-run', '-d',
              is_flag=True,
              help='Show what would be backed up without actually doing it')
def backup(config: Path, credentials: Path, job: str, dry_run: bool):
    """Run backup jobs."""
    try:
        print("🚀 Starting OneDrive/SharePoint Backup")
        # Load configuration
        with console.status("Loading configuration..."):
            backup_config = BackupConfig.from_yaml(config)
            creds_config = CredentialsConfig.read_credentials(backup_config)
        
        console.print(f"✅ Configuration loaded from {config}", style="green")
        
        # Initialize backup manager
        backup_manager = BackupManager(backup_config)
        backup_manager.initialize_auth(creds_config)
        
        if dry_run:
            console.print("🔍 DRY RUN MODE - No files will be uploaded", style="yellow bold")
        
        # Run backup
        asyncio.run(_run_backup_async(backup_manager, job, dry_run))
        
    except Exception as e:
        console.print(f"❌ Error: {e}", style="red bold")
        sys.exit(1)

async def _run_backup_async(backup_manager: BackupManager, job_name: str, dry_run: bool):
    """Run backup asynchronously."""
    if job_name:
        # Run specific job
        job_config = None
        for job in backup_manager.config.backup_jobs:
            if job.name == job_name:
                job_config = job
                break
        
        if not job_config:
            console.print(f"❌ Job '{job_name}' not found", style="red")
            return
        
        console.print(f"🚀 Running backup job: {job_name}")
        results = [backup_manager.run_backup_job(job_config)]
    else:
        # Run all enabled jobs
        enabled_jobs = backup_manager.config.get_enabled_jobs()
        console.print(f"🚀 Running {len(enabled_jobs)} enabled backup jobs")
        results = backup_manager.run_all_jobs()
    
    # Display results
    _display_backup_results(results, backup_manager)

def _display_backup_results(results, backup_manager):
    """Display backup results in a nice table."""
    # Create results table
    table = Table(title="Backup Results")
    table.add_column("Job Name", style="cyan")
    table.add_column("Status", style="magenta")
    table.add_column("Files Processed", justify="right")
    table.add_column("Files Uploaded", justify="right", style="green")
    table.add_column("Files Skipped", justify="right", style="yellow")
    table.add_column("Data Transferred", justify="right")
    table.add_column("Duration", justify="right")
    table.add_column("Errors", justify="right", style="red")
    
    for result in results:
        status_style = "green" if result['status'] == 'completed' else "red"
        duration = f"{result.get('duration', 0):.1f}s"
        data_size = _format_bytes(result.get('bytes_transferred', 0))
        
        table.add_row(
            result['job_name'],
            f"[{status_style}]{result['status']}[/{status_style}]",
            str(result.get('files_processed', 0)),
            str(result.get('files_uploaded', 0)),
            str(result.get('files_skipped', 0)),
            data_size,
            duration,
            str(len(result.get('errors', [])))
        )
    
    console.print(table)
    
    # Display summary
    summary = backup_manager.get_backup_summary(results)
    rprint(f"\n📊 [bold]Summary:[/bold]")
    rprint(f"   • Total jobs: {summary['total_jobs']}")
    rprint(f"   • Successful: [green]{summary['successful_jobs']}[/green]")
    rprint(f"   • Failed: [red]{summary['failed_jobs']}[/red]")
    rprint(f"   • Files processed: {summary['total_files_processed']}")
    rprint(f"   • Files uploaded: [green]{summary['total_files_uploaded']}[/green]")
    rprint(f"   • Data transferred: {_format_bytes(summary['total_bytes_transferred'])}")
    
    # Show errors if any
    if summary['total_errors'] > 0:
        rprint(f"\n⚠️ [yellow]{summary['total_errors']} errors occurred:[/yellow]")
        for result in results:
            for error in result.get('errors', []):
                rprint(f"   • {error}", style="red")

@cli.command()
@click.option('--config', '-c',
              type=click.Path(exists=True, path_type=Path),
              default=Path('config/config.yaml'),
              help='Path to configuration file')
@click.option('--credentials',
              default='config/credentials.yaml',
              help='Path to credentials file')
def test(config: Path, credentials: Path):
    """Test connections to all configured services."""
    try:
        # Load configuration
        backup_config = BackupConfig.from_yaml(config)
        creds_config = CredentialsConfig.read_credentials(backup_config)
        
        # Initialize backup manager
        backup_manager = BackupManager(backup_config)
        backup_manager.initialize_auth(creds_config)
        
        console.print("🔍 Testing connections...\n")
        
        # Test connections
        results = backup_manager.test_connections()
        
        # Display results
        table = Table(title="Connection Test Results")
        table.add_column("Service", style="cyan")
        table.add_column("Status", style="magenta")
        
        for service, status in results.items():
            status_text = "✅ Connected" if status else "❌ Failed"
            status_style = "green" if status else "red"
            table.add_row(service, f"[{status_style}]{status_text}[/{status_style}]")
        
        console.print(table)
        
        # Overall status
        all_connected = all(results.values())
        if all_connected:
            console.print("\n🎉 All connections successful!", style="green bold")
        else:
            console.print("\n⚠️ Some connections failed. Check your configuration.", style="yellow bold")
            sys.exit(1)
        
    except Exception as e:
        console.print(f"❌ Error: {e}", style="red bold")
        sys.exit(1)

@cli.command()
@click.option('--config', '-c',
              type=click.Path(path_type=Path),
              default=Path('config/config.yaml'),
              help='Path to save configuration file')
def init(config: Path):
    """Initialize a new configuration file."""
    if config.exists():
        if not click.confirm(f"Configuration file {config} already exists. Overwrite?"):
            return
    
    console.print("🚀 Creating new configuration file...")
    
    # Create sample configuration
    sample_config = {
        'sources': [
            {
                'type': 'onedrive_personal',
                'name': 'My Personal OneDrive',
                'folders': 'all'
            }
        ],
        'destinations': [
            {
                'type': 'aws_s3',
                'name': 'my_s3_backup',
                'bucket': 'my-backup-bucket',
                'region': 'us-east-1',
                'prefix': 'onedrive-backup/'
            }
        ],
        'backup_jobs': [
            {
                'name': 'daily_personal_backup',
                'sources': ['My Personal OneDrive'],
                'destination': 'my_s3_backup',
                'schedule': '0 2 * * *',
                'change_detection': 'timestamp',
                'enabled': True
            }
        ],
        'sync_options': {
            'retry_attempts': 3,
            'retry_delay': 5,
            'parallel_uploads': 4,
            'encryption': False
        }
    }
    
    # Save configuration
    backup_config = BackupConfig(**sample_config)
    backup_config.to_yaml(config)
    
    console.print(f"✅ Configuration saved to {config}", style="green")
    console.print("\n📝 Next steps:")
    console.print("1. Edit the configuration file to match your setup")
    console.print("2. Create credentials.yaml with your authentication details")
    console.print("3. Run 'onedrive-backup test' to verify connections")
    console.print("4. Run 'onedrive-backup backup' to start backing up")

def _format_bytes(bytes_size: int) -> str:
    """Format bytes as human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f} PB"

if __name__ == '__main__':
    cli()
