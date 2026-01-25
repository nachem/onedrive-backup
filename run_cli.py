#!/usr/bin/env python3
"""
Entry point wrapper for OneDrive Backup CLI.
This script ensures the package is properly imported before running the CLI.
"""
import ssl  # noqa: F401 - force PyInstaller to include stdlib ssl/_ssl artifacts
import sys
from pathlib import Path

# Add src directory to path
src_path = Path(__file__).parent / 'src'
if src_path.exists():
    sys.path.insert(0, str(src_path))

# Import and run the CLI
from onedrive_backup.cli import cli

if __name__ == '__main__':
    cli()
