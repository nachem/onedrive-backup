#!/usr/bin/env python3
"""
Installation script for OneDrive Backup Tool.

This script will:
1. Install required dependencies
2. Set up directory structure
3. Create sample configuration files
4. Verify installation
"""

import subprocess
import sys
from pathlib import Path

def run_command(command, description):
    """Run a command and handle errors."""
    print(f"🔧 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        if e.stdout:
            print(f"   stdout: {e.stdout}")
        if e.stderr:
            print(f"   stderr: {e.stderr}")
        return False

def main():
    """Main installation function."""
    print("🚀 OneDrive Backup Tool - Installation")
    print("=" * 50)
    
    # Check Python version
    python_version = sys.version_info
    if python_version < (3, 9):
        print(f"❌ Python 3.9+ required, found {python_version.major}.{python_version.minor}")
        return 1
    
    print(f"✅ Python {python_version.major}.{python_version.minor}.{python_version.micro} detected")
    
    # Create directories
    print("\n📁 Setting up directory structure...")
    directories = [
        Path("logs"),
        Path("config"),
        Path("data"),
    ]
    
    for directory in directories:
        directory.mkdir(exist_ok=True)
        print(f"   ✅ Created {directory}")
    
    # Install dependencies
    print("\n📦 Installing dependencies...")
    requirements_file = Path("src/requirements.txt")
    
    if requirements_file.exists():
        if not run_command(f"pip install -r {requirements_file}", "Installing Python packages"):
            print("⚠️  Some packages may not have installed correctly")
            print("   You may need to install them manually")
    else:
        print("❌ requirements.txt not found")
        return 1
    
    # Install package in development mode
    print("\n🔧 Installing package in development mode...")
    if not run_command("pip install -e .", "Installing OneDrive Backup Tool"):
        print("⚠️  Package installation failed")
        print("   You can still run the tool using python -m onedrive_backup.cli")
    
    # Create configuration files if they don't exist
    print("\n⚙️ Setting up configuration...")
    
    config_file = Path("config/config.yaml")
    if not config_file.exists():
        if run_command("python -m onedrive_backup.cli init", "Creating initial configuration"):
            print(f"   ✅ Created {config_file}")
        else:
            print(f"   ⚠️  Could not create {config_file}")
    else:
        print(f"   ℹ️  {config_file} already exists")
    
    credentials_template = Path("config/credentials.yaml.template")
    credentials_file = Path("config/credentials.yaml")
    
    if credentials_template.exists() and not credentials_file.exists():
        print(f"   📋 Please copy {credentials_template} to {credentials_file}")
        print(f"      and fill in your authentication details")
    
    # Test installation
    print("\n🧪 Testing installation...")
    test_commands = [
        ("python -m onedrive_backup.cli --help", "CLI help command"),
        ("python -c \"import onedrive_backup; print('✅ Package import successful')\"", "Package import"),
    ]
    
    all_tests_passed = True
    for command, description in test_commands:
        if not run_command(command, f"Testing {description}"):
            all_tests_passed = False
    
    # Show final status
    print("\n" + "=" * 50)
    if all_tests_passed:
        print("🎉 Installation completed successfully!")
        print("\n📖 Next steps:")
        print("1. Edit config/config.yaml to configure your backup sources and destinations")
        print("2. Copy config/credentials.yaml.template to config/credentials.yaml")
        print("3. Fill in your authentication credentials")
        print("4. Run: onedrive-backup test")
        print("5. Run: onedrive-backup backup")
        print("\n📚 Documentation: README.md")
        print("🚀 Example: python run_example.py")
    else:
        print("⚠️  Installation completed with warnings")
        print("   Some components may not work correctly")
        print("   Check the error messages above")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
