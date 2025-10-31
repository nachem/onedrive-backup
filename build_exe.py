#!/usr/bin/env python3
"""
Build script to create standalone executable for OneDrive Backup Tool.

This script uses PyInstaller to create a single executable file that includes
all dependencies and can be run on any Windows machine without Python installed.
"""

import subprocess
import sys
import os
from pathlib import Path

def install_pyinstaller():
    """Install PyInstaller if not already installed."""
    try:
        import PyInstaller
        print("✅ PyInstaller is already installed")
        return True
    except ImportError:
        print("📦 Installing PyInstaller...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])
            print("✅ PyInstaller installed successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install PyInstaller: {e}")
            return False

def create_spec_file():
    """Create PyInstaller spec file for customized build."""
    spec_content = '''# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(
    ['src/onedrive_backup/cli.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('config/config.yaml', 'config'),
        ('config/credentials.yaml.template', 'config'),
        ('README.md', '.'),
    ],
    hiddenimports=[
        'onedrive_backup',
        'onedrive_backup.auth',
        'onedrive_backup.auth.microsoft_auth',
        'onedrive_backup.auth.cloud_auth',
        'onedrive_backup.config',
        'onedrive_backup.config.settings',
        'onedrive_backup.sources',
        'onedrive_backup.sources.onedrive_operations',
        'onedrive_backup.destinations',
        'onedrive_backup.destinations.azure_blob',
        'onedrive_backup.destinations.aws_s3',
        'onedrive_backup.sync',
        'onedrive_backup.sync.backup_manager',
        'onedrive_backup.sync.file_tracker',
        'onedrive_backup.utils',
        'onedrive_backup.utils.logging',
        'onedrive_backup.utils.file_utils',
        'onedrive_backup.utils.encryption',
        'msal',
        'azure.identity',
        'azure.storage.blob',
        'azure.core.exceptions',
        'boto3',
        'botocore',
        'msgraph',
        'msgraph.core',
        'cryptography',
        'rich',
        'rich.console',
        'rich.table',
        'rich.tree',
        'rich.progress',
        'click',
        'yaml',
        'pydantic',
        'requests',
        'tqdm',
        'apscheduler',
        'asyncio',
        'datetime',
        'json',
        'tempfile',
        'hashlib'
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='onedrive-backup',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)
'''
    
    spec_path = Path("onedrive-backup.spec")
    with open(spec_path, 'w') as f:
        f.write(spec_content)
    
    print(f"✅ Created spec file: {spec_path}")
    return spec_path

def build_executable():
    """Build the executable using PyInstaller."""
    print("\n🔧 Building executable...")
    
    try:
        # Create spec file
        spec_path = create_spec_file()
        
        # Run PyInstaller with the spec file
        cmd = [
            sys.executable, "-m", "PyInstaller",
            "--clean",  # Clean build
            str(spec_path)
        ]
        
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Build completed successfully!")
            
            # Check if executable was created
            exe_path = Path("dist") / "onedrive-backup.exe"
            if exe_path.exists():
                size_mb = exe_path.stat().st_size / (1024 * 1024)
                print(f"📦 Executable created: {exe_path}")
                print(f"📏 File size: {size_mb:.1f} MB")
                return exe_path
            else:
                print("❌ Executable not found in expected location")
                return None
        else:
            print("❌ Build failed!")
            print(f"Error output: {result.stderr}")
            return None
            
    except Exception as e:
        print(f"❌ Build error: {e}")
        return None

def test_executable(exe_path):
    """Test the built executable."""
    if not exe_path or not exe_path.exists():
        return False
    
    print(f"\n🧪 Testing executable: {exe_path}")
    
    try:
        # Test help command
        result = subprocess.run([str(exe_path), "--help"], 
                              capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0 and "OneDrive/SharePoint Backup Tool" in result.stdout:
            print("✅ Executable test passed!")
            print("📋 Available commands:")
            for line in result.stdout.split('\n'):
                if line.strip() and ('backup' in line or 'test' in line or 'status' in line or 'init' in line):
                    print(f"   {line.strip()}")
            return True
        else:
            print("❌ Executable test failed!")
            print(f"Exit code: {result.returncode}")
            print(f"Output: {result.stdout}")
            print(f"Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Executable test timed out")
        return False
    except Exception as e:
        print(f"❌ Test error: {e}")
        return False

def create_distribution_package():
    """Create a distribution package with the executable and config files."""
    print("\n📦 Creating distribution package...")
    
    dist_dir = Path("dist/onedrive-backup-portable")
    dist_dir.mkdir(exist_ok=True)
    
    # Copy executable
    exe_path = Path("dist/onedrive-backup.exe")
    if exe_path.exists():
        import shutil
        shutil.copy2(exe_path, dist_dir / "onedrive-backup.exe")
        print(f"✅ Copied executable to {dist_dir}")
    
    # Copy config files
    config_dir = dist_dir / "config"
    config_dir.mkdir(exist_ok=True)
    
    config_files = [
        ("config/config.yaml", "config.yaml"),
        ("config/credentials.yaml.template", "credentials.yaml.template"),
        ("README.md", "../README.md"),
    ]
    
    for src, dst in config_files:
        src_path = Path(src)
        if src_path.exists():
            import shutil
            dst_path = config_dir / dst if not dst.startswith("..") else dist_dir / dst.replace("../", "")
            dst_path.parent.mkdir(exist_ok=True)
            shutil.copy2(src_path, dst_path)
            print(f"✅ Copied {src} to {dst_path}")
    
    # Create startup batch file
    batch_content = '''@echo off
echo OneDrive/SharePoint Backup Tool
echo ===============================
echo.
echo Available commands:
echo   onedrive-backup --help     Show help
echo   onedrive-backup status     Show configuration status  
echo   onedrive-backup test       Test connections
echo   onedrive-backup backup     Run backup jobs
echo   onedrive-backup init       Initialize configuration
echo.
echo Examples:
echo   onedrive-backup backup --dry-run
echo   onedrive-backup test
echo.
cmd /k
'''
    
    batch_path = dist_dir / "start.bat"
    with open(batch_path, 'w') as f:
        f.write(batch_content)
    print(f"✅ Created startup script: {batch_path}")
    
    print(f"\n🎉 Distribution package ready: {dist_dir}")
    return dist_dir

def main():
    """Main build function."""
    print("🚀 OneDrive Backup Tool - Executable Builder")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not Path("src/onedrive_backup/cli.py").exists():
        print("❌ Please run this script from the project root directory")
        return 1
    
    # Install PyInstaller
    if not install_pyinstaller():
        return 1
    
    # Build executable
    exe_path = build_executable()
    if not exe_path:
        return 1
    
    # Test executable
    if not test_executable(exe_path):
        print("⚠️ Executable built but failed testing")
    
    # Create distribution package
    dist_package = create_distribution_package()
    
    print("\n" + "=" * 50)
    print("✅ Build completed successfully!")
    print("\n📁 Files created:")
    print(f"   • Executable: {exe_path}")
    print(f"   • Distribution: {dist_package}")
    print("\n🚀 To run the executable:")
    print(f"   {exe_path} --help")
    print(f"   {exe_path} test")
    print(f"   {exe_path} backup --dry-run")
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
