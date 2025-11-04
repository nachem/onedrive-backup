#!/usr/bin/env python3
"""
Build script to create standalone executable for OneDrive Backup Tool.

This script uses PyInstaller to create a single executable file that includes
all dependencies and can be run on Windows or Linux machines without Python installed.

Usage:
    python build_exe.py              # Build for current platform
    python build_exe.py --windows    # Build for Windows
    python build_exe.py --ubuntu     # Build for Ubuntu/Linux
    python build_exe.py --all        # Build for all platforms
"""

import argparse
import os
import platform
import subprocess
import sys
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

def get_platform_info():
    """Determine the current platform and supported targets."""
    current_os = platform.system().lower()
    return {
        'current': current_os,
        'is_windows': current_os == 'windows',
        'is_linux': current_os == 'linux',
        'is_macos': current_os == 'darwin'
    }

def create_spec_file(target_platform='auto'):
    """Create PyInstaller spec file for customized build.
    
    Args:
        target_platform: 'windows', 'linux', or 'auto' to detect current platform
    """
    platform_info = get_platform_info()
    
    if target_platform == 'auto':
        if platform_info['is_windows']:
            target_platform = 'windows'
        elif platform_info['is_linux']:
            target_platform = 'linux'
        else:
            target_platform = 'linux'  # Default to linux for macOS or other
    
    exe_name = 'onedrive-backup.exe' if target_platform == 'windows' else 'onedrive-backup'
    
    spec_content = f"""# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec file for {target_platform}

import sys
sys.path.insert(0, 'src')

block_cipher = None

a = Analysis(
    ['run_cli.py'],
    pathex=['src'],
    binaries=[],
    datas=[
        ('src/onedrive_backup', 'onedrive_backup'),
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
    hooksconfig={{}},
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
    name='{exe_name}',
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
"""
    
    spec_filename = f"onedrive-backup-{target_platform}.spec"
    spec_path = Path(spec_filename)
    with open(spec_path, 'w') as f:
        f.write(spec_content)
    
    print(f"✅ Created spec file for {target_platform}: {spec_path}")
    return spec_path, target_platform

def build_executable(target_platform='auto'):
    """Build the executable using PyInstaller.
    
    Args:
        target_platform: 'windows', 'linux', or 'auto' to detect current platform
    """
    platform_info = get_platform_info()
    
    if target_platform == 'auto':
        if platform_info['is_windows']:
            target_platform = 'windows'
        elif platform_info['is_linux']:
            target_platform = 'linux'
        else:
            target_platform = 'linux'
    
    # Validate platform compatibility
    if target_platform == 'windows' and not platform_info['is_windows']:
        print("⚠️  Cross-compilation to Windows from non-Windows platform is not fully supported")
        print("    The executable may not work correctly. Consider building on Windows.")
    elif target_platform == 'linux' and platform_info['is_windows']:
        print("⚠️  Cross-compilation to Linux from Windows is not fully supported")
        print("    The executable may not work correctly. Consider building on Linux/Ubuntu.")
    
    print(f"\n� Building executable for {target_platform}...")
    
    try:
        # Create spec file
        spec_path, platform_target = create_spec_file(target_platform)
        
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
            exe_name = "onedrive-backup.exe" if platform_target == 'windows' else "onedrive-backup"
            exe_path = Path("dist") / exe_name
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

def create_distribution_package(target_platform='auto'):
    """Create a distribution package with the executable and config files.
    
    Args:
        target_platform: 'windows', 'linux', or 'auto' to detect current platform
    """
    platform_info = get_platform_info()
    
    if target_platform == 'auto':
        if platform_info['is_windows']:
            target_platform = 'windows'
        elif platform_info['is_linux']:
            target_platform = 'linux'
        else:
            target_platform = 'linux'
    
    print(f"\n📦 Creating distribution package for {target_platform}...")
    
    dist_dir = Path(f"dist/onedrive-backup-portable-{target_platform}")
    dist_dir.mkdir(exist_ok=True)
    
    # Copy executable
    exe_name = "onedrive-backup.exe" if target_platform == 'windows' else "onedrive-backup"
    exe_path = Path("dist") / exe_name
    if exe_path.exists():
        import shutil
        dest_exe = dist_dir / exe_name
        shutil.copy2(exe_path, dest_exe)
        
        # Make executable on Linux
        if target_platform == 'linux':
            os.chmod(dest_exe, 0o755)
        
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
    
    # Create startup script (platform-specific)
    if target_platform == 'windows':
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
        script_path = dist_dir / "start.bat"
        with open(script_path, 'w') as f:
            f.write(batch_content)
    else:  # Linux
        script_content = '''#!/bin/bash
echo "OneDrive/SharePoint Backup Tool"
echo "==============================="
echo ""
echo "Available commands:"
echo "  ./onedrive-backup --help     Show help"
echo "  ./onedrive-backup status     Show configuration status"
echo "  ./onedrive-backup test       Test connections"
echo "  ./onedrive-backup backup     Run backup jobs"
echo "  ./onedrive-backup init       Initialize configuration"
echo ""
echo "Examples:"
echo "  ./onedrive-backup backup --dry-run"
echo "  ./onedrive-backup test"
echo ""
exec bash
'''
        script_path = dist_dir / "start.sh"
        with open(script_path, 'w') as f:
            f.write(script_content)
        os.chmod(script_path, 0o755)
    
    print(f"✅ Created startup script: {script_path}")
    
    print(f"\n🎉 Distribution package ready: {dist_dir}")
    return dist_dir

def build_for_platform(target_platform):
    """Build executable for a specific platform."""
    print(f"\n{'=' * 60}")
    print(f"Building for: {target_platform.upper()}")
    print(f"{'=' * 60}")
    
    # Build executable
    exe_path = build_executable(target_platform)
    if not exe_path:
        return False
    
    # Test executable (only if building for current platform)
    platform_info = get_platform_info()
    can_test = (target_platform == 'windows' and platform_info['is_windows']) or \
               (target_platform == 'linux' and platform_info['is_linux'])
    
    if can_test:
        if not test_executable(exe_path):
            print("⚠️ Executable built but failed testing")
    else:
        print(f"ℹ️  Skipping test (cross-compiled for {target_platform})")
    
    # Create distribution package
    dist_package = create_distribution_package(target_platform)
    
    print(f"\n✅ Build for {target_platform} completed!")
    print(f"📁 Files created:")
    print(f"   • Executable: {exe_path}")
    print(f"   • Distribution: {dist_package}")
    
    if target_platform == 'linux':
        print(f"\n🚀 To run on Ubuntu/Linux:")
        print(f"   chmod +x {exe_path}")
        print(f"   {exe_path} --help")
    else:
        print(f"\n🚀 To run on Windows:")
        print(f"   {exe_path} --help")
    
    return True

def main():
    """Main build function."""
    parser = argparse.ArgumentParser(
        description='Build OneDrive Backup Tool executable',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python build_exe.py              # Build for current platform
  python build_exe.py --windows    # Build for Windows
  python build_exe.py --ubuntu     # Build for Ubuntu/Linux
  python build_exe.py --all        # Build for all platforms
        '''
    )
    parser.add_argument('--windows', action='store_true', 
                       help='Build for Windows')
    parser.add_argument('--ubuntu', '--linux', action='store_true', dest='ubuntu',
                       help='Build for Ubuntu/Linux')
    parser.add_argument('--all', action='store_true',
                       help='Build for all platforms')
    
    args = parser.parse_args()
    
    print("🚀 OneDrive Backup Tool - Executable Builder")
    print("=" * 60)
    
    platform_info = get_platform_info()
    print(f"Current platform: {platform_info['current']}")
    print("=" * 60)
    
    # Check if we're in the right directory
    if not Path("src/onedrive_backup/cli.py").exists():
        print("❌ Please run this script from the project root directory")
        return 1
    
    # Install PyInstaller
    if not install_pyinstaller():
        return 1
    
    # Determine which platforms to build
    platforms_to_build = []
    
    if args.all:
        platforms_to_build = ['windows', 'linux']
    elif args.windows:
        platforms_to_build = ['windows']
    elif args.ubuntu:
        platforms_to_build = ['linux']
    else:
        # Default to current platform
        if platform_info['is_windows']:
            platforms_to_build = ['windows']
        elif platform_info['is_linux']:
            platforms_to_build = ['linux']
        else:
            platforms_to_build = ['linux']
    
    # Build for each platform
    success = True
    for platform_target in platforms_to_build:
        if not build_for_platform(platform_target):
            success = False
    
    print("\n" + "=" * 60)
    if success:
        print("✅ All builds completed successfully!")
    else:
        print("⚠️ Some builds failed. Check the output above.")
    print("=" * 60)
    
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
