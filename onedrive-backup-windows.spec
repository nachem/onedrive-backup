# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec file for windows

import sys
from pathlib import Path

import _ssl  # noqa: F401 - ensure PyInstaller sees native extension
from PyInstaller.utils.hooks import collect_submodules

sys.path.insert(0, 'src')

block_cipher = None

ssl_binary = Path(_ssl.__file__)

base_hiddenimports = [
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
    'hashlib',
    'ssl',
    'urllib3.util.ssl_',
    'certifi',
    'urllib3.contrib.pyopenssl',
    'urllib3.contrib.socks'
]

base_hiddenimports += collect_submodules('urllib3')
base_hiddenimports += collect_submodules('botocore')
base_hiddenimports += collect_submodules('boto3')

a = Analysis(
    ['run_cli.py'],
    pathex=['src'],
    binaries=[
        (str(ssl_binary), '.'),
    ],
    datas=[
        ('src/onedrive_backup', 'onedrive_backup'),
        ('config/config.yaml', 'config'),
        ('config/credentials.yaml.template', 'config'),
        ('README.md', '.'),
    ],
    hiddenimports=base_hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=['pyinstaller_hooks/runtime_ssl.py'],
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
    name='onedrive-backup.exe',
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
