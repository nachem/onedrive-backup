"""Setup script for OneDrive Backup Tool."""

from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
readme_path = Path(__file__).parent / "README.md"
long_description = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""

# Read requirements
requirements_path = Path(__file__).parent / "src" / "requirements.txt"
requirements = []
if requirements_path.exists():
    with open(requirements_path, 'r', encoding='utf-8') as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name="onedrive-backup",
    version="1.0.0",
    author="OneDrive Backup Tool",
    description="Backup OneDrive and SharePoint files to cloud storage",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/onedrive-backup",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: End Users/Desktop",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: System :: Archiving :: Backup",
        "Topic :: Internet :: File Transfer Protocol (FTP)",
        "Topic :: Office/Business",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.5.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "onedrive-backup=onedrive_backup.cli:cli",
        ],
    },
    include_package_data=True,
    package_data={
        "onedrive_backup": ["py.typed"],
    },
    zip_safe=False,
    keywords="onedrive sharepoint backup cloud storage aws azure s3 blob",
    project_urls={
        "Bug Reports": "https://github.com/yourusername/onedrive-backup/issues",
        "Source": "https://github.com/yourusername/onedrive-backup",
        "Documentation": "https://github.com/yourusername/onedrive-backup/blob/main/README.md",
    },
)
