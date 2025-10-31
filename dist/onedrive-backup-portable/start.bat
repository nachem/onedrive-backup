@echo off
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
