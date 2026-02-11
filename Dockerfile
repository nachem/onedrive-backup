# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy the built application from the dist directory
COPY dist/onedrive-backup-portable-linux/onedrive-backup /app/backup/onedrive_backup
COPY dist/onedrive-backup-portable-linux/config/config.yaml /app/backup/config/config.yaml
RUN chmod +x -R /app

ENTRYPOINT ["/app/backup/onedrive_backup", "backup", "--config", "/app/backup/config/config.yaml"]