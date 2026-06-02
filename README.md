# OneDrive Backup

OneDrive Backup is an open source tool for backing up OneDrive data, with first-class support for AWS deployment and Docker-based builds. It is designed for reliability, automation, and easy integration into cloud or on-premises workflows.

## Features

- Incremental and full backup of OneDrive files
- AWS deployment support (ECS, Lambda, etc.)
- Docker container for easy scheduling and automation
- Flexible configuration via YAML files
- Optional credentials file (uses AWS IAM/environment by default)

## Building the App Locally

You can build a standalone executable using the provided `build_exe.py` script:

```bash
python build_exe.py
```

This will generate a distributable executable in the `build/` directory.

## Building with Docker

A `Dockerfile` is provided for containerized builds. To build the Docker image:

```bash
docker build -t onedrive-backup .
```

To run the container:

```bash
docker run --rm -v $(pwd)/config:/app/config onedrive-backup
```

> **Note:** When the Docker container starts, it will perform the backup/update operation and then quit automatically. This is suitable for scheduled or one-off backup jobs.

## Configuration Files

Configuration files are located in the `config/` directory:

- `config.yaml`: Main configuration for backup sources, destinations, and options.
- `credentials.yaml`: (Optional) Only required for non-AWS authentication. If you are using AWS IAM roles or environment credentials, this file is not needed.
- `credentials.yaml.template`: Example template for manual credential configuration.

## AWS Deployment

For AWS deployment, you can use the provided `aws-deploy.md` for step-by-step instructions. The app supports running as an ECS task, Lambda, or other AWS compute services. When using AWS, credentials can be provided via IAM roles, environment variables, or the optional `credentials.yaml` file.

## Quick Start

1. Clone the repository.
2. Edit `config/config.yaml` to match your backup requirements.
3. (Optional) Edit `config/credentials.yaml` if not using AWS IAM roles.
4. Build and run using Docker or locally as described above.

## Contributing

Contributions are welcome! Please open issues or pull requests for bug fixes, features, or documentation improvements.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

For more details, see the documentation in the `docs/` folder.
