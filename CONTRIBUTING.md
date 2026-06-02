# Contributing

Thanks for your interest in contributing! This document explains how to get
set up and the expectations for contributions.

## Getting Started

1. Fork and clone the repository.
2. Create a virtual environment and install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy the example configuration files and fill in your own values:
   ```bash
   cp config/config.yaml.template config/config.yaml
   cp config/credentials.yaml.template config/credentials.yaml
   ```
   Both `config/config.yaml` and `config/credentials.yaml` are gitignored.

## Making Changes

- Create a feature branch for your work.
- Keep changes focused and well-described.
- Run the tests in `tests/` before opening a pull request.

## Security & Privacy

- **Never commit secrets** (API keys, tokens, connection strings) or
  personal data (real file paths, user emails, account IDs).
- Only commit `*.template` configuration files with placeholder values.
- Runtime data (`data/`), logs, and build artifacts are gitignored — do not
  force-add them.

## Reporting Issues

Use GitHub Issues for bugs and feature requests. For security
vulnerabilities, follow the process in [SECURITY.md](SECURITY.md) instead.
