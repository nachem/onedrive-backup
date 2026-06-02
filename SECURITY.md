# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it
privately. Do **not** open a public issue for security problems.

- Email the maintainers, or use GitHub's "Report a vulnerability" feature
  under the **Security** tab of the repository.
- Include a description of the issue, steps to reproduce, and any relevant
  logs or proof-of-concept (with secrets redacted).

We will acknowledge your report as soon as possible and work with you on a
fix and coordinated disclosure.

## Handling Secrets

This project never stores credentials in source control:

- Real credentials belong in `config/credentials.yaml` (gitignored) or in
  environment variables / a secrets manager (AWS Secrets Manager).
- Only `*.template` files are committed as examples.
- Runtime data (`data/`), logs (`logs/`, `*.log`), and build output
  (`build/`, `dist/`) are gitignored because they may contain private
  file paths or other sensitive information.

Before committing, verify you are not adding secrets or personal data.
