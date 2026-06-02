#!/usr/bin/env python3
"""
Cross-platform local runner for OneDrive Backup.

Convenience wrapper around the backup CLI. By default it runs a fast DRY-RUN
check (no files are uploaded) so you can verify configuration, credentials, and
incremental state quickly. Works on Linux, macOS, and Windows (servers too).

Real settings live in (gitignored, never committed):
  - config/config.yaml          sources, destinations, jobs, secret provider
  - config/credentials.yaml     only when not using a cloud secret provider
  - data/file_tracker.json      incremental backup state

Examples:
  python run_local.py                       # fast dry-run check, all jobs
  python run_local.py --job onedrive_backup # dry-run check, single job
  python run_local.py --upload              # real backup (uploads files)
  python run_local.py --test                # verify connectivity / credentials

Run with the project's Python environment that has the dependencies installed
(e.g. the "bernoulli-backup" conda env, or a venv created from requirements.txt).
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# Make the package under src/ importable.
SRC = ROOT / "src"
if SRC.exists():
    sys.path.insert(0, str(SRC))
sys.path.insert(0, str(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Local runner for OneDrive Backup (defaults to a safe dry-run).",
    )
    parser.add_argument(
        "--job",
        help="Name of a specific job to run (matches a job in config/config.yaml). "
        "Omit to run all enabled jobs.",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Perform a REAL run (actually upload). Without this flag a safe dry-run is done.",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test connections to all configured services instead of running a backup.",
    )
    args = parser.parse_args()

    config_path = ROOT / "config" / "config.yaml"
    if not config_path.exists():
        print(
            "Missing config/config.yaml. Copy config/config.yaml.template to "
            "config/config.yaml and fill in your values.",
            file=sys.stderr,
        )
        return 1

    # Build CLI args.
    if args.test:
        cli_args = ["test"]
    else:
        cli_args = ["backup"]
        if args.job:
            cli_args += ["--job", args.job]
        if not args.upload:
            cli_args += ["--dry-run"]
        if args.upload:
            print("WARNING: REAL upload run (files WILL be uploaded).")
        else:
            print("Fast dry-run check (no uploads).")

    # Import here so the path setup above is in effect, and so a missing
    # dependency produces a clear message about the environment.
    try:
        from onedrive_backup.cli import cli
    except ModuleNotFoundError as exc:
        print(
            f"Missing dependency: {exc.name}. Install requirements into the active "
            "Python environment, e.g.:\n"
            "  pip install -r requirements.txt",
            file=sys.stderr,
        )
        return 1

    # Click commands raise SystemExit; translate to a return code.
    try:
        cli(args=cli_args, standalone_mode=False)
    except SystemExit as exc:
        return int(exc.code or 0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
