"""Runtime hook to guarantee the stdlib ssl module is initialized before other dependencies."""

import ssl  # noqa: F401  Ensures PyInstaller bundles ssl/_ssl artifacts
