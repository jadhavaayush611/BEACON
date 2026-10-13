"""CLI entrypoint to launch the BEACON Streamlit dashboard."""

from pathlib import Path
import subprocess
import sys


def main() -> int:
    """Launch the BEACON Streamlit application."""
    app_path = Path(__file__).parent / "app.py"
    if not app_path.exists():
        print(f"ERROR: Dashboard entrypoint not found at {app_path}", file=sys.stderr)
        return 1

    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--server.headless=true",
        "--browser.gatherUsageStats=false",
    ]

    # Pass remaining arguments to Streamlit
    if len(sys.argv) > 1:
        cmd.extend(sys.argv[1:])

    print(f"Launching BEACON Dashboard from {app_path} ...")
    try:
        return subprocess.call(cmd)
    except KeyboardInterrupt:
        print("\nBEACON Dashboard stopped.")
        return 0
    except Exception as err:
        print(f"ERROR: Failed to start Streamlit: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
