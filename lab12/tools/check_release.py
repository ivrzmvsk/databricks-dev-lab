"""Offline release check; never grants approval or contacts a workspace."""

from pathlib import Path

from lab12.release import require_release

if __name__ == "__main__":
    print(f"Reviewed DEV release: {require_release(Path(__file__).resolve().parents[1])}")
