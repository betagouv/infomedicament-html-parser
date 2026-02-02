"""Pytest configuration and fixtures."""

from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_notice_html() -> str:
    """Load sample Notice HTML file."""
    sample_file = FIXTURES_DIR / "N0314839.htm"
    return sample_file.read_bytes().decode("utf-8", errors="replace")
