from pathlib import Path

import pytest

try:
    import sirop  # noqa: F401
except ImportError as e:
    raise RuntimeError("sirop package not found — run 'poetry install' first") from e

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR
