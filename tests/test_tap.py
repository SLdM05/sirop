"""Tests for the tap command's importer registry.

The bug this file guards against
---------------------------------
After adding a new importer (YAML + class), a developer can forget to wire
it into ``_IMPORTER_REGISTRY`` in ``sirop.cli.tap``.  The symptom is a
successful format-detection followed by:

    error: format detected as 'X' but no importer is implemented for 'x' yet.

These tests catch that in two complementary ways:

1. **Per-source regression pins** — one assertion per implemented importer.
   Adding a new importer requires adding a corresponding pin here; removing
   an importer from the registry immediately fails the matching test.

2. **Registry-to-YAML consistency** — every entry in ``_IMPORTER_REGISTRY``
   must have a matching ``config/importers/<source>.yaml`` and the factory
   must construct a working importer from it.  This catches the reverse
   mistake: registering a source whose YAML was deleted or renamed.
"""

from pathlib import Path

import pytest

from sirop.cli.tap import _IMPORTER_REGISTRY

CONFIG_DIR = Path(__file__).parent.parent / "config" / "importers"

# ---------------------------------------------------------------------------
# Per-source regression pins
# ---------------------------------------------------------------------------


def test_ndax_is_registered() -> None:
    """Regression: ndax must always appear in _IMPORTER_REGISTRY."""
    assert "ndax" in _IMPORTER_REGISTRY


def test_shakepay_is_registered() -> None:
    """Regression: shakepay must always appear in _IMPORTER_REGISTRY.

    Was missing after the initial Shakepay importer implementation, causing
    ``sirop tap`` to print "no importer is implemented for 'shakepay' yet"
    even though the class and YAML both existed.
    """
    assert "shakepay" in _IMPORTER_REGISTRY


# ---------------------------------------------------------------------------
# Registry ↔ YAML consistency
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("source", list(_IMPORTER_REGISTRY))
def test_registry_entry_has_yaml(source: str) -> None:
    """Every registered source must have a matching config YAML."""
    yaml_path = CONFIG_DIR / f"{source}.yaml"
    assert yaml_path.exists(), (
        f"Registered source {source!r} has no YAML at {yaml_path}. "
        "Either add the YAML or remove the registry entry."
    )


@pytest.mark.parametrize("source,factory", list(_IMPORTER_REGISTRY.items()))
def test_registry_factory_loads(source: str, factory: object) -> None:
    """Every registered factory must load its YAML and return an importer."""
    yaml_path = CONFIG_DIR / f"{source}.yaml"
    importer = factory(yaml_path)  # type: ignore[operator]
    assert importer is not None, f"Factory for {source!r} returned None"
