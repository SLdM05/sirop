"""Handler for `sirop switch <name>`."""

from sirop.config.settings import Settings, get_settings
from sirop.db.connection import get_batch_path, set_active_batch


def handle_switch(name: str, settings: Settings | None = None) -> int:
    """Set the active batch.

    Parameters
    ----------
    name:
        Batch name to activate (without extension).
    settings:
        Application settings. Resolved from the environment if not supplied.

    Returns
    -------
    int
        Exit code: 0 on success, 1 if the batch file does not exist.
    """
    if settings is None:
        settings = get_settings()

    batch_path = get_batch_path(name, settings)
    if not batch_path.exists():
        print(f"error: batch '{name}' not found at {batch_path}")
        return 1

    set_active_batch(name, settings)
    print(f"Active batch: {name}")
    return 0
