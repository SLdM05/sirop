"""Handler for `sirop switch <name>`."""

from sirop.config.settings import Settings, get_settings
from sirop.db.connection import get_batch_path, set_active_batch
from sirop.models.messages import MessageCode
from sirop.utils.messages import emit


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
        emit(MessageCode.SWITCH_ERROR_BATCH_NOT_FOUND, name=name, path=batch_path)
        return 1

    set_active_batch(name, settings)
    emit(MessageCode.SWITCH_ACTIVATED, name=name)
    return 0
