"""Handler for `sirop list`."""

from sirop.config.settings import Settings, get_settings
from sirop.db.connection import get_active_batch_name
from sirop.models.messages import MessageCode
from sirop.utils.messages import emit


def handle_list(settings: Settings | None = None) -> int:
    """List all .sirop batch files in DATA_DIR, marking the active one.

    Returns
    -------
    int
        Exit code: always 0.
    """
    if settings is None:
        settings = get_settings()

    batches = sorted(settings.data_dir.glob("*.sirop"), key=lambda p: p.stem)
    active = get_active_batch_name(settings)

    if not batches:
        emit(MessageCode.LIST_NO_BATCHES, data_dir=settings.data_dir)
        return 0

    for path in batches:
        name = path.stem
        marker = " *" if name == active else ""
        emit(MessageCode.LIST_BATCH_ITEM, name=name, marker=marker)

    return 0
