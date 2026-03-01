"""Shared Rich console singletons for sirop.

All terminal writes — emit(), spinner(), and _RichLogHandler — must go through
these two Console instances.  Spinner and log handler both use *err* so that
Rich's live display can coordinate: when a log message fires during a spinner,
Rich pauses the spinner, prints above it, then resumes.  Using separate Console
instances breaks that coordination and causes messages to appear on the same
line as the spinner.
"""

from rich.console import Console

# stdout — emit() output and fluff categories
out = Console(highlight=False)

# stderr — emit() error/warning, spinner (progress), and _RichLogHandler.
# All three share this instance so Rich's live display stays coherent.
err = Console(stderr=True, highlight=False)
