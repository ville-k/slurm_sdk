"""Shared helpers for terminal UI feedback using Rich."""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from typing import Iterator, Optional, Tuple

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)


@contextmanager
def status(console: Optional[Console], message: str) -> Iterator[None]:
    """Render a transient spinner when a console is available."""

    if console is None:
        with nullcontext():
            yield
        return

    with console.status(message, spinner="dots", spinner_style="cyan"):
        yield


@contextmanager
def progress_task(
    console: Optional[Console],
    description: str,
    *,
    total: Optional[int] = None,
    transient: bool = True,
) -> Iterator[Tuple[Optional[Progress], Optional[int]]]:
    """Yield a Rich progress instance when a console is provided."""

    if console is None:
        with nullcontext():
            yield None, None
        return

    progress = Progress(
        SpinnerColumn(spinner_name="dots"),
        TextColumn("{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        transient=transient,
        console=console,
    )
    task_id = progress.add_task(description, total=total)
    with progress:
        yield progress, task_id
