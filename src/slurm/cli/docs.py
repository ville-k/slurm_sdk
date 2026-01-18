"""Documentation viewer command for browsing SDK docs in the terminal."""

from __future__ import annotations

from typing import Annotated, Optional

import cyclopts
from rich.console import Console

console = Console(stderr=True)

docs_app = cyclopts.App(
    name="docs",
    help="Browse SDK documentation in an interactive TUI.",
)


@docs_app.default
def docs(
    search: Annotated[
        Optional[str],
        cyclopts.Parameter(
            help="Search query to find in documentation.",
        ),
    ] = None,
    *,
    query: Annotated[
        Optional[str],
        cyclopts.Parameter(
            name="query",
            help="Positional search query (alternative to --search).",
            show=False,
        ),
    ] = None,
) -> None:
    """Launch the documentation viewer TUI.

    Browse the SDK documentation with full-text search support.
    The documentation is bundled with the package for offline access.

    Navigation:
    - Arrow keys to navigate the doc tree
    - Enter to open a document
    - '/' to search
    - 'n'/'N' to jump to next/previous search result
    - 'q' to quit

    Examples:
        slurm docs              # Open docs browser
        slurm docs workflow     # Open with search for "workflow"
        slurm docs --search "task decorator"
    """
    try:
        from ..tui import check_textual_available

        check_textual_available()
    except ImportError as e:
        console.print(f"[red]Error:[/red] {e}")
        console.print(
            "\n[dim]Install TUI support with: pip install slurm-sdk[tui][/dim]"
        )
        raise SystemExit(1)

    from ..tui.docs.app import DocsApp

    # Use positional query if provided, otherwise use --search
    search_query = query or search
    app = DocsApp(initial_search=search_query)
    app.run()
