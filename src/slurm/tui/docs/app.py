"""Documentation viewer TUI application."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.widgets import Footer, Header, Input, Markdown, Static, Tree

from ..common.styles import DOCS_CSS
from .loader import DocsLoader
from .search import DocsSearchIndex
from .widgets.nav_tree import DocsNavTree
from .widgets.search_results import SearchResultsList


class DocsApp(App[None]):
    """Interactive TUI documentation viewer."""

    TITLE = "SLURM SDK Documentation"
    CSS = DOCS_CSS

    BINDINGS = [
        Binding("q", "quit", "Quit", priority=True),
        Binding("/", "focus_search", "Search", priority=True),
        Binding("escape", "clear_search", "Clear", priority=True),
        Binding("tab", "focus_next", "Focus Next", show=False),
        Binding("shift+tab", "focus_previous", "Focus Previous", show=False),
    ]

    def __init__(self, initial_search: Optional[str] = None, **kwargs) -> None:
        """Initialize the documentation viewer.

        Args:
            initial_search: Optional initial search query.
            **kwargs: Additional App arguments.
        """
        super().__init__(**kwargs)
        self._initial_search = initial_search
        self._loader = DocsLoader()
        self._search_index: Optional[DocsSearchIndex] = None
        self._current_doc_path: Optional[str] = None
        self._updating_nav: bool = False  # Flag to prevent event loops

    def compose(self) -> ComposeResult:
        """Compose the documentation viewer layout."""
        yield Header()

        with Horizontal(id="main-container"):
            with Container(id="nav-panel", classes="nav-panel"):
                yield Static("Navigation", classes="panel-title")
                yield DocsNavTree(id="nav-tree")

            with Vertical(id="content-panel", classes="content-panel"):
                yield Input(
                    placeholder="Search documentation...",
                    id="search-input",
                )
                yield SearchResultsList(id="search-results")
                with VerticalScroll(id="markdown-scroll"):
                    yield Markdown(id="markdown-viewer")

        yield Footer()

    def on_mount(self) -> None:
        """Handle app mount - load navigation and initial document."""
        # Load documentation
        try:
            self._loader.load()
            nav_tree = self.query_one(DocsNavTree)
            nav_tree.set_navigation(self._loader.get_navigation())

            # Initialize search index (lazy)
            self._search_index = DocsSearchIndex(self._loader)

            # Load initial document
            paths = self._loader.get_all_paths()
            if paths:
                self._open_document(paths[0])

            # Handle initial search
            if self._initial_search:
                search_input = self.query_one("#search-input", Input)
                search_input.value = self._initial_search
                self._do_search(self._initial_search)

        except FileNotFoundError as e:
            self.notify(str(e), severity="error", timeout=5)

        # Focus navigation tree
        self.query_one(DocsNavTree).focus()

    def _open_document(self, path: str, update_nav: bool = True) -> None:
        """Open a document by path.

        Args:
            path: Document path to open.
            update_nav: Whether to update navigation tree selection.
        """
        doc = self._loader.get_document(path)
        if doc is None:
            self.notify(f"Document not found: {path}", severity="error", timeout=3)
            return

        self._current_doc_path = path

        # Update markdown viewer
        viewer = self.query_one("#markdown-viewer", Markdown)
        viewer.update(doc.content)

        # Update title
        self.title = f"SLURM SDK Docs - {doc.display_title}"

        # Sync navigation tree selection
        if update_nav:
            self._updating_nav = True
            nav_tree = self.query_one(DocsNavTree)
            nav_tree.set_current_document(path)
            # Delay resetting flag to allow async events to be ignored
            self.call_later(self._reset_updating_nav)

    def _reset_updating_nav(self) -> None:
        """Reset the nav update flag after events have processed."""
        self._updating_nav = False

    def _do_search(self, query: str) -> None:
        """Execute search.

        Args:
            query: Search query string.
        """
        if not query.strip():
            self._clear_search_results()
            return

        if self._search_index is None:
            return

        results = self._search_index.search(query)
        results_list = self.query_one(SearchResultsList)
        results_list.results = results

        if not results:
            self.notify("No results found", severity="information", timeout=2)

    def _clear_search_results(self) -> None:
        """Clear search results display."""
        results_list = self.query_one(SearchResultsList)
        results_list.clear_results()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle search submission."""
        if event.input.id == "search-input":
            self._do_search(event.value)

    def on_input_changed(self, event: Input.Changed) -> None:
        """Handle search input changes for live search."""
        if event.input.id == "search-input":
            if len(event.value) >= 2:
                self._do_search(event.value)
            elif len(event.value) == 0:
                self._clear_search_results()

    def on_key(self, event) -> None:
        """Handle key events for search navigation."""
        # Check if search input is focused and arrow key pressed
        search_input = self.query_one("#search-input", Input)
        if search_input.has_focus and event.key in ("down", "up"):
            results_list = self.query_one(SearchResultsList)
            if results_list.results:
                results_list.focus()
                event.prevent_default()

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        """Handle navigation tree selection (Enter key)."""
        if not self._updating_nav:
            self._handle_tree_node(event.node, focus_content=True)

    def on_tree_node_highlighted(self, event: Tree.NodeHighlighted) -> None:
        """Handle navigation tree highlighting (click or arrow keys)."""
        if not self._updating_nav:
            self._handle_tree_node(event.node, focus_content=False)

    def _handle_tree_node(self, node, focus_content: bool = False) -> None:
        """Process a tree node selection/highlight."""
        if node and node.data and node.data.path:
            # Don't update nav since we're already navigating from the tree
            self._open_document(node.data.path, update_nav=False)
            self._clear_search_results()
            self._hide_search()
            if focus_content:
                self.query_one("#markdown-scroll", VerticalScroll).focus()

    def on_search_results_list_result_selected(
        self, event: SearchResultsList.ResultSelected
    ) -> None:
        """Handle search result selection."""
        self._open_document(event.path)
        # Clear and hide search after selection
        search_input = self.query_one("#search-input", Input)
        search_input.value = ""
        self._clear_search_results()
        self._hide_search()
        # Focus document content for reading
        self.query_one("#markdown-scroll", VerticalScroll).focus()

    def action_focus_search(self) -> None:
        """Show and focus the search input."""
        self._show_search()
        search_input = self.query_one("#search-input", Input)
        search_input.focus()

    def action_clear_search(self) -> None:
        """Clear search and results, hide search bar."""
        search_input = self.query_one("#search-input", Input)
        search_input.value = ""
        self._clear_search_results()
        self._hide_search()
        self.query_one(DocsNavTree).focus()

    def _show_search(self) -> None:
        """Show the search input."""
        search_input = self.query_one("#search-input", Input)
        search_input.display = True

    def _hide_search(self) -> None:
        """Hide the search input."""
        search_input = self.query_one("#search-input", Input)
        search_input.display = False

    def on_markdown_link_clicked(self, event: Markdown.LinkClicked) -> None:
        """Handle internal link clicks in markdown."""
        href = event.href

        # Handle internal document links
        if not href.startswith(("http://", "https://", "mailto:")):
            if self._current_doc_path:
                current_dir = Path(self._current_doc_path).parent
                resolved = (current_dir / href).as_posix()
                if not resolved.endswith(".md"):
                    resolved += ".md"
                self._open_document(resolved)
                event.prevent_default()
