"""Search results widget for documentation viewer."""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import VerticalScroll
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Static

if TYPE_CHECKING:
    from ..search import SearchResult


class SearchResultItem(Static):
    """A single search result item."""

    DEFAULT_CSS = """
    SearchResultItem {
        width: 100%;
        height: auto;
        padding: 0 1;
        margin: 0 0 1 0;
    }

    SearchResultItem:hover {
        background: $primary-lighten-3;
    }

    SearchResultItem.--selected {
        background: $accent;
    }

    SearchResultItem .result-title {
        text-style: bold;
    }

    SearchResultItem .result-path {
        color: $text-muted;
        text-style: italic;
    }

    SearchResultItem .result-snippet {
        color: $text-muted;
    }
    """

    class Selected(Message):
        """Message when result is selected."""

        def __init__(self, path: str) -> None:
            self.path = path
            super().__init__()

    def __init__(self, result: "SearchResult", **kwargs) -> None:
        """Initialize search result item.

        Args:
            result: SearchResult to display.
            **kwargs: Additional widget arguments.
        """
        self._result = result
        super().__init__(**kwargs)

    def compose(self) -> ComposeResult:
        """Compose the result item."""
        yield Static(self._result.title, classes="result-title")
        yield Static(self._result.path, classes="result-path")
        yield Static(self._result.display_snippet, classes="result-snippet")

    @property
    def path(self) -> str:
        """Get the document path."""
        return self._result.path

    def on_click(self) -> None:
        """Handle click on result."""
        self.post_message(self.Selected(self._result.path))


class SearchResultsList(VerticalScroll, can_focus=True):
    """List of search results."""

    BINDINGS = [
        Binding("enter", "select_result", "Open", show=False),
        Binding("space", "select_result", "Open", show=False),
        Binding("up", "cursor_up", "Up", show=False),
        Binding("down", "cursor_down", "Down", show=False),
    ]

    DEFAULT_CSS = """
    SearchResultsList {
        width: 100%;
        height: auto;
        max-height: 50%;
        border-bottom: tall $primary;
        padding: 1;
        display: none;
    }

    SearchResultsList.has-results {
        display: block;
    }

    SearchResultsList .results-header {
        text-style: bold;
        padding: 0 0 1 0;
    }
    """

    results: reactive[List["SearchResult"]] = reactive([], always_update=True)
    selected_index: reactive[int] = reactive(0)

    class ResultSelected(Message):
        """Message when a result is selected."""

        def __init__(self, path: str) -> None:
            self.path = path
            super().__init__()

    def __init__(self, **kwargs) -> None:
        """Initialize the results list."""
        super().__init__(**kwargs)
        self._result_widgets: List[SearchResultItem] = []

    async def watch_results(self, results: List["SearchResult"]) -> None:
        """Update display when results change."""
        # Clear existing children and wait for completion
        await self.remove_children()
        self._result_widgets = []

        if not results:
            self.remove_class("has-results")
            return

        self.add_class("has-results")

        # Build all widgets first
        widgets_to_mount = [
            Static(f"Search Results ({len(results)})", classes="results-header")
        ]

        # Add result items (no IDs to avoid duplicates on rapid updates)
        for result in results[:20]:  # Limit displayed results
            widget = SearchResultItem(result)
            self._result_widgets.append(widget)
            widgets_to_mount.append(widget)

        # Mount all at once
        await self.mount_all(widgets_to_mount)

        self.selected_index = 0
        self._update_selection()

    def watch_selected_index(self, index: int) -> None:
        """Update selection highlight."""
        self._update_selection()

    def _update_selection(self) -> None:
        """Update which result is highlighted."""
        for i, widget in enumerate(self._result_widgets):
            if i == self.selected_index:
                widget.add_class("--selected")
            else:
                widget.remove_class("--selected")

    def select_next(self) -> None:
        """Select next result."""
        if self._result_widgets:
            self.selected_index = (self.selected_index + 1) % len(self._result_widgets)

    def select_previous(self) -> None:
        """Select previous result."""
        if self._result_widgets:
            self.selected_index = (self.selected_index - 1) % len(self._result_widgets)

    def get_selected_path(self) -> Optional[str]:
        """Get path of selected result.

        Returns:
            Document path or None.
        """
        if self._result_widgets and 0 <= self.selected_index < len(
            self._result_widgets
        ):
            return self._result_widgets[self.selected_index].path
        return None

    def activate_selected(self) -> None:
        """Activate (open) the selected result."""
        path = self.get_selected_path()
        if path:
            self.post_message(self.ResultSelected(path))

    def clear_results(self) -> None:
        """Clear search results."""
        self.results = []

    def on_search_result_item_selected(
        self, message: SearchResultItem.Selected
    ) -> None:
        """Handle result item click."""
        self.post_message(self.ResultSelected(message.path))

    def action_select_result(self) -> None:
        """Open the selected result."""
        self.activate_selected()

    def action_cursor_up(self) -> None:
        """Move selection up."""
        self.select_previous()

    def action_cursor_down(self) -> None:
        """Move selection down."""
        self.select_next()
