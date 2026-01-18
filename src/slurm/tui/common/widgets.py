"""Shared widgets for TUI applications."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Static


class RefreshIndicator(Widget):
    """Widget showing refresh status and last update time."""

    DEFAULT_CSS = """
    RefreshIndicator {
        width: auto;
        height: 1;
        padding: 0 1;
    }

    RefreshIndicator.auto-enabled {
        color: green;
    }

    RefreshIndicator.auto-disabled {
        color: $text-muted;
    }
    """

    auto_refresh: reactive[bool] = reactive(True)
    last_refresh: reactive[str] = reactive("")
    refresh_interval: reactive[int] = reactive(30)

    def render(self) -> str:
        """Render the refresh indicator."""
        if self.auto_refresh:
            status = f"⟳ Auto ({self.refresh_interval}s)"
        else:
            status = "⟳ Manual"

        if self.last_refresh:
            return f"{status} | {self.last_refresh}"
        return status

    def watch_auto_refresh(self, auto_refresh: bool) -> None:
        """Update styling when auto_refresh changes."""
        self.remove_class("auto-enabled", "auto-disabled")
        if auto_refresh:
            self.add_class("auto-enabled")
        else:
            self.add_class("auto-disabled")


class StatusBadge(Static):
    """A colored badge showing job/partition status."""

    DEFAULT_CSS = """
    StatusBadge {
        width: auto;
        padding: 0 1;
    }

    StatusBadge.running {
        color: green;
    }

    StatusBadge.pending {
        color: yellow;
    }

    StatusBadge.completed {
        color: blue;
    }

    StatusBadge.failed {
        color: red;
    }

    StatusBadge.cancelled {
        color: magenta;
    }

    StatusBadge.timeout {
        color: red;
    }

    StatusBadge.up {
        color: green;
    }

    StatusBadge.down {
        color: red;
    }
    """

    def __init__(self, status: str, **kwargs) -> None:
        """Initialize status badge.

        Args:
            status: Status string to display.
            **kwargs: Additional widget arguments.
        """
        super().__init__(status, **kwargs)
        base_status = status.split()[0].lower() if status else ""
        self.add_class(base_status)


class KeyValue(Horizontal):
    """A horizontal key-value display widget."""

    DEFAULT_CSS = """
    KeyValue {
        height: 1;
        width: 100%;
    }

    KeyValue .label {
        text-style: bold;
        width: 15;
    }

    KeyValue .value {
        width: 1fr;
    }
    """

    def __init__(self, label: str, value: str, **kwargs) -> None:
        """Initialize key-value widget.

        Args:
            label: The label/key text.
            value: The value text.
            **kwargs: Additional widget arguments.
        """
        self._label = label
        self._value = value
        super().__init__(**kwargs)

    def compose(self) -> ComposeResult:
        """Compose the key-value layout."""
        yield Static(f"{self._label}:", classes="label")
        yield Static(self._value, classes="value")


class SectionHeader(Static):
    """A section header with styling."""

    DEFAULT_CSS = """
    SectionHeader {
        text-style: bold;
        color: $text;
        background: $primary-darken-1;
        padding: 0 1;
        width: 100%;
        height: 1;
        margin: 1 0 0 0;
    }
    """

    def __init__(self, title: str, **kwargs) -> None:
        """Initialize section header.

        Args:
            title: Section title text.
            **kwargs: Additional widget arguments.
        """
        super().__init__(title, **kwargs)


class EmptyState(Static):
    """Widget shown when there's no data to display."""

    DEFAULT_CSS = """
    EmptyState {
        width: 100%;
        height: auto;
        padding: 2;
        text-align: center;
        color: $text-muted;
        text-style: italic;
    }
    """

    def __init__(self, message: str = "No data available", **kwargs) -> None:
        """Initialize empty state widget.

        Args:
            message: Message to display.
            **kwargs: Additional widget arguments.
        """
        super().__init__(message, **kwargs)
