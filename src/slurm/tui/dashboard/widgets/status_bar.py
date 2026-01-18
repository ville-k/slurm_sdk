"""Status bar widget for the dashboard footer."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from textual.reactive import reactive
from textual.widgets import Static


class StatusBar(Static):
    """Footer status bar showing refresh info and keyboard shortcuts."""

    DEFAULT_CSS = """
    StatusBar {
        width: 100%;
        height: 1;
        background: $primary-darken-2;
        color: $text;
        padding: 0 1;
    }

    StatusBar .auto-on {
        color: green;
    }

    StatusBar .auto-off {
        color: $text-muted;
    }
    """

    auto_refresh: reactive[bool] = reactive(True)
    refresh_interval: reactive[int] = reactive(30)
    last_refresh: reactive[Optional[datetime]] = reactive(None)
    error_message: reactive[Optional[str]] = reactive(None)

    def render(self) -> str:
        """Render the status bar content."""
        parts = []

        # Refresh status
        if self.auto_refresh:
            parts.append(f"[green]⟳ Auto ({self.refresh_interval}s)[/green]")
        else:
            parts.append("[dim]⟳ Manual[/dim]")

        # Last refresh time
        if self.last_refresh:
            time_str = self.last_refresh.strftime("%H:%M:%S")
            parts.append(f"Last: {time_str}")

        # Error message
        if self.error_message:
            parts.append(f"[red]Error: {self.error_message}[/red]")

        # Keyboard shortcuts
        shortcuts = (
            "↑↓:Navigate  Enter:Expand  r:Refresh  a:Auto-toggle  c:Cancel  q:Quit"
        )

        status = " │ ".join(parts)
        return f"{status}  │  {shortcuts}"
