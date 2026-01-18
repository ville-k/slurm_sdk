"""Shared Textual CSS styles for TUI applications.

These styles provide consistent theming across the dashboard and docs
viewer applications, matching the existing CLI color scheme.
"""

from __future__ import annotations

# Job state colors matching cli/formatters.py
STATE_COLORS = {
    "RUNNING": "green",
    "PENDING": "yellow",
    "COMPLETED": "blue",
    "FAILED": "red",
    "CANCELLED": "magenta",
    "TIMEOUT": "red",
    "NODE_FAIL": "red",
    "COMPLETING": "cyan",
    "CONFIGURING": "cyan",
    "SUSPENDED": "yellow",
}


def get_state_color(state: str) -> str:
    """Get color for job state.

    Args:
        state: Job state string (may include additional info after space).

    Returns:
        Color name for the state.
    """
    base_state = state.split()[0] if state else ""
    return STATE_COLORS.get(base_state, "white")


# Base CSS for all TUI apps
BASE_CSS = """
Screen {
    background: $surface;
}

Header {
    dock: top;
    height: 3;
    background: $primary;
    color: $text;
}

Footer {
    dock: bottom;
    height: 1;
    background: $primary-darken-2;
}

/* Navigation panel styling */
.nav-panel {
    width: 30;
    min-width: 20;
    max-width: 50;
    border-right: tall $primary;
    background: $surface;
}

/* Detail/content panel styling */
.content-panel {
    width: 1fr;
    background: $surface;
    padding: 1 2;
}

/* Tree widget styling */
Tree {
    padding: 1;
    scrollbar-gutter: stable;
}

Tree > .tree--cursor {
    background: $accent;
    color: $text;
}

Tree > .tree--highlight {
    background: $primary-lighten-2;
}

/* Status indicator styling */
.status-running {
    color: green;
}

.status-pending {
    color: yellow;
}

.status-completed {
    color: blue;
}

.status-failed {
    color: red;
}

.status-cancelled {
    color: magenta;
}

/* Panel titles */
.panel-title {
    text-style: bold;
    color: $text;
    padding: 0 1;
    background: $primary;
}

/* Refresh indicator */
.refresh-indicator {
    dock: right;
    width: auto;
    padding: 0 1;
}

.refresh-indicator.auto-enabled {
    color: green;
}

.refresh-indicator.auto-disabled {
    color: $text-muted;
}

/* Search input styling */
Input {
    border: tall $primary;
    padding: 0 1;
}

Input:focus {
    border: tall $accent;
}

/* Loading indicator */
LoadingIndicator {
    background: $surface;
}
"""

# Dashboard-specific CSS
DASHBOARD_CSS = (
    BASE_CSS
    + """
/* Job table styling */
DataTable {
    height: 100%;
}

DataTable > .datatable--cursor {
    background: $accent;
}

/* Detail panel content */
.job-detail {
    padding: 1;
}

.job-detail .label {
    text-style: bold;
    width: 15;
}

.job-detail .value {
    width: 1fr;
}

/* Partition info */
.partition-up {
    color: green;
}

.partition-down {
    color: red;
}

/* Action buttons */
Button {
    margin: 1 0;
}

Button.cancel-btn {
    background: red;
}
"""
)

# Docs viewer-specific CSS
DOCS_CSS = (
    BASE_CSS
    + """
/* Search input hidden by default, shown with / key */
#search-input {
    display: none;
}

/* Markdown scroll container */
#markdown-scroll {
    height: 1fr;
    background: $surface;
    scrollbar-gutter: stable;
}

/* Markdown viewer styling */
#markdown-viewer {
    padding: 1 2;
    background: $surface;
}

Markdown {
    padding: 1 2;
    background: $surface;
}

/* Search results */
.search-results {
    height: auto;
    max-height: 50%;
    border-bottom: tall $primary;
}

.search-result-item {
    padding: 0 1;
}

.search-result-item:hover {
    background: $primary-lighten-3;
}

.search-result-item.--highlight {
    background: $accent;
}

.search-snippet {
    color: $text-muted;
    text-style: italic;
}

/* Doc navigation tree */
.doc-nav Tree {
    padding: 0;
}

/* Current document indicator */
.current-doc {
    text-style: bold;
    color: $accent;
}
"""
)
