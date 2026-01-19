"""Navigation tree widget for documentation viewer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

from textual.widgets import Tree
from textual.widgets.tree import TreeNode

if TYPE_CHECKING:
    from ..loader import NavItem


@dataclass
class NavNodeData:
    """Data attached to navigation tree nodes."""

    path: Optional[str]  # Document path, None for sections
    title: str
    is_section: bool


class DocsNavTree(Tree[NavNodeData]):
    """Navigation tree for documentation structure."""

    DEFAULT_CSS = """
    DocsNavTree {
        width: 100%;
        height: 100%;
        scrollbar-gutter: stable;
        padding: 0 1;
    }

    DocsNavTree > .tree--cursor {
        background: $accent;
    }

    DocsNavTree .tree--guides {
        color: $text-muted;
    }
    """

    def __init__(self, **kwargs) -> None:
        """Initialize the navigation tree."""
        super().__init__(
            "📚 Documentation",
            data=NavNodeData(path=None, title="Documentation", is_section=True),
            **kwargs,
        )

    def set_navigation(self, nav_items: List["NavItem"]) -> None:
        """Set the navigation structure.

        Args:
            nav_items: List of NavItem objects from DocsLoader.
        """
        self.clear()

        def add_items(parent: TreeNode[NavNodeData], items: List["NavItem"]) -> None:
            for item in items:
                icon = self._get_icon(item)
                label = f"{icon} {item.title}"

                node_data = NavNodeData(
                    path=item.path,
                    title=item.title,
                    is_section=item.is_section,
                )

                if item.children:
                    # Section with children
                    node = parent.add(label, data=node_data)
                    add_items(node, item.children)
                else:
                    # Leaf document
                    parent.add_leaf(label, data=node_data)

        add_items(self.root, nav_items)
        self.root.expand()

    def _get_icon(self, item: "NavItem") -> str:
        """Get icon for navigation item.

        Args:
            item: NavItem to get icon for.

        Returns:
            Emoji icon.
        """
        if item.is_section:
            return "📁"

        # Determine icon based on path/title
        title_lower = item.title.lower()
        path_lower = (item.path or "").lower()

        if "tutorial" in path_lower or "getting started" in title_lower:
            return "🎓"
        elif "how-to" in path_lower or "how to" in title_lower:
            return "🔧"
        elif "reference" in path_lower or "api" in title_lower:
            return "📖"
        elif "explanation" in path_lower:
            return "💡"
        elif "changelog" in title_lower:
            return "📋"
        elif "contributing" in title_lower:
            return "🤝"
        elif "index" in path_lower:
            return "📑"
        else:
            return "📄"

    def set_current_document(self, path: Optional[str]) -> None:
        """Select the current document in the tree.

        Args:
            path: Path of current document, or None to clear.
        """
        if path:
            self._select_by_path(path)

    def _select_by_path(self, path: str) -> bool:
        """Select a node by its document path.

        Args:
            path: Document path to select.

        Returns:
            True if found and selected.
        """

        def find_node(node: TreeNode[NavNodeData]) -> TreeNode[NavNodeData] | None:
            """Find node with matching path."""
            if node.data and node.data.path == path:
                return node
            for child in node.children:
                found = find_node(child)
                if found:
                    return found
            return None

        target = find_node(self.root)
        if target:
            # Expand all parent nodes first so the target is visible
            parent = target.parent
            while parent:
                parent.expand()
                parent = parent.parent
            # Now select the node and move cursor
            self.select_node(target)
            self.move_cursor(target)
            self.scroll_to_node(target)
            return True
        return False
