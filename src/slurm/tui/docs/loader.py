"""Documentation loader for bundled markdown files.

Loads documentation from the bundled _bundled_docs directory and parses
the mkdocs.yml navigation structure.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

import yaml


class _SafeLoaderIgnoreUnknown(yaml.SafeLoader):
    """YAML SafeLoader that ignores unknown tags (like !!python/name:...)."""

    pass


def _unknown_tag_constructor(loader: yaml.Loader, tag_suffix: str, node: yaml.Node):
    """Handle unknown YAML tags by returning None or the raw value."""
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    elif isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    return None


# Register handler for all unknown tags
_SafeLoaderIgnoreUnknown.add_multi_constructor("", _unknown_tag_constructor)


@dataclass
class NavItem:
    """Navigation item from mkdocs.yml structure."""

    title: str
    path: Optional[str] = None  # None for section headers
    children: List["NavItem"] = field(default_factory=list)

    @property
    def is_section(self) -> bool:
        """Check if this is a section header (has children, no direct path)."""
        return self.path is None and len(self.children) > 0

    @property
    def is_document(self) -> bool:
        """Check if this is a document (has a path)."""
        return self.path is not None


@dataclass
class Document:
    """A loaded documentation file."""

    path: str  # Relative path like "tutorials/getting_started.md"
    title: str
    content: str

    @property
    def display_title(self) -> str:
        """Get title for display, extracting from first heading if needed."""
        if self.title:
            return self.title

        # Try to extract from first H1
        match = re.match(r"^#\s+(.+)$", self.content, re.MULTILINE)
        if match:
            return match.group(1).strip()

        # Fallback to filename
        return Path(self.path).stem.replace("_", " ").title()


class DocsLoader:
    """Loads bundled documentation and mkdocs.yml navigation."""

    def __init__(self, docs_path: Optional[Path] = None) -> None:
        """Initialize the docs loader.

        Args:
            docs_path: Optional override for docs path. If not provided,
                      will search for bundled docs.
        """
        self._docs_path = docs_path or self._find_docs_path()
        self._nav_structure: List[NavItem] = []
        self._documents: Dict[str, Document] = {}
        self._loaded = False

    def _find_docs_path(self) -> Path:
        """Find the bundled docs directory.

        Returns:
            Path to the docs directory.

        Raises:
            FileNotFoundError: If docs cannot be found.
        """
        # Try importlib.resources for installed package
        try:
            from importlib.resources import files

            pkg_path = files("slurm")
            bundled_path = Path(str(pkg_path)) / "_bundled_docs"
            if bundled_path.exists():
                return bundled_path
        except Exception:
            pass

        # Try relative to this file (development mode)
        dev_path = Path(__file__).parent.parent.parent.parent.parent.parent / "docs"
        if dev_path.exists():
            return dev_path

        # Try from current working directory
        cwd_path = Path.cwd() / "docs"
        if cwd_path.exists():
            return cwd_path

        raise FileNotFoundError(
            "Could not find bundled documentation. "
            "Ensure slurm-sdk is properly installed."
        )

    @property
    def docs_path(self) -> Path:
        """Get the documentation root path."""
        return self._docs_path

    def load(self) -> None:
        """Load navigation structure and cache document metadata."""
        if self._loaded:
            return

        self._nav_structure = self._parse_mkdocs_yml()
        self._loaded = True

    def _parse_mkdocs_yml(self) -> List[NavItem]:
        """Parse mkdocs.yml to build navigation tree.

        Returns:
            List of top-level NavItem objects.
        """
        # Try to find mkdocs.yml - first in bundled docs, then project root
        mkdocs_path = self._docs_path / "mkdocs.yml"
        if not mkdocs_path.exists():
            # Try parent directory (project root in dev mode)
            mkdocs_path = self._docs_path.parent / "mkdocs.yml"

        if not mkdocs_path.exists():
            # Fallback: build nav from directory structure
            return self._build_nav_from_directory()

        with open(mkdocs_path, "r", encoding="utf-8") as f:
            # Using custom SafeLoader subclass that handles unknown tags
            config = yaml.load(f, Loader=_SafeLoaderIgnoreUnknown)  # nosec B506

        nav_config = config.get("nav", [])
        return self._parse_nav_items(nav_config)

    def _parse_nav_items(
        self, nav_config: List[Union[str, Dict[str, Any]]]
    ) -> List[NavItem]:
        """Recursively parse nav configuration.

        Args:
            nav_config: Nav section from mkdocs.yml.

        Returns:
            List of NavItem objects.
        """
        items = []

        for entry in nav_config:
            if isinstance(entry, str):
                # Simple path entry
                items.append(NavItem(title=self._title_from_path(entry), path=entry))

            elif isinstance(entry, dict):
                # Title: path or Title: [children]
                for title, value in entry.items():
                    if isinstance(value, str):
                        # Title: path
                        items.append(NavItem(title=title, path=value))
                    elif isinstance(value, list):
                        # Title: [children]
                        children = self._parse_nav_items(value)
                        items.append(NavItem(title=title, children=children))

        return items

    def _title_from_path(self, path: str) -> str:
        """Generate title from file path.

        Args:
            path: File path like "tutorials/getting_started.md".

        Returns:
            Human-readable title.
        """
        stem = Path(path).stem
        # Handle index files
        if stem == "index":
            stem = Path(path).parent.name or "Home"
        return stem.replace("_", " ").replace("-", " ").title()

    def _build_nav_from_directory(self) -> List[NavItem]:
        """Build navigation from directory structure as fallback.

        Returns:
            List of NavItem objects.
        """
        items = []

        # Add index.md if exists
        index_path = self._docs_path / "index.md"
        if index_path.exists():
            items.append(NavItem(title="Home", path="index.md"))

        # Add directories as sections
        for subdir in sorted(self._docs_path.iterdir()):
            if subdir.is_dir() and not subdir.name.startswith(("_", ".")):
                children = []
                for md_file in sorted(subdir.glob("*.md")):
                    rel_path = md_file.relative_to(self._docs_path)
                    children.append(
                        NavItem(
                            title=self._title_from_path(str(rel_path)),
                            path=str(rel_path),
                        )
                    )
                if children:
                    items.append(
                        NavItem(
                            title=subdir.name.replace("_", " ")
                            .replace("-", " ")
                            .title(),
                            children=children,
                        )
                    )

        # Add top-level md files (except index.md)
        for md_file in sorted(self._docs_path.glob("*.md")):
            if md_file.name != "index.md":
                items.append(
                    NavItem(
                        title=self._title_from_path(md_file.name),
                        path=md_file.name,
                    )
                )

        return items

    def get_navigation(self) -> List[NavItem]:
        """Get the navigation structure.

        Returns:
            List of top-level NavItem objects.
        """
        self.load()
        return self._nav_structure

    def get_document(self, path: str) -> Optional[Document]:
        """Get a document by its path.

        Args:
            path: Relative path like "tutorials/getting_started.md".

        Returns:
            Document object, or None if not found.
        """
        if path in self._documents:
            return self._documents[path]

        full_path = self._docs_path / path
        if not full_path.exists():
            return None

        try:
            content = full_path.read_text(encoding="utf-8")
            title = self._extract_title(content, path)
            doc = Document(path=path, title=title, content=content)
            self._documents[path] = doc
            return doc
        except Exception:
            return None

    def _extract_title(self, content: str, path: str) -> str:
        """Extract title from document content.

        Args:
            content: Markdown content.
            path: File path as fallback.

        Returns:
            Document title.
        """
        # Try to find first H1 heading
        match = re.match(r"^#\s+(.+)$", content, re.MULTILINE)
        if match:
            return match.group(1).strip()

        return self._title_from_path(path)

    def iter_documents(self) -> Iterator[Document]:
        """Iterate over all documents in the navigation.

        Yields:
            Document objects for each navigable document.
        """
        self.load()

        def iter_nav(items: List[NavItem]) -> Iterator[str]:
            for item in items:
                if item.path:
                    yield item.path
                if item.children:
                    yield from iter_nav(item.children)

        for path in iter_nav(self._nav_structure):
            doc = self.get_document(path)
            if doc:
                yield doc

    def get_all_paths(self) -> List[str]:
        """Get all document paths in navigation order.

        Returns:
            List of document paths.
        """
        self.load()
        paths = []

        def collect_paths(items: List[NavItem]) -> None:
            for item in items:
                if item.path:
                    paths.append(item.path)
                if item.children:
                    collect_paths(item.children)

        collect_paths(self._nav_structure)
        return paths

    def find_nav_item(self, path: str) -> Optional[NavItem]:
        """Find a NavItem by its path.

        Args:
            path: Document path to find.

        Returns:
            NavItem if found, None otherwise.
        """
        self.load()

        def search(items: List[NavItem]) -> Optional[NavItem]:
            for item in items:
                if item.path == path:
                    return item
                if item.children:
                    found = search(item.children)
                    if found:
                        return found
            return None

        return search(self._nav_structure)
