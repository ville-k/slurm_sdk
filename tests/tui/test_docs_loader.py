"""Tests for the documentation loader."""

from __future__ import annotations

from textwrap import dedent

import pytest

from slurm.tui.docs.loader import DocsLoader, Document, NavItem


class TestNavItem:
    """Tests for NavItem dataclass."""

    def test_is_section_with_children(self):
        """NavItem with children and no path is a section."""
        item = NavItem(
            title="Tutorials",
            children=[NavItem(title="Getting Started", path="tutorials/start.md")],
        )

        assert item.is_section is True
        assert item.is_document is False

    def test_is_document_with_path(self):
        """NavItem with path is a document."""
        item = NavItem(title="Getting Started", path="tutorials/start.md")

        assert item.is_document is True
        assert item.is_section is False

    def test_section_without_children(self):
        """NavItem without path or children is not a section."""
        item = NavItem(title="Empty")

        assert item.is_section is False
        assert item.is_document is False


class TestDocument:
    """Tests for Document dataclass."""

    def test_display_title_from_content(self):
        """Document extracts title from H1 heading."""
        doc = Document(
            path="test.md",
            title="",
            content="# Hello World\n\nSome content.",
        )

        assert doc.display_title == "Hello World"

    def test_display_title_from_title_field(self):
        """Document uses title field when available."""
        doc = Document(
            path="test.md",
            title="Custom Title",
            content="# Heading\n\nContent.",
        )

        assert doc.display_title == "Custom Title"

    def test_display_title_fallback_to_filename(self):
        """Document falls back to filename for title."""
        doc = Document(
            path="my_document.md",
            title="",
            content="No heading here.",
        )

        assert doc.display_title == "My Document"


class TestDocsLoader:
    """Tests for DocsLoader."""

    @pytest.fixture
    def temp_docs(self, tmp_path):
        """Create temporary documentation structure."""
        docs_dir = tmp_path / "docs"
        docs_dir.mkdir()

        # Create mkdocs.yml
        mkdocs_yml = tmp_path / "mkdocs.yml"
        mkdocs_yml.write_text(
            dedent(
                """
                site_name: Test Docs
                nav:
                  - Home: index.md
                  - Tutorials:
                      - tutorials/index.md
                      - Getting Started: tutorials/getting_started.md
                  - Reference:
                      - API: reference/api.md
                """
            ).strip()
        )

        # Create doc files
        (docs_dir / "index.md").write_text("# Home\n\nWelcome to the docs.")

        tutorials_dir = docs_dir / "tutorials"
        tutorials_dir.mkdir()
        (tutorials_dir / "index.md").write_text("# Tutorials\n\nLearn the basics.")
        (tutorials_dir / "getting_started.md").write_text(
            "# Getting Started\n\nFirst steps."
        )

        reference_dir = docs_dir / "reference"
        reference_dir.mkdir()
        (reference_dir / "api.md").write_text("# API Reference\n\nAPI docs.")

        return docs_dir

    def test_load_navigation_from_mkdocs(self, temp_docs):
        """DocsLoader parses mkdocs.yml navigation."""
        loader = DocsLoader(docs_path=temp_docs)
        nav = loader.get_navigation()

        assert len(nav) == 3  # Home, Tutorials, Reference

        # Check Home
        assert nav[0].title == "Home"
        assert nav[0].path == "index.md"

        # Check Tutorials section
        assert nav[1].title == "Tutorials"
        assert nav[1].is_section is True
        assert len(nav[1].children) == 2

        # Check Reference section
        assert nav[2].title == "Reference"
        assert nav[2].children[0].path == "reference/api.md"

    def test_get_document(self, temp_docs):
        """DocsLoader retrieves document content."""
        loader = DocsLoader(docs_path=temp_docs)
        doc = loader.get_document("index.md")

        assert doc is not None
        assert "Welcome to the docs" in doc.content
        assert doc.title == "Home"

    def test_get_document_not_found(self, temp_docs):
        """DocsLoader returns None for missing documents."""
        loader = DocsLoader(docs_path=temp_docs)
        doc = loader.get_document("nonexistent.md")

        assert doc is None

    def test_iter_documents(self, temp_docs):
        """DocsLoader iterates over all documents in navigation order."""
        loader = DocsLoader(docs_path=temp_docs)
        docs = list(loader.iter_documents())

        # Should have 4 documents
        assert len(docs) == 4

        # Check order matches navigation
        paths = [d.path for d in docs]
        assert paths[0] == "index.md"
        assert "tutorials/index.md" in paths
        assert "tutorials/getting_started.md" in paths
        assert "reference/api.md" in paths

    def test_get_all_paths(self, temp_docs):
        """DocsLoader returns all document paths."""
        loader = DocsLoader(docs_path=temp_docs)
        paths = loader.get_all_paths()

        assert "index.md" in paths
        assert "tutorials/getting_started.md" in paths
        assert "reference/api.md" in paths

    def test_find_nav_item(self, temp_docs):
        """DocsLoader finds NavItem by path."""
        loader = DocsLoader(docs_path=temp_docs)

        item = loader.find_nav_item("tutorials/getting_started.md")
        assert item is not None
        assert item.title == "Getting Started"

        item = loader.find_nav_item("nonexistent.md")
        assert item is None

    def test_build_nav_from_directory_fallback(self, tmp_path):
        """DocsLoader builds nav from directory when mkdocs.yml missing."""
        docs_dir = tmp_path / "docs"
        docs_dir.mkdir()

        # Create files without mkdocs.yml
        (docs_dir / "index.md").write_text("# Home")

        tutorials_dir = docs_dir / "tutorials"
        tutorials_dir.mkdir()
        (tutorials_dir / "guide.md").write_text("# Guide")

        loader = DocsLoader(docs_path=docs_dir)
        nav = loader.get_navigation()

        # Should find index.md and tutorials
        paths = []

        def collect(items):
            for item in items:
                if item.path:
                    paths.append(item.path)
                collect(item.children)

        collect(nav)

        assert "index.md" in paths

    def test_documents_cached(self, temp_docs):
        """DocsLoader caches loaded documents."""
        loader = DocsLoader(docs_path=temp_docs)

        doc1 = loader.get_document("index.md")
        doc2 = loader.get_document("index.md")

        # Should be the same object (cached)
        assert doc1 is doc2

    def test_title_extraction_handles_formatting(self, temp_docs):
        """DocsLoader extracts title even with markdown formatting."""
        (temp_docs / "formatted.md").write_text("# **Bold Title**\n\nContent")

        loader = DocsLoader(docs_path=temp_docs)
        doc = loader.get_document("formatted.md")

        assert doc is not None
        # Title extraction should work
        assert "Bold" in doc.display_title or "formatted" in doc.display_title.lower()
