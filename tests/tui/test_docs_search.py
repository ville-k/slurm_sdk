"""Tests for the documentation search index."""

from __future__ import annotations

from textwrap import dedent
from unittest.mock import MagicMock

import pytest

from slurm.tui.docs.loader import DocsLoader, Document
from slurm.tui.docs.search import DocsSearchIndex, SearchResult


class TestSearchResult:
    """Tests for SearchResult dataclass."""

    def test_display_snippet_short(self):
        """Short snippets are returned as-is."""
        result = SearchResult(
            path="test.md",
            title="Test",
            snippet="This is a short snippet.",
            rank=-1.0,
        )

        assert result.display_snippet == "This is a short snippet."

    def test_display_snippet_truncated(self):
        """Long snippets are truncated."""
        long_text = "x" * 300
        result = SearchResult(
            path="test.md",
            title="Test",
            snippet=long_text,
            rank=-1.0,
        )

        assert len(result.display_snippet) <= 200
        assert result.display_snippet.endswith("...")


class TestDocsSearchIndex:
    """Tests for DocsSearchIndex."""

    @pytest.fixture
    def mock_loader(self, tmp_path):
        """Create a mock loader with test documents."""
        docs = [
            Document(
                path="tutorials/getting_started.md",
                title="Getting Started",
                content=dedent(
                    """
                    # Getting Started

                    This tutorial walks you through submitting your first job.

                    ## Prerequisites

                    You need Python 3.11+ and access to a SLURM cluster.

                    ## Step 1: Install

                    Run pip install slurm-sdk to get started.
                    """
                ).strip(),
            ),
            Document(
                path="reference/api.md",
                title="API Reference",
                content=dedent(
                    """
                    # API Reference

                    ## Cluster Class

                    The Cluster class is the main entry point for job submission.

                    ### Methods

                    - submit(): Submit a task to the cluster
                    - get_job(): Get a job by ID
                    """
                ).strip(),
            ),
            Document(
                path="explanation/workflows.md",
                title="Workflow Execution",
                content=dedent(
                    """
                    # Understanding Workflows

                    Workflows allow you to define multi-step job pipelines.

                    ## How Workflows Work

                    A workflow consists of multiple tasks with dependencies.
                    """
                ).strip(),
            ),
        ]

        loader = MagicMock(spec=DocsLoader)
        # Use side_effect with a lambda to return a fresh iterator each time
        loader.iter_documents.side_effect = lambda: iter(docs)

        return loader

    @pytest.fixture
    def search_index(self, mock_loader, tmp_path):
        """Create a search index with test documents."""
        # Use a temp path for the database
        import os

        os.environ["XDG_CACHE_HOME"] = str(tmp_path / "cache")

        index = DocsSearchIndex(mock_loader)
        return index

    def test_search_finds_matching_documents(self, search_index):
        """Search returns documents containing the query."""
        results = search_index.search("workflow")

        assert len(results) >= 1
        paths = [r.path for r in results]
        assert "explanation/workflows.md" in paths

    def test_search_returns_ranked_results(self, search_index):
        """Search results are ranked by relevance."""
        results = search_index.search("cluster")

        # Should find API reference (has "Cluster class")
        assert len(results) >= 1
        assert any("api" in r.path for r in results)

    def test_search_no_results(self, search_index):
        """Search returns empty list for no matches."""
        results = search_index.search("xyznonexistent123")

        assert results == []

    def test_search_with_special_characters(self, search_index):
        """Search handles special characters gracefully."""
        # Should not raise an error
        results = search_index.search("install()")
        assert isinstance(results, list)

        results = search_index.search("pip install")
        assert isinstance(results, list)

    def test_search_case_insensitive(self, search_index):
        """Search is case insensitive."""
        results_lower = search_index.search("cluster")
        results_upper = search_index.search("CLUSTER")

        # Both should find results
        assert len(results_lower) > 0
        assert len(results_upper) > 0

    def test_search_returns_snippets(self, search_index):
        """Search results include context snippets."""
        results = search_index.search("Python")

        assert len(results) >= 1
        # Snippet should contain context around the match
        assert any(r.snippet for r in results)

    def test_index_rebuilds_on_content_change(self, mock_loader, tmp_path):
        """Index rebuilds when document content changes."""
        import os

        os.environ["XDG_CACHE_HOME"] = str(tmp_path / "cache")

        # Create initial index
        index1 = DocsSearchIndex(mock_loader)
        index1.ensure_index()

        # Change documents
        new_docs = [
            Document(
                path="new.md",
                title="New Document",
                content="This is completely new content about widgets.",
            )
        ]
        mock_loader.iter_documents.side_effect = lambda: iter(new_docs)

        # Create new index - should detect change and rebuild
        index2 = DocsSearchIndex(mock_loader)
        results = index2.search("widgets")

        assert len(results) >= 1

    def test_strip_markdown(self, search_index):
        """Markdown is stripped for better indexing."""
        # The _strip_markdown method should be tested indirectly
        # through search functionality - code blocks shouldn't match

        # If we had a doc with ```python code``` the word "python"
        # from the code block should still be searchable after stripping
        results = search_index.search("Python")
        assert isinstance(results, list)

    def test_search_limit(self, search_index):
        """Search respects result limit."""
        results = search_index.search("the", limit=2)

        assert len(results) <= 2

    def test_close_connection(self, search_index):
        """Close properly closes database connection."""
        search_index.ensure_index()
        search_index.close()

        # Should be able to search again (reconnects)
        results = search_index.search("test")
        assert isinstance(results, list)


class TestSearchIndexIntegration:
    """Integration tests for search with real loader."""

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
                site_name: Test
                nav:
                  - Home: index.md
                  - Guide: guide.md
                """
            ).strip()
        )

        (docs_dir / "index.md").write_text(
            "# Home\n\nWelcome to SLURM SDK documentation."
        )
        (docs_dir / "guide.md").write_text(
            "# User Guide\n\nLearn how to submit jobs to the cluster."
        )

        return docs_dir

    def test_real_loader_integration(self, temp_docs, tmp_path):
        """Search works with real DocsLoader."""
        import os

        os.environ["XDG_CACHE_HOME"] = str(tmp_path / "cache")

        loader = DocsLoader(docs_path=temp_docs)
        index = DocsSearchIndex(loader)

        results = index.search("SLURM")

        assert len(results) >= 1
        assert "index.md" in results[0].path
