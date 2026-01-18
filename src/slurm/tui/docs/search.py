"""Full-text search index for documentation.

Uses SQLite FTS5 for efficient full-text search across all documentation.
"""

from __future__ import annotations

import hashlib
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from .loader import DocsLoader


@dataclass
class SearchResult:
    """A search result with context snippet."""

    path: str
    title: str
    snippet: str
    rank: float

    @property
    def display_snippet(self) -> str:
        """Get snippet formatted for display."""
        # Clean up the snippet
        snippet = self.snippet.strip()
        # Limit length
        if len(snippet) > 200:
            snippet = snippet[:197] + "..."
        return snippet


class DocsSearchIndex:
    """SQLite FTS5 search index for documentation."""

    def __init__(self, loader: "DocsLoader") -> None:
        """Initialize the search index.

        Args:
            loader: DocsLoader instance for accessing documents.
        """
        self._loader = loader
        self._db_path = self._get_cache_path()
        self._conn: Optional[sqlite3.Connection] = None

    def _get_cache_path(self) -> Path:
        """Get path to cache database.

        Returns:
            Path to SQLite database file.
        """
        # Use XDG cache dir or fallback
        cache_dir = (
            Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "slurm"
        )
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / "docs_search.db"

    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection, creating if needed.

        Returns:
            SQLite connection.
        """
        if self._conn is None:
            self._conn = sqlite3.connect(str(self._db_path))
            # Enable FTS5
            self._conn.execute("PRAGMA journal_mode=WAL")
        return self._conn

    def _compute_docs_hash(self) -> str:
        """Compute hash of all documentation files.

        Returns:
            MD5 hash of all doc content.
        """
        hasher = hashlib.md5(usedforsecurity=False)
        for doc in self._loader.iter_documents():
            hasher.update(doc.path.encode())
            hasher.update(doc.content.encode())
        return hasher.hexdigest()

    def _get_stored_hash(self) -> Optional[str]:
        """Get stored documentation hash from database.

        Returns:
            Stored hash, or None if not found.
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute("SELECT value FROM metadata WHERE key = 'docs_hash'")
            row = cursor.fetchone()
            return row[0] if row else None
        except sqlite3.OperationalError:
            return None

    def _store_hash(self, hash_value: str) -> None:
        """Store documentation hash in database.

        Args:
            hash_value: Hash to store.
        """
        conn = self._get_connection()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('docs_hash', ?)",
            (hash_value,),
        )
        conn.commit()

    def ensure_index(self) -> None:
        """Ensure search index is up to date.

        Rebuilds the index if documentation has changed.
        """
        current_hash = self._compute_docs_hash()
        stored_hash = self._get_stored_hash()

        if current_hash != stored_hash:
            self._rebuild_index()
            self._store_hash(current_hash)

    def _rebuild_index(self) -> None:
        """Rebuild the FTS5 search index."""
        conn = self._get_connection()

        # Drop existing table
        conn.execute("DROP TABLE IF EXISTS docs_fts")

        # Create FTS5 table
        conn.execute(
            """
            CREATE VIRTUAL TABLE docs_fts USING fts5(
                path,
                title,
                content,
                tokenize='porter unicode61'
            )
            """
        )

        # Index all documents
        for doc in self._loader.iter_documents():
            # Strip markdown formatting for better search
            plain_content = self._strip_markdown(doc.content)
            conn.execute(
                "INSERT INTO docs_fts (path, title, content) VALUES (?, ?, ?)",
                (doc.path, doc.title, plain_content),
            )

        conn.commit()

    def _strip_markdown(self, content: str) -> str:
        """Strip markdown formatting for indexing.

        Args:
            content: Markdown content.

        Returns:
            Plain text content.
        """
        # Remove code blocks
        content = re.sub(r"```[\s\S]*?```", " ", content)
        content = re.sub(r"`[^`]+`", " ", content)

        # Remove links but keep text
        content = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", content)

        # Remove images
        content = re.sub(r"!\[([^\]]*)\]\([^)]+\)", r"\1", content)

        # Remove headers markers
        content = re.sub(r"^#+\s+", "", content, flags=re.MULTILINE)

        # Remove emphasis markers
        content = re.sub(r"[*_]{1,3}([^*_]+)[*_]{1,3}", r"\1", content)

        # Remove HTML tags
        content = re.sub(r"<[^>]+>", " ", content)

        # Collapse whitespace
        content = re.sub(r"\s+", " ", content)

        return content.strip()

    def search(self, query: str, limit: int = 50) -> List[SearchResult]:
        """Search documentation.

        Args:
            query: Search query string.
            limit: Maximum number of results.

        Returns:
            List of SearchResult objects.
        """
        self.ensure_index()
        conn = self._get_connection()

        # Escape query for FTS5
        safe_query = self._escape_fts_query(query)

        try:
            cursor = conn.execute(
                """
                SELECT
                    path,
                    title,
                    snippet(docs_fts, 2, '**', '**', '...', 32) as snippet,
                    rank
                FROM docs_fts
                WHERE docs_fts MATCH ?
                ORDER BY rank
                LIMIT ?
                """,
                (safe_query, limit),
            )

            results = []
            for row in cursor:
                results.append(
                    SearchResult(
                        path=row[0],
                        title=row[1],
                        snippet=row[2],
                        rank=row[3],
                    )
                )
            return results

        except sqlite3.OperationalError:
            # Query syntax error - try simpler search
            return self._simple_search(query, limit)

    def _escape_fts_query(self, query: str) -> str:
        """Escape query for FTS5 syntax with prefix matching.

        Args:
            query: Raw search query.

        Returns:
            Escaped query safe for FTS5 with wildcards for partial matching.
        """
        # Remove FTS5 special characters that could cause syntax errors
        # Keep simple words and phrases
        query = re.sub(r'[^\w\s"-]', " ", query)

        # Handle quoted phrases
        words = []
        in_quote = False
        current_word = []

        for char in query:
            if char == '"':
                if in_quote:
                    if current_word:
                        # Quoted phrases: exact match, no wildcard
                        words.append('"' + "".join(current_word) + '"')
                        current_word = []
                in_quote = not in_quote
            elif char.isspace():
                if in_quote:
                    current_word.append(char)
                elif current_word:
                    # Add wildcard for prefix matching
                    words.append("".join(current_word) + "*")
                    current_word = []
            else:
                current_word.append(char)

        if current_word:
            if in_quote:
                words.append('"' + "".join(current_word) + '"')
            else:
                # Add wildcard for prefix matching
                words.append("".join(current_word) + "*")

        # Join with space (implicit AND in FTS5)
        return " ".join(words)

    def _simple_search(self, query: str, limit: int) -> List[SearchResult]:
        """Fallback simple search when FTS5 query fails.

        Args:
            query: Search query.
            limit: Maximum results.

        Returns:
            List of SearchResult objects.
        """
        conn = self._get_connection()

        # Simple LIKE search as fallback
        pattern = f"%{query}%"
        cursor = conn.execute(
            """
            SELECT path, title, content
            FROM docs_fts
            WHERE content LIKE ? OR title LIKE ?
            LIMIT ?
            """,
            (pattern, pattern, limit),
        )

        results = []
        for row in cursor:
            path, title, content = row
            # Extract snippet around first match
            snippet = self._extract_snippet(content, query)
            results.append(
                SearchResult(
                    path=path,
                    title=title,
                    snippet=snippet,
                    rank=0.0,
                )
            )
        return results

    def _extract_snippet(self, content: str, query: str, context: int = 50) -> str:
        """Extract snippet around first match.

        Args:
            content: Full content.
            query: Search query.
            context: Characters of context around match.

        Returns:
            Snippet string.
        """
        lower_content = content.lower()
        lower_query = query.lower()
        pos = lower_content.find(lower_query)

        if pos == -1:
            return content[:100] + "..." if len(content) > 100 else content

        start = max(0, pos - context)
        end = min(len(content), pos + len(query) + context)

        snippet = content[start:end]
        if start > 0:
            snippet = "..." + snippet
        if end < len(content):
            snippet = snippet + "..."

        return snippet

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
