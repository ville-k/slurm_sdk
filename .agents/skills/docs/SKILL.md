---
name: docs
description: Detailed Diataxis documentation guidelines — templates, decision guide, common mistakes, and quality checklist. Use when writing or updating documentation, docstrings, tutorials, how-to guides, reference docs, or explanations.
---

# Documentation Guidelines

Follow these guidelines when writing or updating any documentation in this project.

!`cat .agents/skills/docs/DOCUMENTATION_GUIDE.md`

## Project-Specific Notes

- Documentation is published using Material for MkDocs (`uv run mkdocs build` to verify).
- Markdown is formatted with mdformat (`uv run mdformat <docs-paths>`).
- `docs/index.md` is excluded from mdformat as it uses special MkDocs Material grid syntax.
- All public and private APIs have Google-style docstrings rendered via mkdocstrings.
- Add navigation entries in `mkdocs.yml` for new pages.
- Always add changelog entries to `docs/CHANGELOG.md` under `## [Unreleased]`.
