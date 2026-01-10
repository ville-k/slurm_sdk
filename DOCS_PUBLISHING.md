# Documentation Publishing

This document describes how the Slurm SDK documentation is built and published using MkDocs and GitHub Pages.

## Overview

- **Framework**: [MkDocs](https://www.mkdocs.org/) with [Material theme](https://squidfunk.github.io/mkdocs-material/)
- **Hosting**: GitHub Pages
- **Automation**: GitHub Actions workflow

## Local Development

### Preview documentation

```bash
uv run mkdocs serve
```

Opens a local server at `http://127.0.0.1:8000` with hot reload.

### Build documentation

```bash
uv run mkdocs build
```

Builds static HTML to the `site/` directory.

## GitHub Pages Publishing

### Automatic Deployment

The documentation is automatically published to GitHub Pages when:

1. Changes are pushed to the `main` branch affecting:
   - `docs/**` (documentation files)
   - `mkdocs.yml` (configuration)
   - `.github/workflows/publish-docs.yml` (workflow)

2. The workflow can also be triggered manually via GitHub Actions.

### Workflow Location

`.github/workflows/publish-docs.yml`

### Published URL

<https://ville-k.github.io/slurm_sdk/>

## Configuration

### mkdocs.yml

Key settings:

- **site_name**: Slurm SDK
- **theme**: Material with navigation features
- **plugins**: search, mkdocstrings (Python API docs)
- **extensions**: admonition, code highlighting, Mermaid diagrams

### GitHub Repository Settings

To enable GitHub Pages:

1. Go to repository **Settings** > **Pages**
2. Under **Build and deployment**, select **GitHub Actions** as the source
3. The workflow will deploy to the `github-pages` environment

## Documentation Structure

Follows the [Diataxis framework](https://diataxis.fr/):

```text
docs/
├── index.md              # Home page
├── tutorials/            # Learning-oriented lessons
├── how-to/               # Task-oriented guides
├── reference/            # API and technical specs
│   └── api/              # Auto-generated from docstrings
└── explanation/          # Understanding-oriented discussion
```

## Adding New Documentation

1. Create markdown files in the appropriate `docs/` subdirectory
2. Add navigation entries to `mkdocs.yml` under the `nav:` section
3. Preview locally with `uv run mkdocs serve`
4. Commit and push to `main` to publish

## Troubleshooting

### Build fails locally

Ensure dependencies are installed:

```bash
uv sync
```

### Build fails in CI

Check the GitHub Actions logs for errors. Common issues:

- Missing navigation entries for new files
- Broken internal links
- Invalid YAML in mkdocs.yml

### Pages not updating

- Verify the workflow completed successfully in Actions tab
- Check that GitHub Pages is configured to use GitHub Actions as the source
- Allow a few minutes for CDN propagation
