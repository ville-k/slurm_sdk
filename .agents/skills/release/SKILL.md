---
name: release
description: Guide for publishing releases to PyPI — dev releases, production releases, version bumps, and changelog updates. Use when cutting a release or publishing a package.
---

# Publishing to PyPI

The package is published to PyPI via GitHub Actions using trusted publishing (no API tokens needed).

## Dev Releases

Dev releases publish the current version in `pyproject.toml` (e.g., `0.4.5-dev`) for testing:

```bash
gh workflow run publish.yml -f version_type=dev
```

To test the build without uploading:

```bash
gh workflow run publish.yml -f version_type=dev -f dry_run=true
```

## Production Releases

Production releases require a clean version number and updated changelog:

1. **Update version** in `pyproject.toml` (remove `-dev` suffix):

   ```python
   version = "0.4.5"  # was "0.4.5-dev"
   ```

2. **Update changelog** in `docs/CHANGELOG.md`:

   - Move entries from `## [Unreleased]` to new section `## [0.4.5] - YYYY-MM-DD`
   - Keep an empty `## [Unreleased]` section at the top

3. **Commit, tag, and create GitHub release**:

   ```bash
   git add pyproject.toml docs/CHANGELOG.md
   git commit -m "chore: release v0.4.5"
   git tag v0.4.5
   git push origin main --tags
   gh release create v0.4.5 --generate-notes
   ```

   The GitHub release event automatically triggers PyPI publishing.

4. **Prepare for next development cycle**:

   ```bash
   # Update version to next dev version
   # version = "0.4.6-dev"
   git commit -am "chore: bump version to 0.4.6-dev"
   git push
   ```

## Manual Production Release

If you need to publish a release without creating a GitHub release:

```bash
gh workflow run publish.yml -f version_type=release
```

This validates that the version doesn't contain `-dev`, `-alpha`, or `-beta` suffixes.
