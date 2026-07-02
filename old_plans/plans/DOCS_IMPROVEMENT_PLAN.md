# Documentation Improvement Plan

This plan outlines prioritized improvements to SLURM SDK documentation based on competitive analysis of Flyte's documentation.

______________________________________________________________________

## Quick Wins (1-2 weeks)

### 1. Enable Dark Mode

**Description**: Enable MkDocs Material dark mode toggle for better developer experience.

**Acceptance Criteria**:

- [x] Dark mode toggle visible in header
- [x] User preference persisted in localStorage
- [x] All pages render correctly in dark mode
- [x] Code blocks have appropriate contrast in both modes

**Implementation**:

```yaml
# mkdocs.yml
theme:
  palette:
    - scheme: default
      toggle:
        icon: material/brightness-7
        name: Switch to dark mode
    - scheme: slate
      toggle:
        icon: material/brightness-4
        name: Switch to light mode
```

______________________________________________________________________

### 2. Add Code Copy Buttons

**Description**: Enable one-click copy for all code blocks.

**Acceptance Criteria**:

- [x] Copy button appears on hover for all code blocks
- [x] Clicking copies code to clipboard
- [x] Visual feedback on successful copy

**Implementation**:

```yaml
# mkdocs.yml
theme:
  features:
    - content.code.copy
```

______________________________________________________________________

### 3. Add Search Functionality

**Description**: Enable built-in MkDocs search with keyboard shortcut.

**Acceptance Criteria**:

- [x] Search icon visible in header
- [x] Cmd/Ctrl+K opens search modal
- [x] Search results show relevant pages
- [x] Search works across all documentation

**Implementation**:

```yaml
# mkdocs.yml
plugins:
  - search:
      lang: en
      separator: '[\s\-\.]+'
```

______________________________________________________________________

### 4. Add "On This Page" Table of Contents

**Description**: Show right-sidebar ToC for current page navigation.

**Acceptance Criteria**:

- [x] Right sidebar visible on desktop
- [x] Shows H2 and H3 headings
- [x] Highlights current section on scroll
- [x] Collapses on mobile

**Implementation**:

```yaml
# mkdocs.yml
theme:
  features:
    - navigation.toc.integrate
markdown_extensions:
  - toc:
      permalink: true
```

______________________________________________________________________

### 5. Link CONTRIBUTING.md from Docs

**Description**: Make contribution guide discoverable from documentation.

**Acceptance Criteria**:

- [x] "Contributing" link in footer or navigation
- [x] Links to CONTRIBUTING.md or dedicated docs page
- [x] Includes link to GitHub issues

**Implementation**: Add to navigation in mkdocs.yml or create docs/community/contributing.md that references the root file.

______________________________________________________________________

## Priority Improvements (1-3 months)

### 6. Create Marketing-Quality Landing Page

**Description**: Replace current index.md with a feature-rich landing page that communicates value proposition clearly.

**Acceptance Criteria**:

- [x] Hero section with tagline and primary CTA
- [x] "Why SLURM SDK" section with 4-6 key benefits
- [x] Interactive code sample showing @task and @workflow
- [x] Quick start command prominently displayed
- [x] Links to tutorials, guides, and API reference
- [x] Comparison table vs alternatives (brief)

**Content Structure**:

````markdown
# SLURM SDK

> Container-first job orchestration for Slurm clusters

[Get Started](tutorials/getting_started.md){ .md-button .md-button--primary }
[View on GitHub](https://github.com/...){ .md-button }

## Why SLURM SDK?

<grid>
- **Zero Infrastructure**: No Kubernetes, no control plane
- **Native SLURM**: Array jobs, dependencies, partitions
- **Container-First**: Reproducible environments
- **Pythonic API**: @task and @workflow decorators
</grid>

## Quick Start

```python
from slurm import Cluster, task

@task(time="00:10:00", mem="4G")
def train(dataset: str) -> dict:
    return {"accuracy": 0.95}
````

## Documentation

- [Tutorials](tutorials/) - Learn by example
- [How-To Guides](how-to/) - Solve specific problems
- [Reference](reference/) - API documentation
- [Explanation](explanation/) - Understand the architecture

```

---

### 7. Add "SLURM Concepts" Explainer
**Description**: Help users new to SLURM understand key concepts (partitions, array jobs, sbatch, etc.).

**Acceptance Criteria**:
- [x] Explains partitions, nodes, tasks
- [x] Covers sbatch, srun, scancel basics
- [x] Shows how SDK maps to native SLURM
- [x] Links to official SLURM documentation

**Location**: `docs/explanation/slurm_concepts.md`

---

### 8. Add Integration Guides Section
**Description**: Document how to use SLURM SDK with common ML tools.

**Acceptance Criteria**:
- [ ] MLflow integration guide
- [ ] Weights & Biases integration guide
- [ ] Each guide includes working example
- [ ] Shows how to handle artifact logging in container

**Location**: `docs/how-to/integrations/`

**Example Structure**:
```

how-to/
integrations/
index.md
mlflow.md
wandb.md
tensorboard.md

````

---

### 9. Add Domain-Specific Tutorials
**Description**: Create tutorials for common use cases beyond hello world.

**Acceptance Criteria**:
- [ ] Multi-GPU training tutorial (PyTorch DDP or similar)
- [ ] Data processing pipeline tutorial
- [ ] Hyperparameter sweep tutorial using array jobs
- [ ] Each tutorial is end-to-end runnable

**Location**: `docs/tutorials/`

---

### 10. Improve Navigation Structure
**Description**: Enhance sidebar navigation with better grouping and icons.

**Acceptance Criteria**:
- [x] Tutorials, How-To, Reference, Explanation clearly separated
- [x] Icons for each section (optional)
- [x] Collapsible sections for deep content
- [x] Current page highlighted

**Implementation**:
```yaml
# mkdocs.yml
nav:
  - Home: index.md
  - Tutorials:
    - tutorials/index.md
    - Getting Started: tutorials/getting_started_hello_world.md
    - Container Basics: tutorials/container_basics_hello_container.md
    - ...
  - How-To Guides:
    - how-to/index.md
    - ...
````

______________________________________________________________________

### 11. Add Version Warning Banner

**Description**: Show banner indicating documentation version and stability.

**Acceptance Criteria**:

- [ ] Banner visible on all pages
- [ ] Indicates pre-1.0 status
- [ ] Links to changelog

**Implementation**:

```yaml
# mkdocs.yml
extra:
  version:
    provider: mike
    default: latest
```

Or use admonition at top of index.md:

```markdown
!!! warning "Pre-release Documentation"
    This documentation is for SLURM SDK 0.x. APIs may change before 1.0 release.
```

______________________________________________________________________

## Nice-to-Haves (Future)

### 12. API Playground / Interactive Examples

**Description**: Allow users to experiment with API in browser.

**Acceptance Criteria**:

- [ ] Embedded Python REPL or Jupyter-like interface
- [ ] Pre-loaded with SLURM SDK imports
- [ ] Works with local backend only (no cluster needed)

**Notes**: High effort, consider after 1.0. Could use PyScript or similar.

______________________________________________________________________

### 13. Case Studies Section

**Description**: Document real-world usage stories.

**Acceptance Criteria**:

- [ ] At least 2 case studies from actual users
- [ ] Includes problem, solution, results
- [ ] Optional: quotes from users

**Notes**: Requires user participation. Add when community grows.

______________________________________________________________________

### 14. Video Tutorials

**Description**: Create video walkthroughs for complex topics.

**Acceptance Criteria**:

- [ ] Getting started video (5-10 min)
- [ ] Workflow visualization demo
- [ ] Hosted on YouTube with embeds in docs

**Notes**: High effort, optional.

______________________________________________________________________

### 15. Changelog in Docs

**Description**: Surface CHANGELOG.md content in documentation.

**Acceptance Criteria**:

- [x] Changelog accessible from docs navigation
- [x] Formatted consistently with rest of docs
- [ ] Links to relevant GitHub releases

**Implementation**: Either symlink or include CHANGELOG.md in docs build.

______________________________________________________________________

### 16. Community Section

**Description**: Add dedicated community resources page.

**Acceptance Criteria**:

- [ ] Link to GitHub Discussions (or create one)
- [x] Link to contributing guide
- [ ] Optional: Discord/Slack community link
- [ ] Code of conduct

**Location**: `docs/community/`

______________________________________________________________________

## Implementation Checklist

### Phase 1: Quick Wins (Week 1-2)

- [x] Enable dark mode in mkdocs.yml
- [x] Enable code copy buttons
- [x] Enable search plugin
- [x] Add ToC navigation
- [x] Link CONTRIBUTING.md

### Phase 2: Core Improvements (Month 1-2)

- [x] Create new landing page
- [x] Add SLURM concepts explainer
- [ ] Add at least one integration guide (MLflow) *(skipped)*
- [x] Improve navigation structure

### Phase 3: Content Expansion (Month 2-3)

- [ ] Add multi-GPU training tutorial
- [ ] Add hyperparameter sweep tutorial
- [ ] Add W&B integration guide
- [ ] Add version warning banner
- [ ] Surface changelog in docs

### Phase 4: Polish (Month 3+)

- [ ] Custom branding/colors
- [ ] Custom logo
- [ ] Case studies (when available)
- [ ] Community section

______________________________________________________________________

## Success Metrics

| Metric              | Current        | Target   | How to Measure       |
| ------------------- | -------------- | -------- | -------------------- |
| Page count          | ~25            | 40+      | Count pages in nav   |
| Search available    | ~~No~~ **Yes** | Yes      | Feature enabled      |
| Dark mode           | ~~No~~ **Yes** | Yes      | Feature enabled      |
| Time to first job   | Unknown        | \<15 min | User testing         |
| Tutorial completion | Unknown        | >80%     | Analytics (if added) |

______________________________________________________________________

## Dependencies

- MkDocs Material theme (already in use or easy to add)
- mkdocstrings plugin (for API reference)
- No external services required for Phase 1-2

______________________________________________________________________

## Risks & Mitigations

| Risk                        | Mitigation                                           |
| --------------------------- | ---------------------------------------------------- |
| Scope creep                 | Stick to phased approach, ship quick wins first      |
| Content maintenance burden  | Focus on evergreen content, automate API docs        |
| Breaking changes before 1.0 | Version banner warns users, changelog tracks changes |

______________________________________________________________________

## Notes

This plan prioritizes developer experience improvements that can be implemented quickly (dark mode, search, copy buttons) while building toward a more comprehensive documentation site. The phased approach allows for iteration based on user feedback.
