# imap-clients — Development Guidelines

## Branching

| Purpose | Branch prefix | Slash command |
|---------|--------------|---------------|
| New feature | `feat/<slug>` | `/new-feat <description>` |
| Bug fix | `fix/<slug>` | `/new-fix <description>` |
| Docs only | `docs/<slug>` | manual |
| CI / tooling | `ci/<slug>` | manual |

All branches target `main`. Direct pushes to `main` are blocked — always use a PR.

## Commit Messages (Conventional Commits)

Every commit **must** follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short imperative description>

<optional body — what changed and why>

<optional footer — BREAKING CHANGE: ..., Closes #N>
```

### Types and their version impact

| Type | Description | Version bump |
|------|-------------|--------------|
| `feat` | New feature | minor |
| `fix` | Bug fix | patch |
| `perf` | Performance improvement | patch |
| `refactor` | Refactor (no behaviour change) | none |
| `docs` | Documentation only | none |
| `test` | Tests only | none |
| `chore` | Dependency / config updates | none |
| `ci` | CI/CD changes | none |
| `style` | Formatting, whitespace | none |

Append `!` after the type (`feat!:`, `fix!:`) **or** add `BREAKING CHANGE:` in the footer to trigger a **major** version bump.

### Scopes (optional but encouraged)

`python` · `cache` · `pool` · `docs` · `ci` · `deps`

### Examples

```
feat(python): add OAuth2 authentication support
fix(pool): prevent connection leak on throttle timeout  
perf(cache): switch to WAL mode for concurrent reads
docs: add SQLite query reference
ci: configure semantic-release for automated versioning
refactor(python): extract retry logic into RetryPolicy class
feat!: remove deprecated --verbose flag
```

## Versioning

Versions are managed automatically by **python-semantic-release** using a two-stage approach that avoids any post-merge commit on `main`:

| Stage | Workflow | What happens |
|-------|----------|--------------|
| PR open / update | `version-bump.yml` | Calculates next version, updates `pyproject.toml` + `python/imap_cli.py` + `CHANGELOG.md`, commits back to the PR branch |
| Push to `main` | `release.yml` | Detects version files are already correct (no diff), **tags the merge commit**, creates GitHub Release — no new commit |

- Tag format: `v<major>.<minor>.<patch>`
- PRs with only `chore`/`docs`/`style`/`test`/`ci` commits produce no bump; the version-bump workflow is a no-op

Do **not** manually edit the version strings — let the workflows handle it.

## PR Workflow

1. Run `/new-feat` or `/new-fix` — Claude creates the branch and opens the PR
2. Commit using conventional format above
3. `version-bump.yml` automatically commits the version bump + CHANGELOG to your branch
4. Claude updates the PR description after each push to reflect cumulative changes
5. Merge via GitHub UI (squash or merge commit — do not rebase)
