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

Versions are managed automatically by **python-semantic-release** on every push to `main`:

- Commits since the last tag are analysed
- Version in `pyproject.toml` and `python/imap_cli.py` is bumped accordingly
- A GitHub Release + CHANGELOG entry is generated
- Tag format: `v<major>.<minor>.<patch>`

Do **not** manually edit the version strings — let the release workflow handle it.

## PR Workflow

1. Run `/new-feat` or `/new-fix` — Claude creates the branch and opens the PR
2. Commit using conventional format above
3. Claude updates the PR description after each push to reflect cumulative changes
4. Merge via GitHub UI (squash or merge commit — do not rebase)
