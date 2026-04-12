Start a new feature branch and open a draft PR. Feature description: $ARGUMENTS

## Your workflow

### 1. Branch
```bash
git checkout main && git pull origin main
git checkout -b feat/<kebab-slug-derived-from-description>
git push -u origin feat/<slug>
```

### 2. Implement
Work through the changes. Ask for clarification if the scope is ambiguous before writing code.

### 3. Commit (Conventional Commits — required)
Every commit must follow this format:
```
feat(<scope>): <concise imperative description>

<optional body: what changed and why>
```
- **scope** is optional but encouraged: `python`, `cache`, `pool`, `docs`, `ci`
- Use `feat!:` (with exclamation) for breaking changes — this triggers a major version bump
- Keep the subject line under 72 characters

### 4. Open a PR immediately after the first push
Use the GitHub MCP tools (`mcp__github__create_pull_request`) with:
- **title**: `feat: <description>` — matching the primary commit subject
- **base**: `main`
- **body**: structured as below

```markdown
## Summary
<2-4 bullet points describing what this PR does and why>

## Changes
<bulleted list of files/components touched>

## Test plan
- [ ] <specific thing to verify>
- [ ] <another check>
```

### 5. Keep the PR description current
After every subsequent push to this branch, call `mcp__github__update_pull_request` to rewrite the PR body so it reflects **all** changes accumulated so far — not just the latest commit. The description should always give a complete, current picture of the PR.
