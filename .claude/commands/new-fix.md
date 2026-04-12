Start a new bug-fix branch and open a draft PR. Fix description: $ARGUMENTS

## Your workflow

### 1. Branch
```bash
git checkout main && git pull origin main
git checkout -b fix/<kebab-slug-derived-from-description>
git push -u origin fix/<slug>
```

### 2. Diagnose before fixing
Read the relevant code, reproduce the problem in your head, confirm the root cause before writing any fix.

### 3. Commit (Conventional Commits — required)
Every commit must follow this format:
```
fix(<scope>): <concise imperative description>

<optional body: what was broken and how this fixes it>
```
- **scope** is optional but encouraged: `python`, `cache`, `pool`, `docs`, `ci`
- Use `fix!:` (with exclamation) for breaking changes — triggers a major version bump
- Keep the subject line under 72 characters

### 4. Open a PR immediately after the first push
Use the GitHub MCP tools (`mcp__github__create_pull_request`) with:
- **title**: `fix: <description>` — matching the primary commit subject
- **base**: `main`
- **body**: structured as below

```markdown
## Summary
<What was broken, root cause, and how this fixes it>

## Changes
<bulleted list of files/components touched>

## Test plan
- [ ] <confirm the bug no longer occurs>
- [ ] <regression check — ensure nothing adjacent broke>
```

### 5. Keep the PR description current
After every subsequent push to this branch, call `mcp__github__update_pull_request` to rewrite the PR body so it reflects **all** changes accumulated so far — not just the latest commit. The description should always give a complete, current picture of the PR.
