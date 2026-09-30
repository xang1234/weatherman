# Agent Instructions

Issues are tracked in GitHub Issues: https://github.com/xang1234/weatherman/issues

## Quick Reference

```bash
gh issue list                         # Open issues
gh issue view <n>                     # Issue details (sub-issues, blocked-by shown on GitHub)
gh issue create --title "..." --label P2,task
gh issue close <n> --comment "..."    # Complete work
```

- **Priority labels**: P0=critical, P1=high, P2=medium, P3=low
- **Type labels**: feature, task; `routing` groups the weather routing work
- Multi-step work uses sub-issues; ordering uses GitHub "blocked by" relationships
- Pre-migration history (beads, 326 issues): `docs/archive/beads-issues.jsonl`

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, comment on in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
