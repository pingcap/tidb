---
name: tidb-pre-push-lint
description: Use before every git push to the fork in the TiDB repo. Run this after committing and before pushing to catch gofmt, gci, go vet, and revive failures that will break Bazel nogo CI.
---

# TiDB Pre-Push Lint Gate

## Overview

Bazel's nogo analyzer enforces gofmt, gci (import ordering), go vet, and revive on every build.
Pushing without running these locally causes CI failures that take ~20 minutes to surface.
Run this gate after every commit, before every push.

## Step 1 — Find Changed Packages

```bash
# Packages changed by our own commits (excludes upstream merge commits)
git diff --name-only origin/master...HEAD | grep '\.go$' | sed 's|/[^/]*$||' | sort -u
```

## Step 2 — Run All Four Checks

Run these in parallel against the changed packages identified above:

```bash
# 1. gofmt — formatting (struct alignment is the most common failure)
gofmt -l <pkg1>/ <pkg2>/ ...

# 2. gci — import ordering
gci diff <pkg1>/ <pkg2>/ ...

# 3. go vet — type errors, misuse of API (catches wrong argument types)
go vet ./<pkg1>/... ./<pkg2>/...

# 4. revive — style linter
tools/bin/revive -formatter friendly -config tools/check/revive.toml ./<pkg1>/... ./<pkg2>/...
```

**All four must produce no output before pushing.**

## Step 3 — Auto-fix and Re-check

If gofmt or gci report issues, auto-fix them:

```bash
gofmt -w <pkg>/
gci write <pkg>/
```

Then re-run Step 2 to confirm clean.

## Common Failure: Struct Field Alignment

Renaming a field to a longer name breaks column alignment in struct definitions AND all struct literals that initialize it. gofmt enforces consistent alignment.

```go
// After renaming targetPartition → targetPartitions, ALL fields need repadding:
// ❌ breaks nogo
type dbMetaMgrBuilder struct {
    db           *sql.DB
    needChecksum bool
    targetPartitions []string  // longer than the others → misaligned
}

// ✅ fix with: gofmt -w <file>
type dbMetaMgrBuilder struct {
    db               *sql.DB
    needChecksum     bool
    targetPartitions []string
}
```

**Always run `gofmt -w` on every file that defines or initializes the renamed struct.**
