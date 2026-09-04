# AI-DLC state

- Workspace: brownfield TiDB Go/Rust parity worktree
- Stage: CONSTRUCTION / Build and Test (complete for the current bounded work unit)
- Work unit: `pkg/util/dbterror` standard-message precedence parity batch
- Go oracle: fetched `origin/master` (`fc7788ff517c3407dc7e000be989ab23e6648211`)
- Rust target: dedicated worktree branch `codex/hparser-parity-latest`
- User approval: execution requested directly; no interactive approval pause
- Validation: the focused catalogue-precedence regression, serialized
  `tidb-error` owner profile (8 unit + 31 integration tests), compilation,
  formatting, diff, and strict clippy all pass. The prior write-conflict
  profile retains 1,030 passes and 136 pre-existing planner/remote/spill/
  fixture failures; parser #11 has no safe Rust-only code fix without changing
  the public byte/input API.
- Prior commit/push: JSON separator batch `242d294f2c` is pushed to
  `hparser-integration`.
- Commit/push: JSON merge batch `71ffce262e` is pushed to
  `hparser-integration`.
- Commit/push: `pkg/kv` retry-marker batch is validated and the receipt is
  included in the final pushed change.
- Commit/push: `pkg/util/dbterror` precedence batch is validated and pushed as
  `3c1119e3b6` to `hparser-integration`; the later state-only oracle update is
  `8552e1a508`.
- Next action: continue with the next executable package boundary.
