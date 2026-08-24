#!/usr/bin/env bash
#
# Syncs the vendored `rust/third_party/tikv-client-rs/` copy of
# `https://github.com/ngaut/client-rust` (master branch) to the current
# upstream tip, then reapplies this workspace's maintained patch set.
#
# Why this exists: `rust/crates/tidb-pd-client`, `rust/crates/tidb-txnkv`, and
# `rust/crates/tidb-distsql` depend on `ngaut/client-rust` the way Go TiDB
# depends on `github.com/tikv/client-go/v2`, instead of hand-transcreating PD
# and TiKV client protocol logic. That upstream repository is actively
# maintained by a separate agent and this workspace must always build against
# its latest `master`, not a commit frozen at some earlier point -- see
# `rust/docs/client-rust-migration-execplan.md` for the full plan and the
# rationale for vendoring this way (scratch clone + rsync, not a nested git
# checkout or submodule, so the tracked copy is plain files like every other
# crate in this workspace, and `git diff`/`git blame` inside `rust/` work
# normally on it).
#
# The scratch clone lives outside version control
# (`rust/third_party/.scratch/`, gitignored) purely as a fetch/diff workspace; the
# tracked `rust/third_party/tikv-client-rs/` directory it syncs into has no nested
# `.git` and is committed as ordinary files.
#
# Patches: `rust/third_party/patches/tikv-client-rs/NNN-description.patch`,
# applied in sorted order with `git apply` against the scratch clone before
# it is copied over the tracked directory. A patch that no longer applies
# stops the script -- that means either upstream changed nearby code (rebase
# the patch) or upstream already absorbed the fix (delete the patch and
# re-verify the dependent Rust code against the new upstream shape). Never
# silently skip a failing patch.

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SCRATCH_DIR="$REPO_ROOT/third_party/.scratch/tikv-client-rs-src"
VENDOR_DIR="$REPO_ROOT/third_party/tikv-client-rs"
PATCH_DIR="$REPO_ROOT/third_party/patches/tikv-client-rs"
SYNC_LOG="$REPO_ROOT/third_party/tikv-client-rs-SYNC-LOG.md"
UPSTREAM_URL="https://github.com/ngaut/client-rust.git"

mkdir -p "$REPO_ROOT/third_party/.scratch"

if [ -d "$SCRATCH_DIR/.git" ]; then
  git -C "$SCRATCH_DIR" fetch origin master
  git -C "$SCRATCH_DIR" reset --hard origin/master
  git -C "$SCRATCH_DIR" clean -fdx
else
  rm -rf "$SCRATCH_DIR"
  git clone "$UPSTREAM_URL" "$SCRATCH_DIR"
fi

SYNCED_COMMIT=$(git -C "$SCRATCH_DIR" rev-parse HEAD)
SYNCED_DATE=$(git -C "$SCRATCH_DIR" log -1 --format='%cI')

echo "Syncing to ngaut/client-rust@${SYNCED_COMMIT} (${SYNCED_DATE})"

if [ -d "$PATCH_DIR" ]; then
  for patch in $(find "$PATCH_DIR" -maxdepth 1 -name '*.patch' | sort); do
    echo "Applying $(basename "$patch")"
    if ! git -C "$SCRATCH_DIR" apply --check "$patch" 2>/tmp/sync-tikv-client-rs-patch-check.log; then
      echo "FAILED to apply $(basename "$patch"):" >&2
      cat /tmp/sync-tikv-client-rs-patch-check.log >&2
      echo "" >&2
      echo "Either upstream changed nearby code (rebase this patch) or upstream" >&2
      echo "already absorbed the fix (delete this patch and re-verify dependent" >&2
      echo "Rust code against the new upstream shape). Not applying the remaining" >&2
      echo "patch set until this is resolved." >&2
      exit 1
    fi
    git -C "$SCRATCH_DIR" apply "$patch"
  done
fi

rm -rf "$VENDOR_DIR"
mkdir -p "$VENDOR_DIR"
(cd "$SCRATCH_DIR" && find . -mindepth 1 -maxdepth 1 ! -name '.git' -exec cp -r {} "$VENDOR_DIR/" \;)

{
  echo "- $(date -u +%Y-%m-%dT%H:%M:%SZ): synced to ngaut/client-rust@${SYNCED_COMMIT} (committed ${SYNCED_DATE}), patches: $(ls "$PATCH_DIR" 2>/dev/null | wc -l | tr -d ' ') applied"
} >> "$SYNC_LOG"

echo "Synced. Tracked copy: $VENDOR_DIR"
echo "Review with: git -C $REPO_ROOT status rust/third_party/tikv-client-rs"
