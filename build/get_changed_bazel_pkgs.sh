#!/usr/bin/env bash
set -euo pipefail

get_changed_pkgs() {
  BASE_REF="${1:-origin/HEAD}"
  repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || return 1
  git rev-parse --verify -q "$BASE_REF" >/dev/null 2>&1 || {
    echo "ERROR: base ref not found: $BASE_REF" >&2
    return 2
  }
  git diff --name-only "$BASE_REF...HEAD" |
  while IFS= read -r f; do
    [ -e "$f" ] || continue
    d=$(dirname -- "$f")
    while [ "$d" != "." ] && [ "$d" != "/" ]; do
      if [ -f "$d/BUILD" ] || [ -f "$d/BUILD.bazel" ]; then
        case "$d" in
          "$repo_root") rel="";;
          "$repo_root"/*) rel="${d#$repo_root/}";;
          *) rel="$d";;
        esac
        if [ -n "$rel" ]; then
          printf '//%s:all\n' "$rel"
        else
          printf '//:all\n'
        fi
        break
      fi
      d=$(dirname -- "$d")
    done
  done | sort -u
}

get_changed_pkgs "${1:-origin/HEAD}"

