#!/usr/bin/env bash
set -euo pipefail

detect_base_ref() {
  local script_dir
  script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
  "$script_dir/detect_base_branch.sh"
}

get_changed_pkgs() {
  local BASE_REF="${1:-}"
  local repo_root

  if [ -z "$BASE_REF" ]; then
    BASE_REF="$(detect_base_ref)" || {
      echo "ERROR: failed to detect base ref" >&2
      return 2
    }
    echo "INFO: auto-detected base ref: $BASE_REF" >&2
  fi

  repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || return 1
  cd -- "$repo_root"

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

get_changed_pkgs "${1:-}"
