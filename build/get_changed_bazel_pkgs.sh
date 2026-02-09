#!/usr/bin/env bash
#
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

detect_base_ref() {
  local script_dir
  script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
  "$script_dir/detect_base_branch.sh"
}

collect_changed_files() {
  local base_ref="$1"

  # Include committed, staged, unstaged, and untracked changes so
  # "run before commit" can still lint pending local edits.
  {
    git diff --name-only "$base_ref...HEAD"
    git diff --name-only --cached
    git diff --name-only
    git ls-files --others --exclude-standard
  } | sed '/^[[:space:]]*$/d' | sort -u
}

is_high_impact_change() {
  local path="$1"
  # Changes to Bazel dependency/config wiring should lint the whole repo.
  case "$path" in
    DEPS.bzl | WORKSPACE | WORKSPACE.bazel | build/BUILD.bazel)
      return 0
      ;;
    build/linter/* | *.bzl | BUILD | BUILD.bazel | */BUILD | */BUILD.bazel)
      return 0
      ;;
  esac
  return 1
}

map_file_to_bazel_target() {
  local path="$1"
  local d

  # Walk up by path even when the file no longer exists (e.g. deletions).
  d="$(dirname -- "$path")"
  while :; do
    if [ "$d" = "." ]; then
      if [ -f BUILD ] || [ -f BUILD.bazel ]; then
        printf '//:all\n'
        return 0
      fi
      return 1
    fi

    if [ -f "$d/BUILD" ] || [ -f "$d/BUILD.bazel" ]; then
      printf '//%s:all\n' "$d"
      return 0
    fi

    if [ "$d" = "/" ]; then
      return 1
    fi
    d="$(dirname -- "$d")"
  done
}

get_changed_pkgs() {
  local BASE_REF="${1:-}"
  local repo_root
  local f target
  local -a targets=()
  local need_full_lint=0
  local first_high_impact=""

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

  while IFS= read -r f; do
    [ -n "$f" ] || continue

    if is_high_impact_change "$f"; then
      need_full_lint=1
      if [ -z "$first_high_impact" ]; then
        first_high_impact="$f"
      fi
      continue
    fi

    target="$(map_file_to_bazel_target "$f" || true)"
    if [ -n "$target" ]; then
      targets+=("$target")
    fi
  done < <(collect_changed_files "$BASE_REF")

  if [ "$need_full_lint" -eq 1 ]; then
    echo "INFO: fallback to //... due to high-impact change: $first_high_impact" >&2
    printf '//...\n'
    return 0
  fi

  if [ "${#targets[@]}" -eq 0 ]; then
    return 0
  fi

  printf '%s\n' "${targets[@]}" | sort -u
}

get_changed_pkgs "${1:-}"
