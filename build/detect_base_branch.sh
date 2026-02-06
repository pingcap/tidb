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

find_base_branch() {
  local best_branch=""
  local best_score="-1"
  local ref branch merge_base score

  while IFS= read -r ref; do
    [ -n "$ref" ] || continue

    branch="${ref#refs/remotes/}"
    merge_base="$(git merge-base HEAD "$branch" 2>/dev/null || true)"
    [ -n "$merge_base" ] || continue

    score="$(git show -s --format=%ct "$merge_base" 2>/dev/null || true)"
    [[ "$score" =~ ^[0-9]+$ ]] || continue

    if [ -z "$best_branch" ] ||
      [ "$score" -gt "$best_score" ] ||
      { [ "$score" -eq "$best_score" ] && [[ "$branch" < "$best_branch" ]]; }; then
      best_branch="$branch"
      best_score="$score"
    fi
  done < <(
    git for-each-ref --format='%(refname)' \
      refs/remotes/origin/master \
      refs/remotes/origin/release-* \
      refs/remotes/origin/feature/* |
      sort -u
  )

  if [ -z "$best_branch" ]; then
    echo "ERROR: failed to detect base branch from origin/master, origin/release-*, origin/feature/*" >&2
    return 1
  fi

  echo "$best_branch"
}

find_base_branch
