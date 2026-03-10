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

find_pingcap_remotes() {
  local remote url type

  # Only consider remotes that point to the official pingcap/tidb repository.
  git remote -v |
    while read -r remote url type; do
      [ "$type" = "(fetch)" ] || continue
      case "$url" in
        https://github.com/pingcap/tidb | \
          https://github.com/pingcap/tidb.git | \
          git@github.com:pingcap/tidb | \
          git@github.com:pingcap/tidb.git | \
          ssh://git@github.com/pingcap/tidb | \
          ssh://git@github.com/pingcap/tidb.git)
          printf '%s\n' "$remote"
          ;;
      esac
    done | sort -u
}

find_base_branch() {
  local best_branch=""
  local best_score="-1"
  local ref branch merge_base score remote
  local -a candidate_patterns=()

  while IFS= read -r remote; do
    [ -n "$remote" ] || continue
    candidate_patterns+=(
      "refs/remotes/$remote/master"
      "refs/remotes/$remote/release-*"
      "refs/remotes/$remote/feature/*"
    )
  done < <(find_pingcap_remotes)

  if [ "${#candidate_patterns[@]}" -eq 0 ]; then
    echo "ERROR: failed to find remote that points to github.com/pingcap/tidb" >&2
    return 1
  fi

  while IFS= read -r ref; do
    [ -n "$ref" ] || continue

    branch="${ref#refs/remotes/}"
    merge_base="$(git merge-base HEAD "$branch" 2>/dev/null || true)"
    [ -n "$merge_base" ] || continue

    # Prefer the candidate whose merge-base with HEAD is the newest.
    score="$(git show -s --format=%ct "$merge_base" 2>/dev/null || true)"
    [[ "$score" =~ ^[0-9]+$ ]] || continue

    if [ -z "$best_branch" ] ||
      [ "$score" -gt "$best_score" ] ||
      { [ "$score" -eq "$best_score" ] && [[ "$branch" < "$best_branch" ]]; }; then
      best_branch="$branch"
      best_score="$score"
    fi
  done < <(
    git for-each-ref --format='%(refname)' "${candidate_patterns[@]}" |
      sort -u
  )

  if [ -z "$best_branch" ]; then
    echo "ERROR: failed to detect base branch from <pingcap-remote>/master, <pingcap-remote>/release-*, <pingcap-remote>/feature/*" >&2
    return 1
  fi

  echo "$best_branch"
}

find_base_branch
