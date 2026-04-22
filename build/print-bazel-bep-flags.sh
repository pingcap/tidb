#!/bin/bash
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

set -o errexit
set -o nounset
set -o pipefail

git_commit="$(git rev-parse HEAD)"
git_branch="$(git rev-parse --abbrev-ref HEAD)"
ci_provider="local"

if [[ -n "${JENKINS_URL:-}" || -n "${BUILD_URL:-}" || -n "${JOB_NAME:-}" ]]; then
    ci_provider="jenkins"
fi

pr_id="${ghprbPullId:-${CHANGE_ID:-}}"
pr_url="${ghprbPullLink:-${CHANGE_URL:-}}"
pr_target="${ghprbTargetBranch:-${CHANGE_TARGET:-}}"
pr_source="${ghprbSourceBranch:-${CHANGE_BRANCH:-}}"
pr_commit="${ghprbActualCommit:-}"

metadata_flags=()

add_metadata() {
    local key="$1"
    local value="${2:-}"

    if [[ -n "${value}" ]]; then
        metadata_flags+=("--build_metadata=${key}=${value}")
    fi
}

add_keyword() {
    local keyword="${1:-}"

    if [[ -n "${keyword}" ]]; then
        metadata_flags+=("--bes_keywords=${keyword}")
    fi
}

add_metadata "ci.provider" "${ci_provider}"
add_metadata "ci.jenkins.url" "${JENKINS_URL:-}"
add_metadata "ci.job.name" "${JOB_NAME:-}"
add_metadata "ci.job.base_name" "${JOB_BASE_NAME:-}"
add_metadata "ci.build.number" "${BUILD_NUMBER:-}"
add_metadata "ci.build.id" "${BUILD_ID:-}"
add_metadata "ci.build.tag" "${BUILD_TAG:-}"
add_metadata "ci.build.url" "${BUILD_URL:-${RUN_DISPLAY_URL:-}}"
add_metadata "git.commit" "${GIT_COMMIT:-${git_commit}}"
add_metadata "git.branch" "${BRANCH_NAME:-${GIT_BRANCH:-${git_branch}}}"
add_metadata "git.url" "${GIT_URL:-}"
add_metadata "git.repo" "${ghprbGhRepository:-${CHANGE_FORK:-}}"
add_metadata "pr.id" "${pr_id}"
add_metadata "pr.url" "${pr_url}"
add_metadata "pr.target" "${pr_target}"
add_metadata "pr.source" "${pr_source}"
add_metadata "pr.commit" "${pr_commit}"
add_keyword "ci:${ci_provider}"
add_keyword "git.branch=${BRANCH_NAME:-${GIT_BRANCH:-${git_branch}}}"
add_keyword "git.commit=${GIT_COMMIT:-${git_commit}}"

if [[ -n "${pr_id}" ]]; then
    add_keyword "pr:${pr_id}"
fi

printf '%s ' "${metadata_flags[@]}"
echo
