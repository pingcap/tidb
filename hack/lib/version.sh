#!/bin/bash
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

# -----------------------------------------------------------------------------
# Version management helpers.  These functions help to set, save and load the
# following variables:
#
#    TIDB_GIT_COMMIT - The git commit id corresponding to this
#          source code.
#    TIDB_GIT_TREE_STATE - "clean" indicates no changes since the git commit id
#        "dirty" indicates source code changes after the git commit id
#    TIDB_GIT_VERSION - "vX.Y" used to indicate the last release version.

# Grovels through git to set a set of env variables.
#
# If TIDB_GIT_VERSION_FILE, this function will load from that file instead of
# querying git.
tidb::version::get_version_vars() {
  local git=(git --work-tree "${TIDB_ROOT}")

  if [[ -n ${TIDB_GIT_COMMIT-} ]] || TIDB_GIT_COMMIT=$("${git[@]}" rev-parse "HEAD^{commit}" 2>/dev/null); then
    if [[ -z ${TIDB_GIT_TREE_STATE-} ]]; then
      # Check if the tree is dirty.  default to dirty
      if git_status=$("${git[@]}" status --porcelain 2>/dev/null) && [[ -z ${git_status} ]]; then
        TIDB_GIT_TREE_STATE="clean"
      else
        TIDB_GIT_TREE_STATE="dirty"
      fi
    fi

    # Use git describe to find the version based on annotated tags.
    if [[ -n ${TIDB_GIT_VERSION-} ]] || TIDB_GIT_VERSION=$("${git[@]}" describe --tags --abbrev=14 "${TIDB_GIT_COMMIT}^{commit}" 2>/dev/null); then
      # This translates the "git describe" to an actual semver.org
      # compatible semantic version that looks something like this:
      #   v1.1.0-alpha.0.6+84c76d1142ea4d
      #
      # TODO: We continue calling this "git version" because so many
      # downstream consumers are expecting it there.
      DASHES_IN_VERSION=$(echo "${TIDB_GIT_VERSION}" | sed "s/[^-]//g")
      if [[ "${DASHES_IN_VERSION}" == "---" ]] ; then
        # We have distance to subversion (v1.1.0-subversion-1-gCommitHash)
        TIDB_GIT_VERSION=$(echo "${TIDB_GIT_VERSION}" | sed "s/-\([0-9]\{1,\}\)-g\([0-9a-f]\{14\}\)$/.\1\+\2/")
      elif [[ "${DASHES_IN_VERSION}" == "--" ]] ; then
        # We have distance to base tag (v1.1.0-1-gCommitHash)
        TIDB_GIT_VERSION=$(echo "${TIDB_GIT_VERSION}" | sed "s/-g\([0-9a-f]\{14\}\)$/+\1/")
      fi
      if [[ "${TIDB_GIT_TREE_STATE}" == "dirty" ]]; then
        # git describe --dirty only considers changes to existing files, but
        # that is problematic since new untracked .go files affect the build,
        # so use our idea of "dirty" from git status instead.
        TIDB_GIT_VERSION+="-dirty"
      fi
    fi
  fi
}

# Saves the environment flags to $1
tidb::version::save_version_vars() {
  local version_file=${1-}
  [[ -n ${version_file} ]] || {

      echo "!!! Internal error.  No file specified in tidb::version::save_version_vars"
    return 1
  }

  cat <<EOF >"${version_file}"
TIDB_GIT_COMMIT='${TIDB_GIT_COMMIT-}'
TIDB_GIT_TREE_STATE='${TIDB_GIT_TREE_STATE-}'
TIDB_GIT_VERSION='${TIDB_GIT_VERSION-}'
TIDB_GIT_MAJOR='${TIDB_GIT_MAJOR-}'
TIDB_GIT_MINOR='${TIDB_GIT_MINOR-}'
EOF
}

# Loads up the version variables from file $1
tidb::version::load_version_vars() {
  local version_file=${1-}
  [[ -n ${version_file} ]] || {
    echo "!!! Internal error.  No file specified in tidb::version::load_version_vars"
    return 1
  }

  source "${version_file}"
}

tidb::version::ldflag() {
  local key=${1}
  local val=${2}

  # If you update these, also update the list pkg/version/def.bzl.
  echo "-X ${TIDB_GO_PACKAGE}/version.${key}=${val}"
}

# Prints the value that needs to be passed to the -ldflags parameter of go build
# in order to set the TiDB based on the git tree status.
# IMPORTANT: if you update any of these, also update the lists in
# pkg/version/def.bzl and hack/print-workspace-status.sh.
tidb::version::ldflags() {
  tidb::version::get_version_vars

  local -a ldflags=($(tidb::version::ldflag "buildDate" "$(date -u +'%Y-%m-%dT%H:%M:%SZ')"))
  if [[ -n ${TIDB_GIT_COMMIT-} ]]; then
    ldflags+=($(tidb::version::ldflag "gitCommit" "${TIDB_GIT_COMMIT}"))
    ldflags+=($(tidb::version::ldflag "gitTreeState" "${TIDB_GIT_TREE_STATE}"))
  fi

  if [[ -n ${TIDB_GIT_VERSION-} ]]; then
    ldflags+=($(tidb::version::ldflag "gitVersion" "${TIDB_GIT_VERSION}"))
  fi

  # The -ldflags parameter takes a single string, so join the output.
  echo "${ldflags[*]-}"
}
