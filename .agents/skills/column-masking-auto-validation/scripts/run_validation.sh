#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${SKILL_ROOT}/../../.." && pwd)"

WITH_BAZEL_PREPARE=0
WITH_LINT=0
SKIP_TESTS=0
ARTIFACTS_DIR=""

usage() {
  cat <<'EOF'
Usage: run_validation.sh [options]

Options:
  --with-bazel-prepare     Run make bazel_prepare before tests
  --with-lint              Run make bazel_lint_changed after tests
  --skip-tests             Do not run tests; only generate report from artifacts
  --artifacts-dir <dir>    Use a specific artifacts directory
  -h, --help               Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-bazel-prepare)
      WITH_BAZEL_PREPARE=1
      shift
      ;;
    --with-lint)
      WITH_LINT=1
      shift
      ;;
    --skip-tests)
      SKIP_TESTS=1
      shift
      ;;
    --artifacts-dir)
      ARTIFACTS_DIR="${2:-}"
      if [[ -z "${ARTIFACTS_DIR}" ]]; then
        echo "--artifacts-dir requires a path" >&2
        exit 1
      fi
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${ARTIFACTS_DIR}" ]]; then
  ts="$(date '+%Y%m%d-%H%M%S')"
  ARTIFACTS_DIR="${REPO_ROOT}/artifacts/column-masking/${ts}"
fi

mkdir -p "${ARTIFACTS_DIR}/logs"
STEPS_FILE="${ARTIFACTS_DIR}/steps.tsv"
REPORT_FILE="${ARTIFACTS_DIR}/column-masking-report.md"

LAST_RC=0
run_step() {
  local step_name="$1"
  shift
  local cmd="$*"
  local log_file="${ARTIFACTS_DIR}/logs/${step_name}.log"

  echo ">>> [${step_name}] ${cmd}"
  set +e
  bash -lc "cd '${REPO_ROOT}' && ${cmd}" 2>&1 | tee "${log_file}"
  LAST_RC=${PIPESTATUS[0]}
  set -e
  printf "%s\t%s\t%s\n" "${step_name}" "${LAST_RC}" "${cmd}" >> "${STEPS_FILE}"
}

if [[ "${SKIP_TESTS}" -eq 0 ]]; then
  : > "${STEPS_FILE}"

  if [[ "${WITH_BAZEL_PREPARE}" -eq 1 ]]; then
    run_step "bazel_prepare" "make bazel_prepare"
  fi

  run_step "ut_failpoint_enable" "make failpoint-enable"
  run_step "ut_ddl" "go test -run 'TestMaskingPolicy' -tags=intest,deadlock ./pkg/ddl"
  run_step "ut_meta" "go test -run 'TestMaskingPolicy' -tags=intest,deadlock ./pkg/meta"
  run_step "ut_planner" "go test -run 'TestMaskingPolicy' -tags=intest,deadlock ./pkg/planner/core"
  run_step "ut_executor_show" "go test -run 'TestShowMaskingPolicies' -tags=intest,deadlock ./pkg/executor"
  run_step "ut_failpoint_disable" "make failpoint-disable"

  run_step "it_column_masking" "pushd tests/integrationtest >/dev/null && ./run-tests.sh -r privilege/column_masking_policy && popd >/dev/null"
  run_step "it_expression_builtin" "pushd tests/integrationtest >/dev/null && ./run-tests.sh -r expression/builtin && popd >/dev/null"

  if [[ "${WITH_LINT}" -eq 1 ]]; then
    run_step "bazel_lint_changed" "make bazel_lint_changed"
  fi
else
  touch "${STEPS_FILE}"
  if [[ ! -s "${STEPS_FILE}" ]]; then
    echo "No test execution requested and steps file is empty: ${STEPS_FILE}" >&2
    echo "Provide --artifacts-dir pointing to a previous run, or run without --skip-tests." >&2
    exit 1
  fi
fi

python3 "${SCRIPT_DIR}/generate_report.py" \
  --repo-root "${REPO_ROOT}" \
  --artifacts-dir "${ARTIFACTS_DIR}" \
  --output "${REPORT_FILE}"

echo "Report generated: ${REPORT_FILE}"
echo "Artifacts: ${ARTIFACTS_DIR}"
