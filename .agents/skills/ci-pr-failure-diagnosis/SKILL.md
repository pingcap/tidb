---
name: ci-pr-failure-diagnosis
description: "Diagnose CI job failures for a GitHub pull request (explicit PR or PR for the current branch). Supports GitHub Actions, Jenkins pipelines, and Kubernetes Prow jobs, with safe log retrieval and slicing to avoid loading full logs into context."
---

# CI PR Failure Diagnosis

## Overview
Diagnose failing CI checks for a GitHub PR by identifying the failing checks, downloading logs locally, and inspecting only small slices of those logs.

## Workflow Guardrails
- Do not paste full CI logs into the chat. Always download logs to a local file and inspect them in slices.
- Prefer the bundled script `.agents/skills/ci-pr-failure-diagnosis/scripts/log_peek.py` for log slicing.
- Prefer built-in CLIs (`gh`, `curl`) and repo conventions. Use ad-hoc shell pipelines only if the script cannot express the needed view.
- Check tool availability before running commands: `command -v gh`, `command -v curl`, `command -v unzip`

## Quick Workflow
1. Resolve the PR (user-provided or current branch PR).
2. List failing checks and capture URLs.
3. Identify the CI system for each failing check.
4. Download the log to a local file.
5. Use `log_peek.py` to extract only relevant slices.
6. Summarize root cause and next steps.

## Resolve the PR
- If the user provided a PR URL/number, use it directly.
- If not, infer from the current branch:

```bash
git rev-parse --abbrev-ref HEAD
gh pr view --json number,url,headRefName
# or, if needed:
gh pr list --head <branch> --json number,url
```

If no PR exists for the branch, ask the user to create one or provide a PR.

## List failing checks
Use GitHub to list checks and failing contexts:

```bash
gh pr view <PR> --json statusCheckRollup -q '
  .statusCheckRollup[]
  | select((.conclusion != "SUCCESS") and (.state != "SUCCESS"))
  | "\(.name // .context)\t\(.conclusion // .state)\t\(.detailsUrl // .targetUrl)"'
```

Interpretation:
- `detailsUrl` or `targetUrl` usually points to the CI system.
- If both are empty, use the check name and repo conventions to locate logs.

## CI-specific retrieval

### GitHub Actions
Identify by URLs like `https://github.com/<org>/<repo>/actions/runs/<run_id>`.

1. Extract the run id from the URL.
2. Download failed logs to a file:

```bash
gh run view <run_id> --log-failed > /tmp/gh-actions-<run_id>.log
```

If you need a specific job:

```bash
gh run view <run_id> --json jobs -q '.jobs[] | "\(.name)\t\(.id)\t\(.conclusion)"'
gh api /repos/<org>/<repo>/actions/jobs/<job_id>/logs > /tmp/gh-actions-job-<job_id>.log
```

Then use `log_peek.py` to inspect.

### Jenkins pipeline
Identify by URLs containing `/jenkins/` or `job/` in the CI domain.

1. Download console log:

```bash
curl -L "<job_url>/consoleText" -o /tmp/jenkins-<build>.log
```

If the URL already ends with `consoleText`, download it directly.

2. Inspect with `log_peek.py`.

### Kubernetes Prow job
Identify by URLs containing `prow.` or `/view/gs/` or `/logs/`.

Common patterns:
- `https://prow.<domain>/view/gs/.../<build>/`
- `https://prow.<domain>/logs/<job>/<build>`

Log URL patterns:
- Append `/build-log.txt` to the build URL.

Example:

```bash
curl -L "<prow_build_url>/build-log.txt" -o /tmp/prow-<build>.log
```

If the URL is a job-history page, fetch it and extract the `build-log.txt` link.

## Log slicing (required)
Use the bundled script. It reads logs line-by-line and prints only the requested slice.

```bash
.agents/skills/ci-pr-failure-diagnosis/scripts/log_peek.py /tmp/prow-123.log --tail 200
.agents/skills/ci-pr-failure-diagnosis/scripts/log_peek.py /tmp/prow-123.log --grep "ERROR|Error|panic|FAIL" --context 3
.agents/skills/ci-pr-failure-diagnosis/scripts/log_peek.py /tmp/prow-123.log --range 1200:1350
```

## Triage heuristics
- Start with the last 200 to 400 lines.
- Search for `panic`, `fatal`, `ERROR`, `FAIL`, `exit code`, `timeout`.
- Identify the first failure and work backwards to the triggering step.
- If the failure is flaky or infra-related, call it out separately from product failures.

## Output expectations
Provide:
- Which check failed and where (Actions, Jenkins, Prow).
- The minimal log slices that show the failure.
- A short suspected root cause.
- Suggested next steps (rerun, fix config, adjust test).

## Resources
### scripts/
- `scripts/log_peek.py`: Streamed log slicing with head, tail, range, and grep modes.
