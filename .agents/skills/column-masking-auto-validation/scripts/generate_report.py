#!/usr/bin/env python3
"""Generate a human-readable column masking validation report from artifacts."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

STATUS_ORDER = ["PASS", "FAIL", "PARTIAL", "NOT_COVERED", "N/A"]


@dataclass
class StepResult:
    name: str
    rc: int
    cmd: str


def read_steps(steps_file: Path) -> List[StepResult]:
    if not steps_file.exists():
        return []
    rows: List[StepResult] = []
    for line in steps_file.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue
        name, rc_raw, cmd = parts
        try:
            rc = int(rc_raw)
        except ValueError:
            rc = 1
        rows.append(StepResult(name=name, rc=rc, cmd=cmd))
    return rows


def steps_to_map(steps: List[StepResult]) -> Dict[str, StepResult]:
    return {s.name: s for s in steps}


def all_steps_ok(step_names: List[str], step_map: Dict[str, StepResult]) -> Tuple[bool, List[str]]:
    evidence: List[str] = []
    ok = True
    for step in step_names:
        if step not in step_map:
            ok = False
            evidence.append(f"step `{step}` is missing in steps.tsv")
            continue
        rc = step_map[step].rc
        if rc != 0:
            ok = False
            evidence.append(f"step `{step}` failed with exit code {rc}")
        else:
            evidence.append(f"step `{step}` passed")
    return ok, evidence


def evaluate_rule(
    rule: dict,
    step_map: Dict[str, StepResult],
    result_text: str,
) -> Tuple[str, List[str]]:
    rule_type = rule.get("type")
    if rule_type == "fixed":
        status = rule.get("status", "NOT_COVERED")
        evidence = [rule.get("evidence", "status is fixed by scenario policy")]
        return status, evidence

    if rule_type == "steps_pass":
        steps = rule.get("steps", [])
        ok, evidence = all_steps_ok(steps, step_map)
        return ("PASS" if ok else "FAIL"), evidence

    if rule_type == "fail_if_result_contains":
        required_steps = rule.get("required_steps", [])
        required_ok, required_ev = all_steps_ok(required_steps, step_map)
        patterns = rule.get("patterns", [])
        matched = [p for p in patterns if p in result_text]
        if not required_ok:
            return "FAIL", required_ev + ["required step precondition failed"]
        if matched:
            fail_status = rule.get("fail_status", "FAIL")
            ev = required_ev + [f"matched failure signal ({len(matched)} pattern(s))"]
            return fail_status, ev
        pass_status = rule.get("pass_status", "PASS")
        ev = required_ev + ["no failure signal pattern matched"]
        return pass_status, ev

    return "FAIL", [f"unknown rule type `{rule_type}`"]


def render_command_table(steps: List[StepResult]) -> str:
    if not steps:
        return "No command execution record found."
    lines = [
        "| Step | Exit Code | Command |",
        "| --- | ---: | --- |",
    ]
    for s in steps:
        lines.append(f"| `{s.name}` | `{s.rc}` | `{s.cmd}` |")
    return "\n".join(lines)


def render_scenario_blocks(scenarios: List[dict]) -> str:
    blocks: List[str] = []
    for s in scenarios:
        blocks.append(f"### {s['id']}: {s['scenario']}")
        blocks.append(f"- Status: **{s['status']}**")
        blocks.append("Coverage cases:")
        cases = s.get("coverage_cases", [])
        if not cases:
            blocks.append("- (none)")
        else:
            for c in cases:
                blocks.append(f"- {c['name']} (`{c['file']}`)")
        blocks.append("Evidence:")
        for ev in s.get("evidence", []):
            blocks.append(f"- {ev}")
        blocks.append("")
    return "\n".join(blocks).strip()


def render_open_items(scenarios: List[dict]) -> str:
    open_items = [s for s in scenarios if s["status"] in {"FAIL", "PARTIAL", "NOT_COVERED"}]
    if not open_items:
        return "- None"
    lines: List[str] = []
    for s in open_items:
        lines.append(f"- {s['id']} ({s['status']}): {s['scenario']}")
    return "\n".join(lines)


def render_summary_block(scenarios: List[dict]) -> str:
    counts = {k: 0 for k in STATUS_ORDER}
    for s in scenarios:
        counts[s["status"]] = counts.get(s["status"], 0) + 1
    lines = [f"- Total scenarios: {len(scenarios)}"]
    for k in STATUS_ORDER:
        lines.append(f"- {k}: {counts.get(k, 0)}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate column masking auto-validation report.")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--artifacts-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--matrix-file")
    parser.add_argument("--template-file")
    parser.add_argument("--result-file")
    args = parser.parse_args()

    script_path = Path(__file__).resolve()
    skill_root = script_path.parent.parent
    repo_root = Path(args.repo_root).resolve()
    artifacts_dir = Path(args.artifacts_dir).resolve()

    if args.matrix_file:
        matrix_files = [Path(args.matrix_file).resolve()]
    else:
        matrix_files = [
            skill_root / "references" / "p0-scenario-matrix.json",
            skill_root / "references" / "p1-scenario-matrix.json",
        ]
    template_file = Path(args.template_file).resolve() if args.template_file else skill_root / "references" / "report-template.md"
    result_file = (
        Path(args.result_file).resolve()
        if args.result_file
        else repo_root / "tests" / "integrationtest" / "r" / "privilege" / "column_masking_policy.result"
    )
    output_file = Path(args.output).resolve()

    steps_file = artifacts_dir / "steps.tsv"
    steps = read_steps(steps_file)
    step_map = steps_to_map(steps)

    result_text = ""
    if result_file.exists():
        result_text = result_file.read_text(encoding="utf-8")

    matrix: List[dict] = []
    for mf in matrix_files:
        if not mf.exists():
            continue
        rows = json.loads(mf.read_text(encoding="utf-8"))
        if isinstance(rows, list):
            matrix.extend(rows)
    evaluated: List[dict] = []
    for item in matrix:
        status, evidence = evaluate_rule(item.get("rule", {}), step_map, result_text)
        row = dict(item)
        row["status"] = status
        row["evidence"] = evidence
        evaluated.append(row)

    template = template_file.read_text(encoding="utf-8")
    rendered = (
        template.replace("{{GENERATED_AT}}", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))
        .replace("{{REPO_ROOT}}", str(repo_root))
        .replace("{{ARTIFACT_DIR}}", str(artifacts_dir))
        .replace("{{SUMMARY_BLOCK}}", render_summary_block(evaluated))
        .replace("{{SCENARIO_BLOCKS}}", render_scenario_blocks(evaluated))
        .replace("{{COMMAND_TABLE}}", render_command_table(steps))
        .replace("{{OPEN_ITEMS_BLOCK}}", render_open_items(evaluated))
    )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(rendered, encoding="utf-8")
    print(output_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
