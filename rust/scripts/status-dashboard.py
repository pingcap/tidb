#!/usr/bin/env python3
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

"""Render the current rewrite state from checked ledgers and manifests."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from pathlib import Path
import sys
import tomllib


ROOT = Path(__file__).resolve().parent.parent
SOURCE_LEDGER = ROOT / "difftests/corpus/coverage/go_source_inventory.tsv"
TEST_LEDGER = ROOT / "difftests/corpus/coverage/go_test_inventory.tsv"
SLICE_DIR = ROOT / "workstreams/slices"
CAMPAIGN_DIR = ROOT / "workstreams/campaigns"
CLAIM_DIR = ROOT / "workstreams/claims"
OUTPUT = ROOT / "STATUS.md"
STATUSES = ("UNTRIAGED", "PARTIAL", "COVERED", "BLOCKED")


def read_tsv(path: Path) -> list[list[str]]:
    """Read checked TSV data rows, excluding comments and empty lines."""
    return [
        line.split("\t")
        for line in path.read_text().splitlines()
        if line and not line.startswith("#")
    ]


def read_toml_dir(path: Path) -> list[dict[str, object]]:
    """Read deterministic TOML records from one manifest directory."""
    return [tomllib.loads(item.read_text()) for item in sorted(path.glob("*.toml"))]


def markdown_table(headers: tuple[str, ...], rows: list[tuple[object, ...]]) -> list[str]:
    """Render a compact GitHub Markdown table."""
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    lines.extend("| " + " | ".join(str(cell) for cell in row) + " |" for row in rows)
    return lines


def status_counts(rows: list[list[str]], status_index: int) -> Counter[str]:
    """Count the four checked ledger states."""
    return Counter(row[status_index] for row in rows)


def grouped_status_counts(
    rows: list[list[str]], group_index: int, status_index: int
) -> dict[str, Counter[str]]:
    """Count ledger states per target crate or differential ring."""
    grouped: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        grouped[row[group_index]][row[status_index]] += 1
    return grouped


def render() -> str:
    """Render the complete deterministic dashboard."""
    sources = read_tsv(SOURCE_LEDGER)
    tests = read_tsv(TEST_LEDGER)
    slices = read_toml_dir(SLICE_DIR)
    campaigns = read_toml_dir(CAMPAIGN_DIR)
    source_counts = status_counts(sources, 4)
    test_counts = status_counts(tests, 5)
    source_groups = grouped_status_counts(sources, 2, 4)
    test_groups = grouped_status_counts(tests, 4, 5)
    slice_counts = Counter(str(item["status"]) for item in slices)
    active_claims = len(list(CLAIM_DIR.glob("*.claim.json"))) if CLAIM_DIR.exists() else 0

    lines = [
        "# TiDB Rust rewrite current status",
        "",
        "This file is generated from the checked source/test ledgers and workstream",
        "manifests. It is the dispatcher hot path; historical narrative belongs in",
        "`HANDOFF.md`. Regenerate it with `python3 scripts/status-dashboard.py --write`.",
        "",
        "These are ownership states, not product-parity percentages. `PARTIAL` means",
        "the source or obligation still has explicit unported behavior.",
        "",
        "## Queue",
        "",
        f"- Active claims: {active_claims}",
        f"- Active slices: {slice_counts['active']}",
        f"- Declared ready slices: {slice_counts['ready']}",
        f"- Partial slices: {slice_counts['partial']}",
        f"- Covered slices: {slice_counts['covered']}",
        f"- Blocked slices: {slice_counts['blocked']}",
        "",
        "## Campaigns",
        "",
    ]
    campaign_rows = [
        (item["campaign"], item["status"], len(item.get("slices", [])))
        for item in campaigns
    ]
    lines.extend(markdown_table(("Campaign", "Status", "Slices"), campaign_rows))
    lines.extend(["", "## Production source ledger", ""])
    lines.extend(
        markdown_table(
            ("State", "Count"), [(status, source_counts[status]) for status in STATUSES]
        )
    )
    lines.extend(["", "### By target crate", ""])
    source_rows = [
        (
            group,
            counts["UNTRIAGED"],
            counts["PARTIAL"],
            counts["COVERED"],
            counts["BLOCKED"],
        )
        for group, counts in sorted(
            source_groups.items(),
            key=lambda item: (-(item[1]["UNTRIAGED"] + item[1]["PARTIAL"]), item[0]),
        )
    ]
    lines.extend(
        markdown_table(
            ("Target", "Untriaged", "Partial", "Covered", "Blocked"), source_rows
        )
    )
    lines.extend(["", "## Original test/support ledger", ""])
    lines.extend(
        markdown_table(
            ("State", "Count"), [(status, test_counts[status]) for status in STATUSES]
        )
    )
    lines.extend(["", "### By differential ring", ""])
    test_rows = [
        (
            group,
            counts["UNTRIAGED"],
            counts["PARTIAL"],
            counts["COVERED"],
            counts["BLOCKED"],
        )
        for group, counts in sorted(
            test_groups.items(),
            key=lambda item: (-(item[1]["UNTRIAGED"] + item[1]["PARTIAL"]), item[0]),
        )
    ]
    lines.extend(
        markdown_table(
            ("Ring", "Untriaged", "Partial", "Covered", "Blocked"), test_rows
        )
    )

    blocked = sorted(
        (str(item["slice"]), str(item["consumer"]))
        for item in slices
        if item["status"] == "blocked"
    )
    lines.extend(["", "## Blocked slices", ""])
    if blocked:
        lines.extend(f"- `{name}`: {consumer}" for name, consumer in blocked)
    else:
        lines.append("None.")
    retired = sorted(str(item["slice"]) for item in slices if item["status"] == "retired")
    lines.extend(["", "## Retired slices", ""])
    if retired:
        lines.extend(f"- `{name}`" for name in retired)
    else:
        lines.append("None.")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    """Run the dashboard writer/checker."""
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = render()
    if args.write:
        OUTPUT.write_text(rendered)
        return 0
    if args.check:
        current = OUTPUT.read_text() if OUTPUT.exists() else ""
        if current != rendered:
            print("STATUS.md is stale; run python3 scripts/status-dashboard.py --write", file=sys.stderr)
            return 1
        return 0
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
