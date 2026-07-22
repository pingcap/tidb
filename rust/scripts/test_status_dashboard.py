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

"""Regression tests for the generated rewrite status dashboard."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


SCRIPT = Path(__file__).with_name("status-dashboard.py")
SPEC = importlib.util.spec_from_file_location("status_dashboard", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
dashboard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dashboard)


class StatusDashboardTest(unittest.TestCase):
    """Protect deterministic status rendering and checked-ledger counting."""

    def test_checked_dashboard_is_current(self) -> None:
        self.assertEqual(dashboard.OUTPUT.read_text(), dashboard.render())

    def test_status_counts_do_not_treat_partial_as_covered(self) -> None:
        rows = [
            ["a", "1", "crate", "false", "PARTIAL"],
            ["b", "1", "crate", "false", "COVERED"],
            ["c", "1", "crate", "false", "PARTIAL"],
        ]
        counts = dashboard.status_counts(rows, 4)
        self.assertEqual(counts["PARTIAL"], 2)
        self.assertEqual(counts["COVERED"], 1)

    def test_grouped_counts_keep_targets_separate(self) -> None:
        rows = [
            ["a", "1", "tidb-a", "false", "UNTRIAGED"],
            ["b", "1", "tidb-b", "false", "PARTIAL"],
            ["c", "1", "tidb-a", "false", "COVERED"],
        ]
        grouped = dashboard.grouped_status_counts(rows, 2, 4)
        self.assertEqual(grouped["tidb-a"]["UNTRIAGED"], 1)
        self.assertEqual(grouped["tidb-a"]["COVERED"], 1)
        self.assertEqual(grouped["tidb-b"]["PARTIAL"], 1)

    def test_render_identifies_dashboard_as_non_parity_percentage(self) -> None:
        rendered = dashboard.render()
        self.assertIn("ownership states, not product-parity percentages", rendered)
        self.assertNotIn("Active package claims", rendered)
        self.assertIn("- Declared ready packages:", rendered)
        self.assertIn("## Blocked packages", rendered)
        self.assertIn("## Legacy schema-1 evidence", rendered)
        self.assertIn("## Pinned external Go universes", rendered)
        self.assertIn("github.com/tikv/client-go/v2", rendered)
        self.assertIn("github.com/tikv/pd/client", rendered)
        self.assertIn("not included in TiDB product-parity totals", rendered)

    def test_legacy_records_do_not_increment_current_package_queue(self) -> None:
        slices = [
            {"schema": "1", "status": "ready"},
            {"schema": "1", "status": "covered"},
            {"schema": "2", "status": "ready"},
            {"schema": "2", "status": "active"},
        ]
        counts = dashboard.queue_status(slices)

        self.assertEqual(counts["ready"], 1)
        self.assertEqual(counts["active"], 1)
        self.assertEqual(counts["covered"], 0)

    def test_legacy_campaigns_are_not_package_campaigns(self) -> None:
        campaigns = [
            {"schema": "1", "campaign": "legacy", "status": "integrated"},
            {"schema": "2", "campaign": "packages", "status": "planned"},
        ]

        package_campaigns = dashboard.records_for_schema(campaigns, 2)

        self.assertEqual(
            [campaign["campaign"] for campaign in package_campaigns], ["packages"]
        )

    def test_external_ownership_counts_include_checked_promotions(self) -> None:
        rendered = dashboard.render()
        source_counts = dashboard.status_counts(dashboard.read_tsv(dashboard.EXTERNAL_SOURCE_LEDGER), 4)
        test_counts = dashboard.status_counts(dashboard.read_tsv(dashboard.EXTERNAL_TEST_LEDGER), 7)
        self.assertGreaterEqual(source_counts["PARTIAL"], 2)
        self.assertGreaterEqual(test_counts["PARTIAL"], 1)
        self.assertIn(
            f"| All external production sources | {source_counts['UNTRIAGED']} | {source_counts['PARTIAL']} |",
            rendered,
        )
        self.assertIn("| client-go production sources |", rendered)
        self.assertIn("| pd-client production sources |", rendered)


if __name__ == "__main__":
    unittest.main()
