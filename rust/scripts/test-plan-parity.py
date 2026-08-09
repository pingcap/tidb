#!/usr/bin/env python3
"""Focused unit tests for plan-parity protocol and normalization rules."""

from __future__ import annotations

import importlib.util
import struct
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("plan-parity.py")
SPEC = importlib.util.spec_from_file_location("plan_parity", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
PLAN_PARITY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = PLAN_PARITY
SPEC.loader.exec_module(PLAN_PARITY)

GENERATOR_PATH = Path(__file__).with_name("generate-plan-manifest.py")
GENERATOR_SPEC = importlib.util.spec_from_file_location(
    "generate_plan_manifest", GENERATOR_PATH
)
assert GENERATOR_SPEC is not None and GENERATOR_SPEC.loader is not None
GENERATOR = importlib.util.module_from_spec(GENERATOR_SPEC)
GENERATOR_SPEC.loader.exec_module(GENERATOR)


class NormalizePlanTest(unittest.TestCase):
    def test_normalizes_only_generated_ids_and_column_ordinals(self) -> None:
        plan = [[
            "  \u2514\u2500IndexRangeScan_42(Build)",
            "10.00",
            "cop[tikv]",
            "table:t, index:i(a)",
            "eq(test.t.a, Column#31), keep order:false",
        ]]

        self.assertEqual(
            PLAN_PARITY.normalize_plan(plan),
            [[
                "  \u2514\u2500IndexRangeScan(Build)",
                "10.00",
                "cop[tikv]",
                "table:t, index:i(a)",
                "eq(test.t.a, Column#?), keep order:false",
            ]],
        )

    def test_preserves_protected_plan_fields(self) -> None:
        go = [["IndexReader", "1.00", "root", "index:IndexRangeScan", ""]]
        rust = [["IndexRangeScan", "1.00", "root", "index:i(a)", ""]]

        self.assertNotEqual(
            PLAN_PARITY.normalize_plan(go), PLAN_PARITY.normalize_plan(rust)
        )


class ExecutePayloadTest(unittest.TestCase):
    def test_encodes_exact_mysql_parameter_types(self) -> None:
        parameters = [
            PLAN_PARITY.Parameter("i32", 7),
            PLAN_PARITY.Parameter("i64", -9),
            PLAN_PARITY.Parameter("f64", 1.5),
            PLAN_PARITY.Parameter("string", "ab"),
            PLAN_PARITY.Parameter("null", None),
        ]

        payload = PLAN_PARITY.build_execute_payload(0x01020304, parameters)

        self.assertEqual(payload[:9], b"\x04\x03\x02\x01\x00\x01\x00\x00\x00")
        self.assertEqual(payload[9:11], b"\x10\x01")
        self.assertEqual(
            payload[11:21],
            bytes((
                PLAN_PARITY.MYSQL_TYPE_LONG, 0,
                PLAN_PARITY.MYSQL_TYPE_LONGLONG, 0,
                PLAN_PARITY.MYSQL_TYPE_DOUBLE, 0,
                PLAN_PARITY.MYSQL_TYPE_STRING, 0,
                PLAN_PARITY.MYSQL_TYPE_NULL, 0,
            )),
        )
        expected_values = (
            (7).to_bytes(4, "little", signed=True)
            + (-9).to_bytes(8, "little", signed=True)
            + struct.pack("<d", 1.5)
            + b"\x02ab"
        )
        self.assertEqual(payload[21:], expected_values)


class PlanFamilyTest(unittest.TestCase):
    def test_expands_only_declared_multi_row_widths(self) -> None:
        manifest = {
            "cases": [],
            "plan_families": [
                {
                    "id": "sysbench.prepare.insert",
                    "suite": "sysbench",
                    "phase": "prepare",
                    "kind": "plan",
                    "protocol": "direct",
                    "source": "prepare.lua:35",
                    "generator": "multi_row_insert",
                    "sql_prefix": "INSERT INTO sbtest1 VALUES ",
                    "row_template": "({row},'x')",
                    "row_counts": [1, 3],
                    "separator": ",",
                }
            ],
        }

        cases = PLAN_PARITY.expand_plan_families(manifest)

        self.assertEqual(
            [case["id"] for case in cases],
            [
                "sysbench.prepare.insert.rows_1",
                "sysbench.prepare.insert.rows_3",
            ],
        )
        self.assertEqual(
            cases[1]["sql"],
            "INSERT INTO sbtest1 VALUES (1,'x'),(2,'x'),(3,'x')",
        )

    def test_rejects_unbounded_family_ranges(self) -> None:
        family = {"id": "bad", "row_counts": {"min": 1, "max": 4097}}

        with self.assertRaises(PLAN_PARITY.GateError):
            PLAN_PARITY.family_row_counts(family)

    def test_accepts_bulk_insert_padding_comment(self) -> None:
        family = {
            "id": "sysbench.run.bulk_insert",
            "row_template": "({row},{row},'','')/*padding*/",
        }

        self.assertEqual(
            PLAN_PARITY.render_family_row(family, 7),
            "(7,7,'','')/*padding*/",
        )


class WorkloadCoverageTest(unittest.TestCase):
    def test_covers_all_sysbench_tables_and_worker_local_tables(self) -> None:
        manifest = GENERATOR.build_manifest()
        identities = {case["id"] for case in manifest["cases"]}
        family_ids = {family["id"] for family in manifest["plan_families"]}

        self.assertIn("sysbench.run.point_select.table_32", identities)
        self.assertIn("sysbench.run.random_points.width_10.table_16", identities)
        self.assertNotIn(
            "sysbench.run.random_points.width_10.table_17", identities
        )
        self.assertIn("sysbench.prepare.insert.table_32", family_ids)
        self.assertIn("sysbench.run.bulk_insert.table_16", family_ids)
        self.assertNotIn("sysbench.run.bulk_insert.table_17", family_ids)

    def test_manifest_counts_include_all_dynamic_sql_shapes(self) -> None:
        manifest = GENERATOR.build_manifest()

        self.assertEqual(manifest["static_case_count"], 563)
        self.assertEqual(manifest["plan_family_count"], 57)
        self.assertEqual(manifest["expanded_case_count"], 15_248)


if __name__ == "__main__":
    unittest.main()
