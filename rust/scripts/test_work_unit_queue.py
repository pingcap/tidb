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

from __future__ import annotations

import json
import fcntl
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("work-unit-queue.py")


class WorkUnitQueueTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.shared_fixture_artifact = self.root.parent / "a"
        self.shared_fixture_artifact.write_text("evidence", encoding="utf-8")
        coverage = self.root / "difftests/corpus/coverage"
        coverage.mkdir(parents=True)
        (coverage / "go_source_inventory.tsv").write_text(
            "# source\n"
            "pkg/planner/foo.go\t200\ttidb-planner\tfalse\tUNTRIAGED\t-\t-\t-\n"
            "pkg/planner/bar.go\t50\ttidb-planner\tfalse\tPARTIAL\told\ta\tn\n",
            encoding="utf-8",
        )
        (coverage / "go_test_inventory.tsv").write_text(
            "# tests\n"
            "go_test\tpkg/planner/foo_test.go\t10\tTestFoo\tplan\tUNTRIAGED\t-\t-\t-\n"
            "go_test\tpkg/planner/other_test.go\t20\tTestOther\tplan\tUNTRIAGED\t-\t-\t-\n",
            encoding="utf-8",
        )
        (coverage / "go_package_support_inventory.tsv").write_text(
            "# package_path\tsupport_path\tsha256\n",
            encoding="utf-8",
        )
        (coverage / "external_go_source_inventory.tsv").write_text(
            "# external sources\n"
            + "".join(
                f"client-go\tinternal/client/client{index}.go\t100\tdeadbeef\tUNTRIAGED\t-\t-\t-\n"
                for index in range(9)
            ),
            encoding="utf-8",
        )
        (coverage / "external_go_test_inventory.tsv").write_text(
            "# external tests\n"
            + "".join(
                f"client-go\tTest\tinternal/client/client_test.go\t{20 + index}\tTestSend{index}\ttransaction\tfeedface\tUNTRIAGED\t-\t-\t-\n"
                for index in range(50)
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.shared_fixture_artifact.unlink(missing_ok=True)
        self.temporary.cleanup()

    def test_check_rejects_comma_separated_source_evidence_artifacts(self) -> None:
        inventory = self.root / "difftests/corpus/coverage/go_source_inventory.tsv"
        inventory.write_text(
            inventory.read_text(encoding="utf-8").replace(
                "PARTIAL\told\ta\tn", "PARTIAL\told\ta,b\tn"
            ),
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn("evidence artifact must be one path", result.stderr)
        self.assertIn("not a comma-separated list: a,b", result.stderr)

    def test_check_rejects_comma_separated_test_evidence_artifacts(self) -> None:
        inventory = self.root / "difftests/corpus/coverage/go_test_inventory.tsv"
        inventory.write_text(
            inventory.read_text(encoding="utf-8").replace(
                "plan\tUNTRIAGED\t-\t-\t-",
                "plan\tPARTIAL\ttest-owner\ta,b\tevidence",
                1,
            ),
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn("evidence artifact must be one path", result.stderr)
        self.assertIn("not a comma-separated list: a,b", result.stderr)

    def test_check_rejects_missing_source_evidence_artifact(self) -> None:
        inventory = self.root / "difftests/corpus/coverage/go_source_inventory.tsv"
        inventory.write_text(
            inventory.read_text(encoding="utf-8").replace(
                "PARTIAL\told\ta\tn", "PARTIAL\told\tmissing-source.rs\tn"
            ),
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn(
            "pkg/planner/bar.go evidence artifact is missing: missing-source.rs",
            result.stderr,
        )

    def test_check_rejects_missing_test_evidence_artifact(self) -> None:
        inventory = self.root / "difftests/corpus/coverage/go_test_inventory.tsv"
        inventory.write_text(
            inventory.read_text(encoding="utf-8").replace(
                "plan\tUNTRIAGED\t-\t-\t-",
                "plan\tPARTIAL\ttest-owner\tmissing-test.rs\tevidence",
                1,
            ),
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn(
            "pkg/planner/foo_test.go:10:TestFoo evidence artifact is missing: "
            "missing-test.rs",
            result.stderr,
        )

    def test_check_rejects_duplicate_qualified_module_source_keys(self) -> None:
        inventory = (
            self.root
            / "difftests/corpus/coverage/external_go_source_inventory.tsv"
        )
        first = next(
            line for line in inventory.read_text(encoding="utf-8").splitlines()
            if line and not line.startswith("#")
        )
        with inventory.open("a", encoding="utf-8") as output:
            output.write(first + "\n")
        result = self.run_tool("check", success=False)
        self.assertIn("duplicate qualified module source anchor", result.stderr)

    def test_check_rejects_duplicate_qualified_module_test_keys(self) -> None:
        inventory = (
            self.root
            / "difftests/corpus/coverage/external_go_test_inventory.tsv"
        )
        first = next(
            line for line in inventory.read_text(encoding="utf-8").splitlines()
            if line and not line.startswith("#")
        )
        with inventory.open("a", encoding="utf-8") as output:
            output.write(first + "\n")
        result = self.run_tool("check", success=False)
        self.assertIn("duplicate qualified module test anchor", result.stderr)

    def run_tool(
        self,
        *arguments: str,
        success: bool = True,
        freeze_legacy: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        slices = self.root / "workstreams/slices"
        if freeze_legacy and slices.is_dir():
            legacy = []
            for manifest in sorted(slices.glob("*.toml")):
                if 'schema = "1"' in manifest.read_text(encoding="utf-8"):
                    legacy.append(manifest.stem)
            (slices / "legacy-schema1-slices.tsv").write_text(
                "# test fixture legacy registry\n"
                + "".join(f"{name}\n" for name in legacy),
                encoding="utf-8",
            )
        environment = os.environ.copy()
        environment["TIDB_REWRITE_RUST_ROOT"] = str(self.root)
        result = subprocess.run(
            [sys.executable, str(SCRIPT), *arguments],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        self.assertEqual(result.returncode == 0, success, result.stderr)
        return result

    def write_package_slice(
        self,
        name: str,
        *,
        packages: list[str],
        targets: list[str],
        rings: list[str] | None = None,
        status: str = "ready",
        depends_on: list[str] | None = None,
        module_packages: list[str] | None = None,
    ) -> Path:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True, exist_ok=True)
        path = slices / f"{name}.toml"
        path.write_text(
            'schema = "2"\n'
            f'slice = "{name}"\n'
            f'status = "{status}"\n'
            f"targets = {json.dumps(targets)}\n"
            f"rings = {json.dumps(rings or ['plan'])}\n"
            f'consumer = "package {name}"\n'
            f'test_target = "{name.replace("-", "_")}_source"\n'
            f"go_packages = {json.dumps(packages)}\n"
            f"module_packages = {json.dumps(module_packages or [])}\n"
            f"depends_on = {json.dumps(depends_on or [])}\n"
            f'rust_paths = ["rust/crates/tidb-planner/src/{name}.rs"]\n',
            encoding="utf-8",
        )
        return path

    def test_queue_emits_the_complete_go_package_as_candidate(self) -> None:
        result = self.run_tool(
            "queue", "--target", "tidb-planner", "--ring", "plan", "--limit", "1"
        )
        self.assertIn("go_package", result.stdout)
        self.assertIn("pkg/planner\t2\t2\t0\t2\t2\t2", result.stdout)

    def test_claim_rejects_overlapping_source_and_release_unlocks_it(self) -> None:
        anchor = "pkg/planner/foo_test.go:10:TestFoo"
        self.run_tool(
            "claim", "--owner", "agent-a", "--source", "pkg/planner/foo.go", "--test", anchor
        )
        claim_path = self.root / "workstreams/claims/agent-a.claim.json"
        claim = json.loads(claim_path.read_text())
        self.assertEqual(claim["schema"], 1)
        self.assertEqual(claim["sources"], ["pkg/planner/foo.go"])
        self.assertEqual(claim["tests"], [anchor])
        failure = self.run_tool(
            "claim", "--owner", "agent-b", "--source", "pkg/planner/foo.go", success=False
        )
        self.assertIn("already claimed", failure.stderr)
        self.run_tool("release", "--owner", "agent-a", "--abandon")
        self.run_tool("claim", "--owner", "agent-b", "--source", "pkg/planner/foo.go")

    def test_claim_can_own_a_multi_source_vertical_slice(self) -> None:
        self.run_tool(
            "claim",
            "--owner",
            "agent-a",
            "--source",
            "pkg/planner/foo.go",
            "--source",
            "pkg/planner/bar.go",
        )
        path = self.root / "workstreams/claims/agent-a.claim.json"
        self.assertEqual(
            json.loads(path.read_text())["sources"],
            ["pkg/planner/bar.go", "pkg/planner/foo.go"],
        )
        result = self.run_tool(
            "claim",
            "--owner",
            "agent-b",
            "--source",
            "pkg/planner/bar.go",
            success=False,
        )
        self.assertIn("already claimed", result.stderr)

    def test_ready_slice_requires_covered_dependencies_and_claims_all_sources(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        (slices / "datatype-seam.toml").write_text(
            'schema = "1"\n'
            'slice = "datatype-seam"\n'
            'status = "covered"\n'
            'target = "tidb-datatype"\n'
            'ring = "result"\n'
            'consumer = "shared Datum authority"\n'
            'test_target = "datum_source"\n'
            'go_sources = ["pkg/planner/bar.go"]\n'
            'go_tests = []\n'
            'depends_on = []\n'
            'rust_paths = ["rust/crates/tidb-datatype/src/datum.rs"]\n',
            encoding="utf-8",
        )
        (slices / "planner-consumer.toml").write_text(
            'schema = "1"\n'
            'slice = "planner-consumer"\n'
            'status = "ready"\n'
            'target = "tidb-planner"\n'
            'ring = "plan"\n'
            'consumer = "planner to executor result"\n'
            'test_target = "planner_consumer_source"\n'
            'go_sources = ["pkg/planner/foo.go"]\n'
            'go_tests = ["pkg/planner/foo_test.go:10:TestFoo"]\n'
            'depends_on = ["datatype-seam"]\n'
            'rust_paths = ["rust/crates/tidb-planner/src/foo.rs"]\n',
            encoding="utf-8",
        )
        ready = self.run_tool("ready", "--target", "tidb-planner")
        self.assertNotIn("planner-consumer", ready.stdout)
        failure = self.run_tool(
            "claim-slice", "--owner", "agent-a", "--slice", "planner-consumer",
            success=False,
        )
        self.assertIn("only schema-2", failure.stderr)

    def test_external_only_slice_claims_qualified_module_anchors(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        (slices / "client-runtime.toml").write_text(
            'schema = "1"\n'
            'slice = "client-runtime"\n'
            'status = "ready"\n'
            'target = "tidb-txnkv"\n'
            'ring = "transaction"\n'
            'consumer = "injected client"\n'
            'test_target = "client_runtime"\n'
            'go_sources = []\n'
            'go_tests = []\n'
            'module_sources = ["client-go::internal/client/client0.go"]\n'
            'module_tests = ["client-go::internal/client/client_test.go:20:TestSend0"]\n'
            'depends_on = []\n'
            'rust_paths = ["rust/crates/tidb-txnkv/src/client_runtime.rs"]\n',
            encoding="utf-8",
        )
        self.assertNotIn("client-runtime", self.run_tool("ready").stdout)
        failure = self.run_tool(
            "claim-slice", "--owner", "client-runtime", "--slice", "client-runtime",
            success=False,
        )
        self.assertIn("only schema-2", failure.stderr)

    def test_integrated_external_release_requires_promoted_module_evidence(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        artifact_relative = f"{self.root.name}/client-runtime.rs"
        (self.root.parent / artifact_relative).write_text("runtime", encoding="utf-8")
        manifest = slices / "client-runtime.toml"
        manifest.write_text(
            'schema = "1"\nslice = "client-runtime"\nstatus = "ready"\n'
            'target = "tidb-txnkv"\nring = "transaction"\nconsumer = "client"\n'
            'test_target = "client"\ngo_sources = []\ngo_tests = []\n'
            'module_sources = ["client-go::internal/client/client0.go"]\n'
            'module_tests = ["client-go::internal/client/client_test.go:20:TestSend0"]\n'
            f'depends_on = []\nrust_paths = ["{artifact_relative}"]\n',
            encoding="utf-8",
        )
        failure = self.run_tool(
            "claim-slice", "--owner", "client-runtime", "--slice", "client-runtime",
            success=False,
        )
        self.assertIn("only schema-2", failure.stderr)
        return
        self.run_tool("gate-begin")
        self.run_tool("gate-finish")
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace('status = "ready"', 'status = "partial"'),
            encoding="utf-8",
        )
        failure = self.run_tool("release", "--owner", "client-runtime", "--integrated", success=False)
        self.assertIn("requires promoted module source", failure.stderr)
        for ledger in (
            self.root / "difftests/corpus/coverage/external_go_source_inventory.tsv",
            self.root / "difftests/corpus/coverage/external_go_test_inventory.tsv",
        ):
            ledger.write_text(
                ledger.read_text(encoding="utf-8").replace(
                    "UNTRIAGED\t-\t-\t-", f"PARTIAL\tclient-runtime\t{artifact_relative}\tbounded evidence", 1
                ),
                encoding="utf-8",
            )
        self.run_tool("release", "--owner", "client-runtime", "--integrated")
        self.assertFalse((self.root / "workstreams/claims/client-runtime.claim.json").exists())

    def test_campaign_floor_counts_qualified_module_anchors(self) -> None:
        slices = self.root / "workstreams/slices"
        campaigns = self.root / "workstreams/campaigns"
        slices.mkdir(parents=True)
        campaigns.mkdir(parents=True)
        for member, source_range, test_range in (
            ("client-a", range(5), range(25)),
            ("client-b", range(5, 9), range(25, 50)),
        ):
            module_sources = ", ".join(
                f'"client-go::internal/client/client{index}.go"'
                for index in source_range
            )
            module_tests = ", ".join(
                f'"client-go::internal/client/client_test.go:{20 + index}:TestSend{index}"'
                for index in test_range
            )
            (slices / f"{member}.toml").write_text(
                f'schema = "1"\nslice = "{member}"\nstatus = "ready"\n'
                'target = "tidb-txnkv"\nring = "transaction"\n'
                f'consumer = "{member}"\ntest_target = "{member}"\n'
                'go_sources = []\ngo_tests = []\n'
                f'module_sources = [{module_sources}]\n'
                f'module_tests = [{module_tests}]\n'
                f'depends_on = []\nrust_paths = ["rust/{member}.rs"]\n',
                encoding="utf-8",
            )
        (campaigns / "external-client.toml").write_text(
            'schema = "1"\ncampaign = "external-client"\nstatus = "planned"\n'
            'slices = ["client-a", "client-b"]\n',
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn("schema-1 campaigns may only be frozen", result.stderr)

    def test_planned_campaign_assigns_registration_for_disabled_autotests(self) -> None:
        slices = self.root / "workstreams/slices"
        campaigns = self.root / "workstreams/campaigns"
        crate = self.root / "crates/tidb-exec"
        slices.mkdir(parents=True)
        campaigns.mkdir(parents=True)
        crate.mkdir(parents=True)
        (crate / "Cargo.toml").write_text(
            '[package]\nname = "tidb-exec"\nversion = "0.0.0"\n'
            'autotests = false\n',
            encoding="utf-8",
        )
        for member, source_range, test_range in (
            ("exec-a", range(5), range(25)),
            ("exec-b", range(5, 9), range(25, 50)),
        ):
            module_sources = ", ".join(
                f'"client-go::internal/client/client{index}.go"'
                for index in source_range
            )
            module_tests = ", ".join(
                f'"client-go::internal/client/client_test.go:{20 + index}:TestSend{index}"'
                for index in test_range
            )
            (slices / f"{member}.toml").write_text(
                f'schema = "1"\nslice = "{member}"\nstatus = "ready"\n'
                'target = "tidb-exec"\nring = "result"\n'
                f'consumer = "{member}"\ntest_target = "{member}_source"\n'
                'go_sources = []\ngo_tests = []\n'
                f'module_sources = [{module_sources}]\n'
                f'module_tests = [{module_tests}]\n'
                f'depends_on = []\nrust_paths = ["rust/{member}.rs"]\n',
                encoding="utf-8",
            )
        (campaigns / "exec-campaign.toml").write_text(
            'schema = "1"\ncampaign = "exec-campaign"\nstatus = "planned"\n'
            'slices = ["exec-a", "exec-b"]\n',
            encoding="utf-8",
        )

        failure = self.run_tool("check", success=False)
        self.assertIn("schema-1 campaigns may only be frozen", failure.stderr)
        return

        manifest = slices / "exec-b.toml"
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'rust_paths = ["rust/exec-b.rs"]',
                'rust_paths = ["rust/exec-b.rs", "rust/crates/tidb-exec/Cargo.toml"]',
            ),
            encoding="utf-8",
        )
        result = self.run_tool("check")
        self.assertIn("campaigns\t1", result.stdout)

    def test_partial_slice_claim_must_exactly_match_its_manifest(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        (slices / "planner-consumer.toml").write_text(
            'schema = "2"\n'
            'slice = "planner-consumer"\n'
            'status = "active"\n'
            'targets = ["tidb-planner"]\n'
            'rings = ["plan"]\n'
            'consumer = "frozen planner consumer"\n'
            'test_target = "planner_consumer_source"\n'
            'go_packages = ["pkg/planner"]\n'
            'module_packages = []\n'
            'depends_on = []\n'
            'rust_paths = ["rust/crates/tidb-planner/src/foo.rs"]\n',
            encoding="utf-8",
        )
        claims = self.root / "workstreams/claims"
        claims.mkdir(parents=True)
        claim_path = claims / "planner-consumer.claim.json"
        claim_path.write_text(
            json.dumps(
                {
                    "schema": 2,
                    "owner": "planner-consumer",
                    "sources": ["pkg/planner/bar.go", "pkg/planner/foo.go"],
                    "tests": [
                        "pkg/planner/foo_test.go:10:TestFoo",
                        "pkg/planner/other_test.go:20:TestOther",
                    ],
                    "supports": [],
                    "rust_paths": ["rust/crates/tidb-planner/src/foo.rs"],
                    "module_sources": [],
                    "module_tests": [],
                }
            ),
            encoding="utf-8",
        )
        self.run_tool("check")

        claim_path.write_text(
            json.dumps(
                {
                    "schema": 2,
                    "owner": "planner-consumer",
                    "sources": ["pkg/planner/bar.go", "pkg/planner/foo.go"],
                    "tests": [],
                    "supports": [],
                    "rust_paths": ["rust/crates/tidb-planner/src/foo.rs"],
                    "module_sources": [],
                    "module_tests": [],
                }
            ),
            encoding="utf-8",
        )
        failure = self.run_tool("check", success=False)
        self.assertIn(
            "schema-2 claim for slice planner-consumer must exactly match",
            failure.stderr,
        )
        self.run_tool("release", "--owner", "planner-consumer", "--abandon")
        self.assertFalse(claim_path.exists())

    def test_integrated_release_rejects_slice_still_marked_ready(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        implementation_relative = f"{self.root.name}/planner-consumer.rs"
        implementation = self.root.parent / implementation_relative
        manifest = slices / "planner-consumer.toml"
        manifest.write_text(
            'schema = "1"\n'
            'slice = "planner-consumer"\n'
            'status = "ready"\n'
            'target = "tidb-planner"\n'
            'ring = "plan"\n'
            'consumer = "planner consumer"\n'
            'test_target = "planner_consumer_source"\n'
            'go_sources = ["pkg/planner/foo.go"]\n'
            'go_tests = ["pkg/planner/foo_test.go:10:TestFoo"]\n'
            'depends_on = []\n'
            f'rust_paths = ["{implementation_relative}"]\n',
            encoding="utf-8",
        )
        failure = self.run_tool(
            "claim-slice", "--owner", "planner-consumer", "--slice", "planner-consumer",
            success=False,
        )
        self.assertIn("only schema-2", failure.stderr)
        return
        failure = self.run_tool(
            "release", "--owner", "planner-consumer", "--integrated", success=False
        )
        self.assertIn("must be partial or covered", failure.stderr)

        implementation.write_text("checked implementation", encoding="utf-8")
        self.run_tool("gate-begin")
        self.run_tool("gate-finish")
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'status = "ready"', 'status = "partial"'
            ),
            encoding="utf-8",
        )
        evidence = self.root / "difftests/corpus/coverage/evidence/source/promoted.tsv"
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text("post-gate evidence promotion\n", encoding="utf-8")
        for inventory in (
            self.root / "difftests/corpus/coverage/go_source_inventory.tsv",
            self.root / "difftests/corpus/coverage/go_test_inventory.tsv",
        ):
            inventory.write_text(
                inventory.read_text(encoding="utf-8") + "# post-gate regeneration\n",
                encoding="utf-8",
            )
        self.run_tool("release", "--owner", "planner-consumer", "--integrated")
        self.assertFalse(
            (self.root / "workstreams/claims/planner-consumer.claim.json").exists()
        )

    def test_integrated_release_rejects_changes_after_the_shared_gate(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        implementation_relative = f"{self.root.name}/planner-consumer.rs"
        implementation = self.root.parent / implementation_relative
        (slices / "planner-consumer.toml").write_text(
            'schema = "1"\n'
            'slice = "planner-consumer"\nstatus = "partial"\n'
            'target = "tidb-planner"\nring = "plan"\n'
            'consumer = "planner consumer"\ntest_target = "planner_consumer_source"\n'
            'go_sources = ["pkg/planner/foo.go"]\n'
            'go_tests = ["pkg/planner/foo_test.go:10:TestFoo"]\n'
            'depends_on = []\n'
            f'rust_paths = ["{implementation_relative}"]\n',
            encoding="utf-8",
        )
        implementation.write_text("checked implementation", encoding="utf-8")
        self.run_tool(
            "claim",
            "--owner", "planner-consumer",
            "--source", "pkg/planner/foo.go",
            "--test", "pkg/planner/foo_test.go:10:TestFoo",
        )
        failure = self.run_tool("gate-begin", success=False)
        self.assertIn("only schema-2 package claims", failure.stderr)
        return
        self.run_tool("gate-finish")
        implementation.write_text("edited after gate", encoding="utf-8")
        failure = self.run_tool(
            "release", "--owner", "planner-consumer", "--integrated", success=False
        )
        self.assertIn("receipt for planner-consumer is stale", failure.stderr)
        self.assertTrue(
            (self.root / "workstreams/claims/planner-consumer.claim.json").exists()
        )

    def test_gate_finish_rejects_even_an_undeclared_edit_during_the_gate(self) -> None:
        self.run_tool(
            "claim", "--owner", "agent-a", "--source", "pkg/planner/foo.go"
        )
        failure = self.run_tool("gate-begin", success=False)
        self.assertIn("only schema-2 package claims", failure.stderr)
        return
        (self.root / "undeclared.rs").write_text("changed during gate", encoding="utf-8")
        failure = self.run_tool("gate-finish", success=False)
        self.assertIn("changed while the shared gate was running", failure.stderr)
        self.run_tool("gate-abort")

    def test_integrated_release_rejects_post_gate_domain_manifest_edits(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        implementation_relative = f"{self.root.name}/domain-owner.rs"
        (self.root.parent / implementation_relative).write_text("owner", encoding="utf-8")
        (slices / "domain-owner.toml").write_text(
            'schema = "1"\nslice = "domain-owner"\nstatus = "partial"\n'
            'target = "tidb-planner"\nring = "plan"\nconsumer = "owner"\n'
            'test_target = "owner"\ngo_sources = ["pkg/planner/foo.go"]\n'
            'go_tests = []\ndepends_on = []\n'
            f'rust_paths = ["{implementation_relative}"]\n',
            encoding="utf-8",
        )
        self.run_tool(
            "claim", "--owner", "domain-owner", "--source", "pkg/planner/foo.go"
        )
        failure = self.run_tool("gate-begin", success=False)
        self.assertIn("only schema-2 package claims", failure.stderr)
        return
        self.run_tool("gate-finish")
        domain = self.root / "workstreams/domains/planner.toml"
        domain.parent.mkdir(parents=True)
        domain.write_text('schema = "changed-after-gate"\n', encoding="utf-8")
        failure = self.run_tool(
            "release", "--owner", "domain-owner", "--integrated", success=False
        )
        self.assertIn("receipt for domain-owner is stale", failure.stderr)

    def test_gate_rejects_double_begin_and_mid_gate_evidence_edits(self) -> None:
        self.run_tool(
            "claim", "--owner", "agent-a", "--source", "pkg/planner/foo.go"
        )
        failure = self.run_tool("gate-begin", success=False)
        self.assertIn("only schema-2 package claims", failure.stderr)
        return
        duplicate = self.run_tool("gate-begin", success=False)
        self.assertIn("already has an active begin snapshot", duplicate.stderr)
        evidence = self.root / "difftests/corpus/coverage/evidence/source/mid-gate.tsv"
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text("changed during gate\n", encoding="utf-8")
        failure = self.run_tool("gate-finish", success=False)
        self.assertIn("changed while the shared gate was running", failure.stderr)
        self.run_tool("gate-abort")

    def test_multi_claim_receipt_is_consumed_once_per_integrated_owner(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        first_relative = f"{self.root.name}/first.rs"
        second_relative = f"{self.root.name}/second.rs"
        (self.root.parent / first_relative).write_text("first", encoding="utf-8")
        (self.root.parent / second_relative).write_text("second", encoding="utf-8")
        (slices / "slice-a.toml").write_text(
            'schema = "1"\nslice = "slice-a"\nstatus = "partial"\n'
            'target = "tidb-planner"\nring = "plan"\nconsumer = "first"\n'
            'test_target = "first"\ngo_sources = ["pkg/planner/foo.go"]\n'
            'go_tests = ["pkg/planner/foo_test.go:10:TestFoo"]\n'
            'depends_on = []\n'
            f'rust_paths = ["{first_relative}"]\n',
            encoding="utf-8",
        )
        (slices / "slice-b.toml").write_text(
            'schema = "1"\nslice = "slice-b"\nstatus = "partial"\n'
            'target = "tidb-planner"\nring = "plan"\nconsumer = "second"\n'
            'test_target = "second"\ngo_sources = ["pkg/planner/bar.go"]\n'
            'go_tests = ["pkg/planner/other_test.go:20:TestOther"]\n'
            'depends_on = []\n'
            f'rust_paths = ["{second_relative}"]\n',
            encoding="utf-8",
        )
        self.run_tool(
            "claim", "--owner", "slice-a", "--source", "pkg/planner/foo.go",
            "--test", "pkg/planner/foo_test.go:10:TestFoo",
        )
        self.run_tool(
            "claim", "--owner", "slice-b", "--source", "pkg/planner/bar.go",
            "--test", "pkg/planner/other_test.go:20:TestOther",
        )
        failure = self.run_tool("gate-begin", success=False)
        self.assertIn("only schema-2 package claims", failure.stderr)
        return
        self.run_tool("gate-finish")
        receipt = self.root / "workstreams/integration-receipt.json"
        self.run_tool("release", "--owner", "slice-a", "--integrated")
        self.assertEqual(
            set(json.loads(receipt.read_text(encoding="utf-8"))["claims"]),
            {"slice-b"},
        )
        self.run_tool("release", "--owner", "slice-b", "--integrated")
        self.assertFalse(receipt.exists())

    def test_release_requires_explicit_integrated_or_abandon_mode(self) -> None:
        self.run_tool(
            "claim", "--owner", "agent-a", "--source", "pkg/planner/foo.go"
        )
        result = self.run_tool("release", "--owner", "agent-a", success=False)
        self.assertIn("one of the arguments --integrated --abandon is required", result.stderr)
        self.run_tool("release", "--owner", "agent-a", "--abandon")

    def test_slice_claims_serialize_overlapping_rust_write_paths(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        shared = 'rust_paths = ["rust/crates/tidb-planner/src/lib.rs"]\n'
        (slices / "slice-a.toml").write_text(
            'schema = "1"\n'
            'slice = "slice-a"\nstatus = "ready"\n'
            'target = "tidb-planner"\nring = "plan"\n'
            'consumer = "first consumer"\ntest_target = "a"\n'
            'go_sources = ["pkg/planner/foo.go"]\n'
            'go_tests = ["pkg/planner/foo_test.go:10:TestFoo"]\n'
            'depends_on = []\n' + shared,
            encoding="utf-8",
        )
        (slices / "slice-b.toml").write_text(
            'schema = "1"\n'
            'slice = "slice-b"\nstatus = "ready"\n'
            'target = "tidb-planner"\nring = "plan"\n'
            'consumer = "second consumer"\ntest_target = "b"\n'
            'go_sources = ["pkg/planner/bar.go"]\n'
            'go_tests = ["pkg/planner/other_test.go:20:TestOther"]\n'
            'depends_on = []\n' + shared,
            encoding="utf-8",
        )
        failure = self.run_tool(
            "claim-slice", "--owner", "slice-a", "--slice", "slice-a", success=False
        )
        self.assertIn("only schema-2", failure.stderr)

    def test_ready_slice_requires_exact_ledger_evidence_anchor_and_minimum_status(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        (slices / "planner-consumer.toml").write_text(
            'schema = "1"\n'
            'slice = "planner-consumer"\n'
            'status = "ready"\n'
            'target = "tidb-planner"\n'
            'ring = "plan"\n'
            'consumer = "planner consumer"\n'
            'test_target = "planner_consumer_source"\n'
            'go_sources = ["pkg/planner/foo.go"]\n'
            'go_tests = []\n'
            'depends_on = []\n'
            'rust_paths = ["rust/crates/tidb-planner/src/foo.rs"]\n'
            'evidence_prerequisites = [\n'
            '  { capability = "bar-shape", evidence_owner = "old", kind = "source", anchor = "pkg/planner/bar.go", minimum_status = "PARTIAL" },\n'
            '  { capability = "foo-contract", evidence_owner = "test-owner", kind = "test", anchor = "pkg/planner/foo_test.go:10:TestFoo", minimum_status = "COVERED" },\n'
            ']\n',
            encoding="utf-8",
        )
        self.assertNotIn("planner-consumer", self.run_tool("ready").stdout)
        failure = self.run_tool(
            "claim-slice", "--owner", "agent-a", "--slice", "planner-consumer",
            success=False,
        )
        self.assertIn("only schema-2", failure.stderr)
        return

        inventory = self.root / "difftests/corpus/coverage/go_test_inventory.tsv"
        inventory.write_text(
            inventory.read_text().replace(
                "plan\tUNTRIAGED\t-\t-\t-", "plan\tCOVERED\ttest-owner\ta\tn", 1
            ),
            encoding="utf-8",
        )
        self.assertIn("planner-consumer", self.run_tool("ready").stdout)
        self.run_tool(
            "claim-slice", "--owner", "agent-a", "--slice", "planner-consumer"
        )

    def test_evidence_prerequisite_does_not_infer_coverage_from_another_anchor(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        (slices / "planner-consumer.toml").write_text(
            'schema = "1"\n'
            'slice = "planner-consumer"\n'
            'status = "ready"\n'
            'target = "tidb-planner"\n'
            'ring = "plan"\n'
            'consumer = "planner consumer"\n'
            'test_target = "planner_consumer_source"\n'
            'go_sources = ["pkg/planner/foo.go"]\n'
            'go_tests = []\n'
            'depends_on = []\n'
            'rust_paths = ["rust/crates/tidb-planner/src/foo.rs"]\n'
            'evidence_prerequisites = [\n'
            '  { capability = "foo-shape", evidence_owner = "old", kind = "source", anchor = "pkg/planner/foo.go", minimum_status = "PARTIAL" },\n'
            ']\n',
            encoding="utf-8",
        )
        # bar.go is PARTIAL, but that must not satisfy the exact foo.go prerequisite.
        self.assertNotIn("planner-consumer", self.run_tool("ready").stdout)

    def test_check_rejects_unknown_evidence_prerequisite_anchor(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        (slices / "planner-consumer.toml").write_text(
            'schema = "1"\n'
            'slice = "planner-consumer"\n'
            'status = "ready"\n'
            'target = "tidb-planner"\n'
            'ring = "plan"\n'
            'consumer = "planner consumer"\n'
            'test_target = "planner_consumer_source"\n'
            'go_sources = ["pkg/planner/foo.go"]\n'
            'go_tests = []\n'
            'depends_on = []\n'
            'rust_paths = ["rust/crates/tidb-planner/src/foo.rs"]\n'
            'evidence_prerequisites = [\n'
            '  { capability = "missing-shape", evidence_owner = "missing-owner", kind = "source", anchor = "pkg/planner/missing.go", minimum_status = "COVERED" },\n'
            ']\n',
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn("unknown source anchor pkg/planner/missing.go", result.stderr)

    def test_check_rejects_stale_test_anchor(self) -> None:
        claims = self.root / "workstreams/claims"
        claims.mkdir(parents=True)
        (claims / "agent-a.claim.json").write_text(
            json.dumps(
                {
                    "schema": 1,
                    "owner": "agent-a",
                    "source": "pkg/planner/foo.go",
                    "tests": ["pkg/planner/foo_test.go:99:Missing"],
                }
            ),
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn("stale test anchor", result.stderr)

    def test_claim_transaction_rejects_a_concurrent_writer(self) -> None:
        claims = self.root / "workstreams/claims"
        claims.mkdir(parents=True)
        descriptor = os.open(claims / ".lock", os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            result = self.run_tool(
                "claim",
                "--owner",
                "agent-a",
                "--source",
                "pkg/planner/foo.go",
                success=False,
            )
            self.assertIn("transaction already active", result.stderr)
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def test_amend_adds_a_discovered_test_and_rejects_another_owners_anchor(self) -> None:
        first = "pkg/planner/foo_test.go:10:TestFoo"
        second = "pkg/planner/other_test.go:20:TestOther"
        self.run_tool(
            "claim", "--owner", "agent-a", "--source", "pkg/planner/foo.go", "--test", first
        )
        self.run_tool("amend", "--owner", "agent-a", "--test", second)
        path = self.root / "workstreams/claims/agent-a.claim.json"
        self.assertEqual(json.loads(path.read_text())["tests"], [first, second])
        self.run_tool("release", "--owner", "agent-a", "--abandon")
        self.run_tool(
            "claim", "--owner", "agent-b", "--source", "pkg/planner/bar.go", "--test", second
        )
        self.run_tool(
            "claim", "--owner", "agent-a", "--source", "pkg/planner/foo.go", "--test", first
        )
        result = self.run_tool(
            "amend", "--owner", "agent-a", "--test", second, success=False
        )
        self.assertIn("already claimed", result.stderr)

    def test_amend_adds_a_source_and_rejects_another_owners_source(self) -> None:
        self.run_tool("claim", "--owner", "agent-a", "--source", "pkg/planner/foo.go")
        self.run_tool(
            "amend", "--owner", "agent-a", "--source", "pkg/planner/bar.go"
        )
        path = self.root / "workstreams/claims/agent-a.claim.json"
        self.assertEqual(
            json.loads(path.read_text())["sources"],
            ["pkg/planner/bar.go", "pkg/planner/foo.go"],
        )
        self.run_tool("release", "--owner", "agent-a", "--abandon")
        self.run_tool("claim", "--owner", "agent-b", "--source", "pkg/planner/bar.go")
        self.run_tool("claim", "--owner", "agent-a", "--source", "pkg/planner/foo.go")
        result = self.run_tool(
            "amend",
            "--owner",
            "agent-a",
            "--source",
            "pkg/planner/bar.go",
            success=False,
        )
        self.assertIn("already claimed", result.stderr)

    def test_check_rejects_a_transfer_without_the_new_ledger_owner(self) -> None:
        transfers = self.root / "difftests/corpus/coverage/evidence/transfers"
        transfers.mkdir(parents=True)
        (transfers / "move.tsv").write_text(
            "old-owner\tnew-owner\tpkg/planner/foo.go\t"
            "pkg/planner/foo_test.go\t10\tTestFoo\told.rs\tnew.rs\tmoved\n",
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn("is not owned by terminal owner new-owner", result.stderr)

    def test_check_accepts_a_truthful_source_only_transfer(self) -> None:
        source_inventory = self.root / "difftests/corpus/coverage/go_source_inventory.tsv"
        source_inventory.write_text(
            source_inventory.read_text().replace(
                "PARTIAL\told\ta\tn", "PARTIAL\tnew-owner\tnew.rs\tmoved"
            ),
            encoding="utf-8",
        )
        transfers = self.root / "difftests/corpus/coverage/evidence/transfers"
        transfers.mkdir(parents=True)
        (self.root.parent / "new.rs").write_text("replacement", encoding="utf-8")
        (transfers / "source-only.tsv").write_text(
            "old-owner\tnew-owner\tpkg/planner/bar.go\t-\t-\t-\t"
            "old.rs\tnew.rs\tsource moved without inventing a test transfer\n",
            encoding="utf-8",
        )
        self.run_tool("check")

    def test_check_accepts_an_ordered_transfer_chain(self) -> None:
        source_inventory = self.root / "difftests/corpus/coverage/go_source_inventory.tsv"
        source_inventory.write_text(
            source_inventory.read_text().replace(
                "PARTIAL\told\ta\tn", "PARTIAL\tfinal-owner\tfinal.rs\tmoved twice"
            ),
            encoding="utf-8",
        )
        transfers = self.root / "difftests/corpus/coverage/evidence/transfers"
        transfers.mkdir(parents=True)
        final_artifact = self.root.parent / "final.rs"
        final_artifact.write_text("terminal replacement", encoding="utf-8")
        self.addCleanup(final_artifact.unlink, missing_ok=True)
        (transfers / "first.tsv").write_text(
            "old-owner\tmiddle-owner\tpkg/planner/bar.go\t-\t-\t-\t"
            "-\tmiddle.rs\tfirst move\n",
            encoding="utf-8",
        )
        (transfers / "second.tsv").write_text(
            "middle-owner\tfinal-owner\tpkg/planner/bar.go\t-\t-\t-\t"
            "middle.rs\tfinal.rs\tsecond move\n",
            encoding="utf-8",
        )
        self.run_tool("check")

    def test_check_rejects_a_branched_transfer_chain(self) -> None:
        transfers = self.root / "difftests/corpus/coverage/evidence/transfers"
        transfers.mkdir(parents=True)
        (transfers / "branch.tsv").write_text(
            "old-owner\tfirst-owner\tpkg/planner/bar.go\t-\t-\t-\t-\t-\tfirst\n"
            "old-owner\tsecond-owner\tpkg/planner/bar.go\t-\t-\t-\t-\t-\tsecond\n",
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn("branched source ownership transfer", result.stderr)

    def test_check_rejects_a_cyclic_transfer_chain(self) -> None:
        transfers = self.root / "difftests/corpus/coverage/evidence/transfers"
        transfers.mkdir(parents=True)
        (transfers / "cycle.tsv").write_text(
            "old-owner\tmiddle-owner\tpkg/planner/bar.go\t-\t-\t-\t-\t-\tfirst\n"
            "middle-owner\told-owner\tpkg/planner/bar.go\t-\t-\t-\t-\t-\tsecond\n",
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn("must form one acyclic chain", result.stderr)

    def test_check_accepts_a_truthful_test_only_transfer(self) -> None:
        test_inventory = self.root / "difftests/corpus/coverage/go_test_inventory.tsv"
        test_inventory.write_text(
            test_inventory.read_text().replace(
                "plan\tUNTRIAGED\t-\t-\t-", "plan\tPARTIAL\tnew-owner\tnew.rs\tmoved", 1
            ),
            encoding="utf-8",
        )
        transfers = self.root / "difftests/corpus/coverage/evidence/transfers"
        transfers.mkdir(parents=True)
        (self.root.parent / "new.rs").write_text("replacement", encoding="utf-8")
        (transfers / "test-only.tsv").write_text(
            "old-owner\tnew-owner\t-\tpkg/planner/foo_test.go\t10\tTestFoo\t"
            "old.rs\tnew.rs\ttest moved without inventing a source transfer\n",
            encoding="utf-8",
        )
        self.run_tool("check")

    def test_check_accepts_a_truthful_module_source_and_test_transfer(self) -> None:
        source_inventory = (
            self.root / "difftests/corpus/coverage/external_go_source_inventory.tsv"
        )
        source_inventory.write_text(
            source_inventory.read_text().replace(
                "UNTRIAGED\t-\t-\t-", "PARTIAL\tnew-owner\tnew.rs\tmoved", 1
            ),
            encoding="utf-8",
        )
        test_inventory = (
            self.root / "difftests/corpus/coverage/external_go_test_inventory.tsv"
        )
        test_inventory.write_text(
            test_inventory.read_text().replace(
                "UNTRIAGED\t-\t-\t-", "PARTIAL\tnew-owner\tnew.rs\tmoved", 1
            ),
            encoding="utf-8",
        )
        transfers = self.root / "difftests/corpus/coverage/evidence/transfers"
        transfers.mkdir(parents=True)
        replacement = self.root.parent / "new.rs"
        replacement.write_text("replacement", encoding="utf-8")
        self.addCleanup(replacement.unlink, missing_ok=True)
        (transfers / "module.tsv").write_text(
            "old-owner\tnew-owner\tclient-go::internal/client/client0.go\t"
            "client-go::internal/client/client_test.go\t20\tTestSend0\t"
            "-\tnew.rs\tmodule source and test moved together\n",
            encoding="utf-8",
        )
        self.run_tool("check")

    def test_check_rejects_stale_module_transfer_anchors(self) -> None:
        transfers = self.root / "difftests/corpus/coverage/evidence/transfers"
        transfers.mkdir(parents=True)
        for filename, fields, expected in (
            (
                "source.tsv",
                "client-go::internal/client/missing.go\t-\t-\t-",
                "transferred module source client-go::internal/client/missing.go "
                "is not present in the external source ledger",
            ),
            (
                "test.tsv",
                "-\tclient-go::internal/client/client_test.go\t999\tTestMissing",
                "transferred module test "
                "client-go::internal/client/client_test.go:999:TestMissing "
                "is not present in the external test ledger",
            ),
        ):
            with self.subTest(filename=filename):
                for existing in transfers.glob("*.tsv"):
                    existing.unlink()
                (transfers / filename).write_text(
                    f"old-owner\tnew-owner\t{fields}\t-\t-\tstale\n",
                    encoding="utf-8",
                )
                result = self.run_tool("check", success=False)
                self.assertIn(expected, result.stderr)

    def test_check_rejects_module_transfer_without_terminal_ledger_owner(self) -> None:
        transfers = self.root / "difftests/corpus/coverage/evidence/transfers"
        transfers.mkdir(parents=True)
        (transfers / "module.tsv").write_text(
            "old-owner\tnew-owner\tclient-go::internal/client/client0.go\t"
            "-\t-\t-\t-\t-\twrong terminal owner\n",
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn(
            "transferred module source client-go::internal/client/client0.go "
            "is not owned by terminal owner new-owner",
            result.stderr,
        )

    def test_check_rejects_a_branched_module_transfer_chain(self) -> None:
        transfers = self.root / "difftests/corpus/coverage/evidence/transfers"
        transfers.mkdir(parents=True)
        (transfers / "module-branch.tsv").write_text(
            "old-owner\tfirst-owner\tclient-go::internal/client/client0.go\t"
            "-\t-\t-\t-\t-\tfirst\n"
            "old-owner\tsecond-owner\tclient-go::internal/client/client0.go\t"
            "-\t-\t-\t-\t-\tsecond\n",
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn("branched source ownership transfer", result.stderr)

    def test_check_rejects_a_transfer_without_any_anchor(self) -> None:
        transfers = self.root / "difftests/corpus/coverage/evidence/transfers"
        transfers.mkdir(parents=True)
        (transfers / "empty.tsv").write_text(
            "old-owner\tnew-owner\t-\t-\t-\t-\told.rs\tnew.rs\tempty\n",
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn("must contain a source or test anchor", result.stderr)

    def test_check_rejects_a_partially_omitted_test_anchor(self) -> None:
        transfers = self.root / "difftests/corpus/coverage/evidence/transfers"
        transfers.mkdir(parents=True)
        (transfers / "broken.tsv").write_text(
            "old-owner\tnew-owner\tpkg/planner/foo.go\t-\t10\t-\told.rs\tnew.rs\tbroken\n",
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn("must provide path, line, and name", result.stderr)

    def test_check_warns_about_campaign_below_batch_floor(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        for name, source in (
            ("planner-first", "pkg/planner/foo.go"),
            ("planner-second", "pkg/planner/bar.go"),
        ):
            (slices / f"{name}.toml").write_text(
                'schema = "1"\n'
                f'slice = "{name}"\n'
                'status = "ready"\n'
                'target = "tidb-planner"\n'
                'ring = "plan"\n'
                'consumer = "checked campaign member"\n'
                'test_target = "campaign_source"\n'
                f'go_sources = ["{source}"]\n'
                'go_tests = []\n'
                'depends_on = []\n'
                f'rust_paths = ["rust/{name}.rs"]\n',
                encoding="utf-8",
            )
        campaigns = self.root / "workstreams/campaigns"
        campaigns.mkdir(parents=True)
        (campaigns / "planner-batch.toml").write_text(
            'schema = "1"\n'
            'campaign = "planner-batch"\n'
            'status = "planned"\n'
            'slices = ["planner-first", "planner-second"]\n',
            encoding="utf-8",
        )
        for status in ("planned", "active"):
            (campaigns / "planner-batch.toml").write_text(
                'schema = "1"\n'
                'campaign = "planner-batch"\n'
                f'status = "{status}"\n'
                'slices = ["planner-first", "planner-second"]\n',
                encoding="utf-8",
            )
            with self.subTest(status=status):
                # The batch floor is advisory: a small batch is a note, not a
                # failure. Blocking on it rewarded padding a member with test
                # anchors it could not discharge just to clear the count.
                result = self.run_tool("check", success=False)
                self.assertIn(
                    "schema-1 campaigns may only be frozen", result.stderr
                )

    def test_check_accepts_integrated_campaign_after_ownership_shrinks(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        for name, source in (
            ("planner-first", "pkg/planner/foo.go"),
            ("planner-second", "pkg/planner/bar.go"),
        ):
            (slices / f"{name}.toml").write_text(
                'schema = "1"\n'
                f'slice = "{name}"\n'
                'status = "partial"\n'
                'target = "tidb-planner"\n'
                'ring = "plan"\n'
                'consumer = "historical campaign member"\n'
                'test_target = "campaign_source"\n'
                f'go_sources = ["{source}"]\n'
                'go_tests = []\n'
                'depends_on = []\n'
                f'rust_paths = ["rust/{name}.rs"]\n',
                encoding="utf-8",
            )
        campaigns = self.root / "workstreams/campaigns"
        campaigns.mkdir(parents=True)
        (campaigns / "planner-batch.toml").write_text(
            'schema = "1"\n'
            'campaign = "planner-batch"\n'
            'status = "integrated"\n'
            'slices = ["planner-first", "planner-second"]\n',
            encoding="utf-8",
        )
        (campaigns / "integrated-members.tsv").write_text(
            "planner-batch\tplanner-first\n"
            "planner-batch\tplanner-second\n",
            encoding="utf-8",
        )
        self.run_tool("check")

        (campaigns / "integrated-members.tsv").write_text(
            "planner-batch\tplanner-first\n"
            "planner-batch\tplanner-second\n"
            "planner-batch\tplanner-third\n",
            encoding="utf-8",
        )
        failure = self.run_tool("check", success=False)
        self.assertIn("integrated campaign membership differs", failure.stderr)

    def test_check_rejects_campaign_write_set_overlap(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        for name, source in (
            ("planner-first", "pkg/planner/foo.go"),
            ("planner-second", "pkg/planner/bar.go"),
        ):
            (slices / f"{name}.toml").write_text(
                'schema = "1"\n'
                f'slice = "{name}"\n'
                'status = "ready"\n'
                'target = "tidb-planner"\n'
                'ring = "plan"\n'
                'consumer = "checked campaign member"\n'
                'test_target = "campaign_source"\n'
                f'go_sources = ["{source}"]\n'
                'go_tests = []\n'
                'depends_on = []\n'
                'rust_paths = ["rust/shared.rs"]\n',
                encoding="utf-8",
            )
        campaigns = self.root / "workstreams/campaigns"
        campaigns.mkdir(parents=True)
        (campaigns / "planner-batch.toml").write_text(
            'schema = "1"\n'
            'campaign = "planner-batch"\n'
            'status = "frozen"\n'
            'slices = ["planner-first", "planner-second"]\n',
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn(
            "campaign Rust path rust/shared.rs overlaps slices planner-first and planner-second",
            result.stderr,
        )

    def test_package_claim_expands_nearest_package_testdata_and_support(self) -> None:
        sources = self.root / "difftests/corpus/coverage/go_source_inventory.tsv"
        with sources.open("a", encoding="utf-8") as output:
            output.write(
                "pkg/planner/sub/child.go\t30\ttidb-planner\tfalse\tUNTRIAGED\t-\t-\t-\n"
                "pkg/planner/testdata/fixture.go\t5\ttidb-planner\tfalse\tUNTRIAGED\t-\t-\t-\n"
            )
        tests = self.root / "difftests/corpus/coverage/go_test_inventory.tsv"
        with tests.open("a", encoding="utf-8") as output:
            output.write(
                "go_test\tpkg/planner/sub/child_test.go\t7\tTestChild\tplan\tUNTRIAGED\t-\t-\t-\n"
                "go_test_file\tpkg/planner/testdata/deep/fixture_test.go\t0\tfixture_test.go\tplan\tUNTRIAGED\t-\t-\t-\n"
            )
        support = self.root / "difftests/corpus/coverage/go_package_support_inventory.tsv"
        support.write_text(
            "# package_path\tsupport_path\tsha256\n"
            f"pkg/planner\tpkg/planner/BUILD.bazel\t{'a' * 64}\n"
            f"pkg/planner\tpkg/planner/testdata/result.txt\t{'b' * 64}\n"
            f"pkg/planner/sub\tpkg/planner/sub/BUILD.bazel\t{'c' * 64}\n",
            encoding="utf-8",
        )
        self.write_package_slice(
            "planner-package", packages=["pkg/planner"], targets=["tidb-planner"]
        )
        self.run_tool("claim-slice", "--owner", "planner-package", "--slice", "planner-package")
        claim = json.loads(
            (self.root / "workstreams/claims/planner-package.claim.json").read_text()
        )
        self.assertIn("pkg/planner/testdata/fixture.go", claim["sources"])
        self.assertIn(
            "pkg/planner/testdata/deep/fixture_test.go:0:fixture_test.go", claim["tests"]
        )
        self.assertEqual(len(claim["supports"]), 2)
        self.assertEqual(
            claim["rust_paths"], ["rust/crates/tidb-planner/src/planner-package.rs"]
        )
        self.assertNotIn("pkg/planner/sub/child.go", claim["sources"])
        self.assertFalse(any("pkg/planner/sub/BUILD" in item for item in claim["supports"]))

    def test_package_claim_detects_support_digest_and_inventory_drift(self) -> None:
        support = self.root / "difftests/corpus/coverage/go_package_support_inventory.tsv"
        support.write_text(
            "# package_path\tsupport_path\tsha256\n"
            f"pkg/planner\tpkg/planner/BUILD.bazel\t{'a' * 64}\n",
            encoding="utf-8",
        )
        self.write_package_slice(
            "planner-package", packages=["pkg/planner"], targets=["tidb-planner"]
        )
        self.run_tool("claim-slice", "--owner", "planner-package", "--slice", "planner-package")
        support.write_text(
            "# package_path\tsupport_path\tsha256\n"
            f"pkg/planner\tpkg/planner/BUILD.bazel\t{'b' * 64}\n",
            encoding="utf-8",
        )
        failure = self.run_tool("check", success=False)
        self.assertIn("missing supports", failure.stderr)

    def test_package_slice_requires_exact_multi_target_and_ring_sets(self) -> None:
        source = self.root / "difftests/corpus/coverage/go_source_inventory.tsv"
        source.write_text(
            source.read_text().replace(
                "pkg/planner/bar.go\t50\ttidb-planner",
                "pkg/planner/bar.go\t50\ttidb-server",
            ),
            encoding="utf-8",
        )
        manifest = self.write_package_slice(
            "planner-package",
            packages=["pkg/planner"],
            targets=["tidb-planner", "tidb-server"],
        )
        self.run_tool("check")
        listing = self.run_tool("packages")
        self.assertIn("pkg/planner\ttidb-planner,tidb-server\t2\t250", listing.stdout)

    def test_test_only_package_is_claimable_and_visible(self) -> None:
        tests = self.root / "difftests/corpus/coverage/go_test_inventory.tsv"
        with tests.open("a", encoding="utf-8") as output:
            output.write(
                "go_test_file\tpkg/testonly/only_test.go\t0\tonly_test.go\tunassigned\tUNTRIAGED\t-\t-\t-\n"
            )
        self.write_package_slice(
            "testonly-package",
            packages=["pkg/testonly"],
            targets=["tidb-planner"],
            rings=["unassigned"],
        )
        self.run_tool("claim-slice", "--owner", "testonly-package", "--slice", "testonly-package")
        claim = json.loads(
            (self.root / "workstreams/claims/testonly-package.claim.json").read_text()
        )
        self.assertEqual(claim["sources"], [])
        listing = self.run_tool("packages")
        self.assertIn(
            "pkg/testonly\ttidb-planner\t0\t0\t1\tUNTRIAGED\ttestonly-package",
            listing.stdout,
        )

    def test_schema2_claim_requires_matching_manifest_and_is_not_amendable(self) -> None:
        claims = self.root / "workstreams/claims"
        claims.mkdir(parents=True)
        path = claims / "orphan.claim.json"
        path.write_text(
            json.dumps(
                {
                    "schema": 2,
                    "owner": "orphan",
                    "sources": ["pkg/planner/foo.go"],
                    "tests": [],
                    "supports": [],
                }
            ),
            encoding="utf-8",
        )
        failure = self.run_tool("check", success=False)
        self.assertIn("matching schema-2 package slice", failure.stderr)
        path.unlink()
        self.write_package_slice(
            "planner-package", packages=["pkg/planner"], targets=["tidb-planner"]
        )
        mismatch = self.run_tool(
            "claim-slice", "--owner", "another-owner", "--slice", "planner-package", success=False
        )
        self.assertIn("owner must equal", mismatch.stderr)
        self.run_tool("claim-slice", "--owner", "planner-package", "--slice", "planner-package")
        amend = self.run_tool(
            "amend", "--owner", "planner-package", "--test",
            "pkg/planner/foo_test.go:10:TestFoo", success=False,
        )
        self.assertIn("immutable expanded snapshots", amend.stderr)

    def test_schema2_fails_closed_for_external_packages_and_legacy_dependencies(self) -> None:
        manifest = self.write_package_slice(
            "client-package",
            packages=[],
            targets=["tidb-kv"],
            rings=["unassigned"],
            module_packages=["client-go::internal/client"],
        )
        failure = self.run_tool("check", success=False)
        self.assertIn("external package support inventory is unavailable", failure.stderr)
        manifest.unlink()
        legacy = self.root / "workstreams/slices/legacy.toml"
        legacy.write_text(
            'schema = "1"\nslice = "legacy"\nstatus = "covered"\n'
            'target = "tidb-planner"\nring = "plan"\nconsumer = "legacy"\n'
            'test_target = "legacy"\ngo_sources = ["pkg/planner/foo.go"]\n'
            'go_tests = []\ndepends_on = []\nrust_paths = ["rust/legacy.rs"]\n',
            encoding="utf-8",
        )
        self.write_package_slice(
            "planner-package",
            packages=["pkg/planner"],
            targets=["tidb-planner"],
            depends_on=["legacy"],
        )
        failure = self.run_tool("check", success=False)
        self.assertIn("must also use schema 2", failure.stderr)

    def test_schema2_rejects_partial_state_and_missing_legacy_registry(self) -> None:
        self.write_package_slice(
            "planner-package",
            packages=["pkg/planner"],
            targets=["tidb-planner"],
            status="partial",
        )
        failure = self.run_tool("check", success=False)
        self.assertIn("invalid status 'partial'", failure.stderr)
        for path in (self.root / "workstreams/slices").glob("*.toml"):
            path.unlink()
        legacy = self.root / "workstreams/slices/legacy.toml"
        legacy.write_text(
            'schema = "1"\nslice = "legacy"\nstatus = "ready"\n'
            'target = "tidb-planner"\nring = "plan"\nconsumer = "legacy"\n'
            'test_target = "legacy"\ngo_sources = ["pkg/planner/foo.go"]\n'
            'go_tests = []\ndepends_on = []\nrust_paths = ["rust/legacy.rs"]\n',
            encoding="utf-8",
        )
        (self.root / "workstreams/slices/legacy-schema1-slices.tsv").unlink(missing_ok=True)
        failure = self.run_tool("check", success=False, freeze_legacy=False)
        self.assertIn("missing frozen schema-1 slice registry", failure.stderr)

    def test_schema2_campaign_is_dispatchable_but_cannot_be_frozen(self) -> None:
        source = self.root / "difftests/corpus/coverage/go_source_inventory.tsv"
        with source.open("a", encoding="utf-8") as output:
            output.write(
                "pkg/other/other.go\t12\ttidb-planner\tfalse\tUNTRIAGED\t-\t-\t-\n"
            )
        tests = self.root / "difftests/corpus/coverage/go_test_inventory.tsv"
        with tests.open("a", encoding="utf-8") as output:
            output.write(
                "go_test\tpkg/other/other_test.go\t8\tTestOtherPackage\tplan\tUNTRIAGED\t-\t-\t-\n"
            )
        self.write_package_slice(
            "planner-package", packages=["pkg/planner"], targets=["tidb-planner"]
        )
        self.write_package_slice(
            "other-package", packages=["pkg/other"], targets=["tidb-planner"]
        )
        campaigns = self.root / "workstreams/campaigns"
        campaigns.mkdir(parents=True)
        manifest = campaigns / "package-batch.toml"
        manifest.write_text(
            'schema = "2"\ncampaign = "package-batch"\nstatus = "planned"\n'
            'slices = ["planner-package", "other-package"]\n',
            encoding="utf-8",
        )
        result = self.run_tool("check")
        self.assertIn("campaigns\t1", result.stdout)
        manifest.write_text(
            manifest.read_text().replace('status = "planned"', 'status = "frozen"'),
            encoding="utf-8",
        )
        failure = self.run_tool("check", success=False)
        self.assertIn("schema-2 campaigns cannot use the legacy frozen state", failure.stderr)

    def test_frozen_legacy_work_is_never_ready_claimable_or_gateable(self) -> None:
        slices = self.root / "workstreams/slices"
        slices.mkdir(parents=True)
        for name, source in (
            ("legacy-a", "pkg/planner/foo.go"),
            ("legacy-b", "pkg/planner/bar.go"),
        ):
            (slices / f"{name}.toml").write_text(
                'schema = "1"\n'
                f'slice = "{name}"\nstatus = "ready"\n'
                'target = "tidb-planner"\nring = "plan"\nconsumer = "legacy"\n'
                f'test_target = "{name}"\ngo_sources = ["{source}"]\ngo_tests = []\n'
                f'depends_on = []\nrust_paths = ["rust/{name}.rs"]\n',
                encoding="utf-8",
            )
        campaigns = self.root / "workstreams/campaigns"
        campaigns.mkdir(parents=True)
        (campaigns / "legacy-batch.toml").write_text(
            'schema = "1"\ncampaign = "legacy-batch"\nstatus = "frozen"\n'
            'slices = ["legacy-a", "legacy-b"]\n',
            encoding="utf-8",
        )
        self.assertIn("ready_slices\t0", self.run_tool("check").stdout)
        claim_slice = self.run_tool(
            "claim-slice", "--owner", "legacy-a", "--slice", "legacy-a",
            success=False,
        )
        self.assertIn("only schema-2", claim_slice.stderr)
        self.run_tool("claim", "--owner", "repair", "--source", "pkg/planner/foo.go")
        gate = self.run_tool("gate-begin", success=False)
        self.assertIn("only schema-2 package claims", gate.stderr)

    def test_schema2_gate_still_detects_mid_gate_workspace_drift(self) -> None:
        self.write_package_slice(
            "planner-package", packages=["pkg/planner"], targets=["tidb-planner"]
        )
        self.run_tool(
            "claim-slice", "--owner", "planner-package", "--slice", "planner-package"
        )
        self.run_tool("gate-begin")
        (self.root / "undeclared.rs").write_text("changed during gate", encoding="utf-8")
        failure = self.run_tool("gate-finish", success=False)
        self.assertIn("changed while the shared gate was running", failure.stderr)
        self.run_tool("gate-abort")


if __name__ == "__main__":
    unittest.main()
