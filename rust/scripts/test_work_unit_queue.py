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

    def run_tool(self, *arguments: str, success: bool = True) -> subprocess.CompletedProcess[str]:
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

    def test_queue_pairs_only_same_directory_and_stem_as_candidate(self) -> None:
        result = self.run_tool(
            "queue", "--target", "tidb-planner", "--ring", "plan", "--limit", "1"
        )
        self.assertIn("same-directory-stem-candidate", result.stdout)
        self.assertIn("pkg/planner/foo_test.go:10:TestFoo", result.stdout)
        self.assertNotIn("TestOther", result.stdout)

    def test_claim_rejects_overlapping_source_and_release_unlocks_it(self) -> None:
        anchor = "pkg/planner/foo_test.go:10:TestFoo"
        self.run_tool(
            "claim", "--owner", "agent-a", "--source", "pkg/planner/foo.go", "--test", anchor
        )
        claim_path = self.root / "workstreams/claims/agent-a.claim.json"
        claim = json.loads(claim_path.read_text())
        self.assertEqual(claim["schema"], 2)
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
        self.assertIn("planner-consumer", ready.stdout)
        self.run_tool(
            "claim-slice", "--owner", "agent-a", "--slice", "planner-consumer"
        )
        claim = json.loads(
            (self.root / "workstreams/claims/agent-a.claim.json").read_text()
        )
        self.assertEqual(claim["sources"], ["pkg/planner/foo.go"])
        self.assertEqual(claim["tests"], ["pkg/planner/foo_test.go:10:TestFoo"])
        self.assertNotIn("planner-consumer", self.run_tool("ready").stdout)

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
        self.assertIn("client-runtime", self.run_tool("ready").stdout)
        self.run_tool(
            "claim-slice", "--owner", "client-runtime", "--slice", "client-runtime"
        )
        claim = json.loads(
            (self.root / "workstreams/claims/client-runtime.claim.json").read_text()
        )
        self.assertEqual(
            claim["module_sources"],
            ["client-go::internal/client/client0.go"],
        )
        self.assertEqual(
            claim["module_tests"],
            [
                "client-go::internal/client/client_test.go:20:TestSend0"
            ],
        )
        self.assertNotIn("client-runtime", self.run_tool("ready").stdout)

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
        self.run_tool("claim-slice", "--owner", "client-runtime", "--slice", "client-runtime")
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
        result = self.run_tool("check")
        self.assertIn("campaigns\t1", result.stdout)

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
        self.assertIn("campaign adds unregistered tidb-exec test targets", failure.stderr)
        self.assertIn(
            "exactly one member must own rust/crates/tidb-exec/Cargo.toml",
            failure.stderr,
        )

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
            'schema = "1"\n'
            'slice = "planner-consumer"\n'
            'status = "partial"\n'
            'target = "tidb-planner"\n'
            'ring = "plan"\n'
            'consumer = "frozen planner consumer"\n'
            'test_target = "planner_consumer_source"\n'
            'go_sources = ["pkg/planner/foo.go"]\n'
            'go_tests = ["pkg/planner/foo_test.go:10:TestFoo"]\n'
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
                    "sources": ["pkg/planner/foo.go"],
                    "tests": ["pkg/planner/foo_test.go:10:TestFoo"],
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
                    "sources": ["pkg/planner/foo.go"],
                    "tests": [],
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
        self.run_tool(
            "claim-slice", "--owner", "planner-consumer", "--slice", "planner-consumer"
        )
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
        self.run_tool("gate-begin")
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
        self.run_tool("gate-begin")
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
        self.run_tool("gate-begin")
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
        self.run_tool("gate-begin")
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
        self.run_tool("gate-begin")
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
        self.run_tool("claim-slice", "--owner", "slice-a", "--slice", "slice-a")
        self.assertNotIn("slice-b", self.run_tool("ready").stdout)
        self.assertIn("ready_slices\t0", self.run_tool("check").stdout)
        failure = self.run_tool(
            "claim-slice", "--owner", "slice-b", "--slice", "slice-b", success=False
        )
        self.assertIn("Rust path", failure.stderr)

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
        self.assertIn(
            "foo-contract:test:pkg/planner/foo_test.go:10:TestFoo@test-owner>=COVERED "
            "(current -@UNTRIAGED)",
            failure.stderr,
        )

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

    def test_check_rejects_campaign_below_batch_floor(self) -> None:
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
                result = self.run_tool("check", success=False)
                self.assertIn(
                    "campaign has 2 production sources; minimum is 9", result.stderr
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
            'status = "active"\n'
            'slices = ["planner-first", "planner-second"]\n',
            encoding="utf-8",
        )
        result = self.run_tool("check", success=False)
        self.assertIn(
            "campaign Rust path rust/shared.rs overlaps slices planner-first and planner-second",
            result.stderr,
        )


if __name__ == "__main__":
    unittest.main()
