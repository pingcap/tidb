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

"""Regression tests for atomic whole-package campaign close."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("campaign_close.py")
GATE_SCRIPT = Path(__file__).with_name("rewrite-gate.sh")
SPEC = importlib.util.spec_from_file_location("campaign_close", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
campaign_close = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign_close)


class CampaignCloseTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.repository = Path(self.temporary.name)
        self.root = self.repository / "rust"
        (self.repository / "go.mod").write_text(
            "module github.com/pingcap/tidb\n\ngo 1.25\n", encoding="utf-8"
        )
        self.root.mkdir()
        (self.root / "Cargo.toml").write_text(
            '[workspace]\nmembers = ["."]\n\n'
            '[package]\nname = "tidb-planner"\nversion = "0.0.0"\n',
            encoding="utf-8",
        )
        coverage = self.root / "difftests/corpus/coverage"
        (coverage / "evidence/source").mkdir(parents=True)
        (coverage / "evidence/tests").mkdir(parents=True)
        (coverage / "evidence/transfers").mkdir(parents=True)
        (coverage / "external_go_source_inventory.tsv").write_text(
            "# external source inventory\n", encoding="utf-8"
        )
        (coverage / "external_go_test_inventory.tsv").write_text(
            "# external test inventory\n", encoding="utf-8"
        )

        slices = self.root / "workstreams/slices"
        campaigns = self.root / "workstreams/campaigns"
        claims = self.root / "workstreams/claims"
        evidence = self.root / "workstreams/package-evidence"
        slices.mkdir(parents=True)
        campaigns.mkdir(parents=True)
        claims.mkdir(parents=True)
        evidence.mkdir(parents=True)
        (slices / "legacy-schema1-slices.tsv").write_text(
            "# frozen legacy slices\n", encoding="utf-8"
        )
        integration_seam = self.root / "integration.rs"
        integration_seam.write_text("shared integration seam\n", encoding="utf-8")

        source_rows = ["# source inventory\n"]
        test_rows = ["# test inventory\n"]
        support_rows = ["# package\tsupport_path\tsha256\n"]
        self.members = ("package-a", "package-b")
        for suffix, member in (("a", "package-a"), ("b", "package-b")):
            package = f"pkg/{suffix}"
            source = f"{package}/source.go"
            test = f"{package}/source_test.go:10:Test{suffix.upper()}"
            upstream_source = self.repository / source
            upstream_source.parent.mkdir(parents=True, exist_ok=True)
            upstream_source.write_text(f"package {suffix}\n", encoding="utf-8")
            (self.repository / f"{package}/source_test.go").write_text(
                f"package {suffix}\n", encoding="utf-8"
            )
            implementation = self.root / f"impl-{suffix}.rs"
            implementation.write_text(f"implementation {suffix}\n", encoding="utf-8")
            source_fragment = coverage / f"evidence/source/{member}.tsv"
            source_fragment.write_text(
                "# source_path\tstatus\towner\tevidence_artifact\tnote\n"
                f"{source}\tCOVERED\t{member}\trust/impl-{suffix}.rs\tcomplete\n",
                encoding="utf-8",
            )
            test_fragment = coverage / f"evidence/tests/{member}.tsv"
            test_fragment.write_text(
                "# kind\tsource_path\tsource_line\ttest_name\tstatus\towner\tevidence_artifact\tnote\n"
                f"go_test\t{package}/source_test.go\t10\tTest{suffix.upper()}\t"
                f"COVERED\t{member}\trust/impl-{suffix}.rs\tcomplete\n",
                encoding="utf-8",
            )
            source_rows.append(
                f"{source}\t20\ttidb-planner\tfalse\tCOVERED\t{member}\t"
                f"rust/difftests/corpus/coverage/evidence/source/{member}.tsv\tcomplete\n"
            )
            test_rows.append(
                f"go_test\t{package}/source_test.go\t10\tTest{suffix.upper()}\tplan\t"
                f"COVERED\t{member}\t"
                f"rust/difftests/corpus/coverage/evidence/tests/{member}.tsv\tcomplete\n"
            )
            support = self.repository / f"{package}/testdata/case.txt"
            support.parent.mkdir(parents=True)
            support.write_text(f"support {suffix}\n", encoding="utf-8")
            support_sha = hashlib.sha256(support.read_bytes()).hexdigest()
            support_anchor = f"{package}/testdata/case.txt@{support_sha}"
            support_rows.append(
                f"{package}\t{package}/testdata/case.txt\t{support_sha}\n"
            )
            (evidence / f"{member}-support.tsv").write_text(
                "# support_path\tsha256\tdisposition\tevidence_artifact\tnote\n"
                f"{package}/testdata/case.txt\t{support_sha}\t"
                f"test-transcreated\trust/impl-{suffix}.rs\toriginal fixture reviewed\n",
                encoding="utf-8",
            )
            (slices / f"{member}.toml").write_text(
                'schema = "2"\n'
                f'slice = "{member}"\n'
                'status = "active"\n'
                'targets = ["tidb-planner"]\n'
                'rings = ["plan"]\n'
                f'consumer = "{member}"\n'
                f'test_target = "{member}"\n'
                f'go_packages = ["{package}"]\n'
                'module_packages = []\n'
                'depends_on = []\n'
                f'rust_paths = ["rust/impl-{suffix}.rs"]\n',
                encoding="utf-8",
            )
            with (slices / f"{member}.toml").open("a", encoding="utf-8") as output:
                output.write('integration_paths = ["rust/integration.rs"]\n')
            (claims / f"{member}.claim.json").write_text(
                json.dumps(
                    {
                        "schema": 2,
                        "owner": member,
                        "sources": [source],
                        "tests": [test],
                        "supports": [support_anchor],
                        "rust_paths": [f"rust/impl-{suffix}.rs"],
                        "integration_paths": ["rust/integration.rs"],
                        "module_sources": [],
                        "module_tests": [],
                        "upstream_sha256": {
                            source: hashlib.sha256(
                                upstream_source.read_bytes()
                            ).hexdigest(),
                            f"{package}/source_test.go": hashlib.sha256(
                                (self.repository / f"{package}/source_test.go").read_bytes()
                            ).hexdigest(),
                            f"{package}/testdata/case.txt": support_sha,
                        },
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
        (coverage / "go_source_inventory.tsv").write_text(
            "".join(source_rows), encoding="utf-8"
        )
        (coverage / "go_test_inventory.tsv").write_text(
            "".join(test_rows), encoding="utf-8"
        )
        (coverage / "go_package_support_inventory.tsv").write_text(
            "".join(support_rows), encoding="utf-8"
        )
        self.campaign_path = campaigns / "campaign-x.toml"
        self.campaign_path.write_text(
            'schema = "2"\ncampaign = "campaign-x"\nstatus = "active"\n'
            'slices = ["package-a", "package-b"]\n',
            encoding="utf-8",
        )
        self.archive_path = campaigns / "integrated-members.tsv"
        subprocess.run(["git", "init", "-q"], cwd=self.repository, check=True)
        subprocess.run(["git", "add", "."], cwd=self.repository, check=True)
        subprocess.run(
            [
                "git",
                "-c",
                "user.name=Campaign Test",
                "-c",
                "user.email=campaign@example.com",
                "commit",
                "-qm",
                "fixture baseline",
            ],
            cwd=self.repository,
            check=True,
        )
        base_commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=self.repository,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        for member in self.members:
            claim_path = claims / f"{member}.claim.json"
            claim = json.loads(claim_path.read_text(encoding="utf-8"))
            claim["base_commit"] = base_commit
            claim_path.write_text(
                json.dumps(claim, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_integrate_fails_static_before_expensive_workspace_work(self) -> None:
        integrate = GATE_SCRIPT.read_text(encoding="utf-8").split(
            "    integrate)", 1
        )[1]
        first_static = integrate.index("        static_gates")
        clippy = integrate.index("        cargo clippy")
        tests = integrate.index("        cargo test")
        last_static = integrate.rindex("        static_gates")
        self.assertLess(first_static, clippy)
        self.assertLess(clippy, tests)
        self.assertGreater(last_static, tests)

    def test_prepare_regenerates_only_derived_close_surfaces(self) -> None:
        prepare = GATE_SCRIPT.read_text(encoding="utf-8").split(
            "prepare_generated() {", 1
        )[1].split("\n}", 1)[0]
        self.assertIn('go_source_ledger" --write', prepare)
        self.assertIn('go_test_ledger" --write-evidence', prepare)
        self.assertIn("status-dashboard.py --write", prepare)

    def _replace_once(self, path: Path, old: str, new: str) -> None:
        path.write_text(
            path.read_text(encoding="utf-8").replace(old, new, 1),
            encoding="utf-8",
        )

    def _gate_runner(self, command: list[str], root: Path) -> None:
        if command == ["scripts/rewrite-gate.sh", "integrate"]:
            claims = campaign_close.queue.validate_claims(root)
            campaign_close.queue.atomic_write_json(
                root / campaign_close.queue.INTEGRATION_RECEIPT,
                campaign_close.queue.release_snapshot(root, claims),
            )
        elif command[-2:] == ["scripts/status-dashboard.py", "--write"]:
            (root / "STATUS.md").write_text("generated\n", encoding="utf-8")
        else:
            self.fail(f"unexpected command: {command}")

    def test_preflight_is_read_only_and_covers_all_members(self) -> None:
        before = self.campaign_path.read_bytes()
        plan = campaign_close.build_close_plan(self.root, "campaign-x")
        self.assertEqual(plan.members, self.members)
        self.assertEqual(plan.active_members, self.members)
        self.assertIn('status = "integrated"', plan.writes[self.campaign_path])
        self.assertEqual(self.campaign_path.read_bytes(), before)
        self.assertFalse(self.archive_path.exists())

    def test_prepare_refreshes_only_active_campaign_rows(self) -> None:
        source = self.root / "difftests/corpus/coverage/go_source_inventory.tsv"
        tests = self.root / "difftests/corpus/coverage/go_test_inventory.tsv"
        self._replace_once(
            source,
            "COVERED\tpackage-a\trust/difftests/corpus/coverage/"
            "evidence/source/package-a.tsv\tcomplete",
            "PARTIAL\tlegacy-owner\trust/difftests/corpus/coverage/"
            "evidence/source/package-a.tsv\tlegacy",
        )
        self._replace_once(
            tests,
            "COVERED\tpackage-a\trust/difftests/corpus/coverage/"
            "evidence/tests/package-a.tsv\tcomplete",
            "PARTIAL\tlegacy-owner\trust/difftests/corpus/coverage/"
            "evidence/tests/package-a.tsv\tlegacy",
        )

        def prepare(command: list[str], root: Path) -> None:
            self.assertEqual(command, ["scripts/rewrite-gate.sh", "prepare"])
            self._replace_once(
                source,
                "PARTIAL\tlegacy-owner\trust/difftests/corpus/coverage/"
                "evidence/source/package-a.tsv\tlegacy",
                "COVERED\tpackage-a\trust/difftests/corpus/coverage/"
                "evidence/source/package-a.tsv\tprepared",
            )
            self._replace_once(
                tests,
                "PARTIAL\tlegacy-owner\trust/difftests/corpus/coverage/"
                "evidence/tests/package-a.tsv\tlegacy",
                "COVERED\tpackage-a\trust/difftests/corpus/coverage/"
                "evidence/tests/package-a.tsv\tprepared",
            )
            (root / "STATUS.md").write_text("prepared\n", encoding="utf-8")

        plan = campaign_close.prepare_close_plan(
            self.root, "campaign-x", command_runner=prepare
        )

        self.assertEqual(plan.members, self.members)
        self.assertIn("evidence/source/package-a.tsv\tprepared", source.read_text())
        self.assertIn("evidence/tests/package-a.tsv\tprepared", tests.read_text())
        self.assertEqual((self.root / "STATUS.md").read_text(), "prepared\n")

    def test_prepare_rejects_unrelated_generated_churn_and_rolls_back(self) -> None:
        self.campaign_path.write_text(
            'schema = "2"\ncampaign = "campaign-x"\nstatus = "active"\n'
            'slices = ["package-a"]\n',
            encoding="utf-8",
        )
        (self.root / "workstreams/claims/package-b.claim.json").unlink()
        package_b = self.root / "workstreams/slices/package-b.toml"
        self._replace_once(package_b, 'status = "active"', 'status = "ready"')
        source = self.root / "difftests/corpus/coverage/go_source_inventory.tsv"
        before = source.read_bytes()

        def prepare(command: list[str], root: Path) -> None:
            self._replace_once(
                source,
                "COVERED\tpackage-b\trust/difftests/corpus/coverage/"
                "evidence/source/package-b.tsv\tcomplete",
                "COVERED\tpackage-a\trust/difftests/corpus/coverage/"
                "evidence/source/package-b.tsv\tspoofed",
            )
            (root / "STATUS.md").write_text("must roll back\n", encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "outside active campaign claims"):
            campaign_close.prepare_close_plan(
                self.root, "campaign-x", command_runner=prepare
            )

        self.assertEqual(source.read_bytes(), before)
        self.assertFalse((self.root / "STATUS.md").exists())

    def test_prepare_rejects_claimed_anchor_with_wrong_owner(self) -> None:
        source = self.root / "difftests/corpus/coverage/go_source_inventory.tsv"
        before = source.read_bytes()

        def prepare(command: list[str], root: Path) -> None:
            self._replace_once(
                source,
                "COVERED\tpackage-b\trust/difftests/corpus/coverage/"
                "evidence/source/package-b.tsv\tcomplete",
                "COVERED\tpackage-a\trust/difftests/corpus/coverage/"
                "evidence/source/package-b.tsv\twrong-owner",
            )

        with self.assertRaisesRegex(ValueError, "outside active campaign claims"):
            campaign_close.prepare_close_plan(
                self.root, "campaign-x", command_runner=prepare
            )

        self.assertEqual(source.read_bytes(), before)

    def test_single_package_campaign_closes_atomically(self) -> None:
        self.campaign_path.write_text(
            'schema = "2"\ncampaign = "campaign-x"\nstatus = "active"\n'
            'slices = ["package-a"]\n',
            encoding="utf-8",
        )
        (self.root / "workstreams/claims/package-b.claim.json").unlink()
        package_b = self.root / "workstreams/slices/package-b.toml"
        self._replace_once(package_b, 'status = "active"', 'status = "ready"')

        plan = campaign_close.build_close_plan(self.root, "campaign-x")
        self.assertEqual(plan.members, ("package-a",))
        self.assertEqual(plan.active_members, ("package-a",))
        campaign_close.apply_close_plan(
            self.root, plan, command_runner=self._gate_runner
        )

        self.assertEqual(
            self.archive_path.read_text(encoding="utf-8"),
            "campaign-x\tpackage-a\n",
        )
        self.assertTrue(
            (self.root / "workstreams/package-receipts/package-a.json").is_file()
        )
        self.assertIn('status = "ready"', package_b.read_text(encoding="utf-8"))

    def test_rejects_active_claim_outside_campaign(self) -> None:
        self.campaign_path.write_text(
            'schema = "2"\ncampaign = "campaign-x"\nstatus = "active"\n'
            'slices = ["package-a"]\n',
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "active claim package-b is outside"):
            campaign_close.build_close_plan(self.root, "campaign-x")

    def test_rejects_untriaged_package_row(self) -> None:
        inventory = self.root / "difftests/corpus/coverage/go_source_inventory.tsv"
        self._replace_once(inventory, "COVERED\tpackage-a", "UNTRIAGED\t-")
        with self.assertRaisesRegex(ValueError, "at COVERED"):
            campaign_close.build_close_plan(self.root, "campaign-x")

    def test_rejects_partial_package_row(self) -> None:
        inventory = self.root / "difftests/corpus/coverage/go_test_inventory.tsv"
        self._replace_once(inventory, "COVERED\tpackage-b", "PARTIAL\tpackage-b")
        with self.assertRaisesRegex(ValueError, "at COVERED"):
            campaign_close.build_close_plan(self.root, "campaign-x")

    def test_rejects_hand_authored_covered_without_covered_evidence(self) -> None:
        evidence = (
            self.root
            / "difftests/corpus/coverage/evidence/source/package-a.tsv"
        )
        self._replace_once(evidence, "COVERED", "PARTIAL")
        with self.assertRaisesRegex(ValueError, "evidence.*at COVERED"):
            campaign_close.build_close_plan(self.root, "campaign-x")

    def test_rejects_missing_campaign_member_claim(self) -> None:
        (self.root / "workstreams/claims/package-b.claim.json").unlink()
        with self.assertRaisesRegex(ValueError, "every member must be active"):
            campaign_close.build_close_plan(self.root, "campaign-x")

    def test_rejects_claim_missing_support_or_rust_path(self) -> None:
        claim_path = self.root / "workstreams/claims/package-a.claim.json"
        claim = json.loads(claim_path.read_text(encoding="utf-8"))
        claim["supports"] = []
        claim["rust_paths"] = []
        claim_path.write_text(json.dumps(claim), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "must exactly match"):
            campaign_close.build_close_plan(self.root, "campaign-x")

    def test_rejects_claim_integration_path_mismatch_and_missing_seam(self) -> None:
        claim_path = self.root / "workstreams/claims/package-a.claim.json"
        claim = json.loads(claim_path.read_text(encoding="utf-8"))
        claim["integration_paths"] = []
        claim_path.write_text(json.dumps(claim), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "integration paths"):
            campaign_close.build_close_plan(self.root, "campaign-x")
        claim["integration_paths"] = ["rust/integration.rs"]
        claim_path.write_text(json.dumps(claim), encoding="utf-8")
        (self.root / "integration.rs").unlink()
        with self.assertRaisesRegex(ValueError, "integration path is missing"):
            campaign_close.build_close_plan(self.root, "campaign-x")

    def test_rejects_missing_or_stale_support_and_disposition(self) -> None:
        support = self.repository / "pkg/a/testdata/case.txt"
        support.unlink()
        with self.assertRaisesRegex(ValueError, "missing or replaced"):
            campaign_close.build_close_plan(self.root, "campaign-x")
        support.write_text("changed\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "stale sha256"):
            campaign_close.build_close_plan(self.root, "campaign-x")

    def test_rejects_missing_duplicate_and_stale_support_evidence(self) -> None:
        evidence = self.root / "workstreams/package-evidence/package-a-support.tsv"
        original = evidence.read_text(encoding="utf-8")
        evidence.unlink()
        with self.assertRaisesRegex(ValueError, "missing package support evidence"):
            campaign_close.build_close_plan(self.root, "campaign-x")
        evidence.write_text(original + original.splitlines()[-1] + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "duplicate support evidence"):
            campaign_close.build_close_plan(self.root, "campaign-x")
        evidence.write_text(original.replace("test-transcreated", "unreviewed"), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "invalid support disposition"):
            campaign_close.build_close_plan(self.root, "campaign-x")

    def test_gate_failure_rolls_back_every_status_and_receipt(self) -> None:
        plan = campaign_close.build_close_plan(self.root, "campaign-x")
        before = {
            path: path.read_bytes() if path.exists() else None
            for path in plan.touched_paths
        }

        def fail(command: list[str], root: Path) -> None:
            (root / campaign_close.queue.INTEGRATION_ATTEMPT).write_text(
                "attempt\n", encoding="utf-8"
            )
            raise RuntimeError("gate failed")

        with self.assertRaisesRegex(RuntimeError, "gate failed"):
            campaign_close.apply_close_plan(self.root, plan, command_runner=fail)
        for path, content in before.items():
            self.assertEqual(path.read_bytes() if path.exists() else None, content)
        self.assertFalse((self.root / campaign_close.queue.INTEGRATION_ATTEMPT).exists())
        self.assertFalse((self.root / campaign_close.queue.INTEGRATION_RECEIPT).exists())

    def test_receipt_mismatch_rolls_back_without_release(self) -> None:
        plan = campaign_close.build_close_plan(self.root, "campaign-x")

        def mismatch(command: list[str], root: Path) -> None:
            if command == ["scripts/rewrite-gate.sh", "integrate"]:
                receipt = campaign_close.queue.release_snapshot(
                    root, campaign_close.queue.validate_claims(root)
                )
                del receipt["claims"]["package-b"]
                campaign_close.queue.atomic_write_json(
                    root / campaign_close.queue.INTEGRATION_RECEIPT, receipt
                )

        with self.assertRaisesRegex(ValueError, "exact active schema-2 claim set"):
            campaign_close.apply_close_plan(
                self.root, plan, command_runner=mismatch
            )
        self.assertTrue(
            (self.root / "workstreams/claims/package-a.claim.json").exists()
        )
        self.assertIn('status = "active"', self.campaign_path.read_text())

    def test_workspace_receipt_mismatch_rolls_back(self) -> None:
        plan = campaign_close.build_close_plan(self.root, "campaign-x")

        def mismatch(command: list[str], root: Path) -> None:
            if command == ["scripts/rewrite-gate.sh", "integrate"]:
                receipt = campaign_close.queue.release_snapshot(
                    root, campaign_close.queue.validate_claims(root)
                )
                receipt["workspace_sha256"] = "0" * 64
                campaign_close.queue.atomic_write_json(
                    root / campaign_close.queue.INTEGRATION_RECEIPT, receipt
                )

        with self.assertRaisesRegex(ValueError, "workspace digest differs"):
            campaign_close.apply_close_plan(
                self.root, plan, command_runner=mismatch
            )
        self.assertTrue(
            (self.root / "workstreams/claims/package-b.claim.json").exists()
        )

    def test_success_creates_durable_exact_package_receipts(self) -> None:
        plan = campaign_close.build_close_plan(self.root, "campaign-x")
        campaign_close.apply_close_plan(
            self.root, plan, command_runner=self._gate_runner
        )
        self.assertIn('status = "integrated"', self.campaign_path.read_text())
        self.assertEqual(
            self.archive_path.read_text(encoding="utf-8"),
            "campaign-x\tpackage-a\ncampaign-x\tpackage-b\n",
        )
        self.assertFalse((self.root / campaign_close.queue.INTEGRATION_RECEIPT).exists())
        for member in self.members:
            self.assertFalse(
                (self.root / f"workstreams/claims/{member}.claim.json").exists()
            )
            manifest = self.root / f"workstreams/slices/{member}.toml"
            self.assertIn('status = "covered"', manifest.read_text())
            record = campaign_close.queue.load_slices(self.root)[member]
            receipt = campaign_close.queue.validate_package_receipt(
                self.root, member, record
            )
            self.assertEqual(receipt["gate"]["result"], "passed")
            self.assertEqual(len(receipt["package"]["supports"]), 1)
            self.assertEqual(
                receipt["package"]["integration_paths"], ["rust/integration.rs"]
            )
            self.assertEqual(len(receipt["package"]["inventory_sha256"]), 64)
        (self.root / "integration.rs").write_text(
            "later steward edit\n", encoding="utf-8"
        )
        for member in self.members:
            record = campaign_close.queue.load_slices(self.root)[member]
            campaign_close.queue.validate_package_receipt(self.root, member, record)
        package_a_record = campaign_close.queue.load_slices(self.root)["package-a"]
        (self.root / "impl-a.rs").write_text(
            "stale stable leaf\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(ValueError, "artifact digest is stale"):
            campaign_close.queue.validate_package_receipt(
                self.root, "package-a", package_a_record
            )
        (self.root / "workstreams/package-receipts/package-a.json").unlink()
        with self.assertRaisesRegex(ValueError, "invalid package receipt"):
            campaign_close.queue.load_slices(self.root)

    def test_existing_package_receipt_is_never_overwritten(self) -> None:
        receipts = self.root / campaign_close.queue.PACKAGE_RECEIPTS_DIR
        receipts.mkdir(parents=True)
        receipt = receipts / "package-a.json"
        receipt.write_text("immutable\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "cannot be overwritten"):
            campaign_close.build_close_plan(self.root, "campaign-x")
        self.assertEqual(receipt.read_text(), "immutable\n")


if __name__ == "__main__":
    unittest.main()
