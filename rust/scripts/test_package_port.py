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

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("package-port.py")
SPEC = importlib.util.spec_from_file_location("package_port", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
package_port = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = package_port
SPEC.loader.exec_module(package_port)


class PackagePortTest(unittest.TestCase):
    def test_inventory_is_complete_and_preserves_colons_in_subtests(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            package = root / "pkg" / "demo"
            package.mkdir(parents=True)
            (package / "demo.go").write_text(
                "package demo\nimport `github.com/pingcap/tidb/pkg/base`\n",
                encoding="utf-8",
            )
            (package / "demo_test.go").write_text(
                'package demo\nfunc TestDemo(t *testing.T) { t.Run("leader: healthy", func(t *testing.T) {}); t.Run(`raw:name`, func(t *testing.T) {}) }\n',
                encoding="utf-8",
            )
            (package / "BUILD.bazel").write_text("go_library()\n", encoding="utf-8")
            (package / "testdata").mkdir()
            (package / "testdata" / "case.txt").write_text("case\n", encoding="utf-8")
            (package / "fixtures").mkdir()
            (package / "fixtures" / "case.json").write_text("{}\n", encoding="utf-8")
            nested_package = package / "nested"
            nested_package.mkdir()
            (nested_package / "nested.go").write_text("package nested\n", encoding="utf-8")
            (nested_package / "private.txt").write_text("nested\n", encoding="utf-8")
            deep_package = package / "internal" / "deep"
            deep_package.mkdir(parents=True)
            (deep_package / "deep.go").write_text("package deep\n", encoding="utf-8")
            (deep_package / "private.txt").write_text("deep\n", encoding="utf-8")
            original_root = package_port.REPO_ROOT
            try:
                package_port.REPO_ROOT = root
                inventory = package_port.package_inventory("pkg/demo")
            finally:
                package_port.REPO_ROOT = original_root

            self.assertEqual(inventory.sources, ("pkg/demo/demo.go",))
            self.assertEqual(inventory.test_files, ("pkg/demo/demo_test.go",))
            self.assertEqual(
                inventory.supports,
                (
                    "pkg/demo/BUILD.bazel",
                    "pkg/demo/fixtures/case.json",
                    "pkg/demo/testdata/case.txt",
                ),
            )
            self.assertEqual(inventory.dependencies, ("pkg/base",))
            self.assertIn("pkg/demo/demo_test.go:2:TestDemo", inventory.tests)
            self.assertIn(
                "pkg/demo/demo_test.go:2:subtest:leader: healthy", inventory.tests
            )
            self.assertIn("pkg/demo/demo_test.go:2:subtest:raw:name", inventory.tests)

    def test_inventory_digest_changes_with_support_data(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            package = root / "pkg" / "demo"
            package.mkdir(parents=True)
            (package / "demo.go").write_text("package demo\n", encoding="utf-8")
            support = package / "fixture.json"
            support.write_text("{}\n", encoding="utf-8")
            original_root = package_port.REPO_ROOT
            try:
                package_port.REPO_ROOT = root
                before = package_port.package_inventory("pkg/demo").digest
                support.write_text('{"changed":true}\n', encoding="utf-8")
                after = package_port.package_inventory("pkg/demo").digest
            finally:
                package_port.REPO_ROOT = original_root
            self.assertNotEqual(before, after)

    def test_cargo_test_executables_are_deduplicated_and_test_only(self) -> None:
        messages = "\n".join(
            [
                '{"reason":"compiler-artifact","profile":{"test":true},"executable":"/tmp/a"}',
                '{"reason":"compiler-artifact","profile":{"test":true},"executable":"/tmp/a"}',
                '{"reason":"compiler-artifact","profile":{"test":false},"executable":"/tmp/b"}',
                '{"reason":"build-finished","success":true}',
            ]
        )
        self.assertEqual(package_port.cargo_test_executables(messages), ["/tmp/a"])
        self.assertTrue(package_port.is_aggregate_test_executable("/tmp/all-deadbeef"))
        self.assertFalse(package_port.is_aggregate_test_executable("/tmp/small-deadbeef"))


if __name__ == "__main__":
    unittest.main()
