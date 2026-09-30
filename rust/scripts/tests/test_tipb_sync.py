"""Regression checks for complete input membership and baseline selection."""
import importlib.util
import pathlib
import tempfile
import unittest
from unittest import mock

SOURCE = pathlib.Path(__file__).resolve().parents[1] / "sync-tipb.py"
spec = importlib.util.spec_from_file_location("sync_tipb", SOURCE)
sync = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sync)


class SourceContract(unittest.TestCase):
    def test_missing_changed_and_extra_inputs_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            expected = {"expression.proto": b"message Expr {}", "executor.proto": b"enum ExecType {}"}
            self.assertEqual(len(sync.differences(root, expected)), 2)
            for name, data in expected.items():
                (root / name).write_bytes(data)
            self.assertEqual(sync.differences(root, expected), [])
            (root / "expression.proto").write_bytes(b"message Expr { /* dropped field */ }")
            (root / "projection.proto").write_bytes(b"message DuplicateOwner {}")
            self.assertEqual(sync.differences(root, expected), [
                "extra local input: projection.proto", "modified upstream input: expression.proto"])

    def test_master_pin_is_explicit_and_unambiguous(self):
        self.assertEqual(sync.version_from_go_mod("require (\n github.com/pingcap/tipb v0.0.0-20260908093239-fed7bc47c39d\n)"),
                         "v0.0.0-20260908093239-fed7bc47c39d")
        for text in ["", "require github.com/example/tipb v1.0.0", "require github.com/pingcap/tipb v1.0.0\nrequire github.com/pingcap/tipb v2.0.0"]:
            with self.assertRaises(ValueError):
                sync.version_from_go_mod(text)

    def test_fetched_master_pin_cannot_silently_drift(self):
        with mock.patch.object(sync, "run", return_value="require github.com/pingcap/tipb v1.2.3"):
            sync.check_master_pin({"version": "v1.2.3"})
            with self.assertRaisesRegex(ValueError, "fetched Go master changed TiPB"):
                sync.check_master_pin({"version": "v1.2.2"})

    def test_etcd_includes_all_packages_and_new_schema_files(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(sync, "MODULE", "go.etcd.io/etcd/api/v3"):
            root = pathlib.Path(directory)
            (root / "LICENSE").write_bytes(b"upstream license")
            for package in ("authpb", "membershippb", "etcdserverpb", "mvccpb", "futurepb"):
                (root / package).mkdir()
                (root / package / "api.proto").write_bytes(package.encode())
            expected = {"etcd/api/" + name + "/api.proto": name.encode()
                        for name in ("authpb", "membershippb", "etcdserverpb", "mvccpb", "futurepb")}
            expected["LICENSE"] = b"upstream license"
            self.assertEqual(sync.schema_inputs(root), expected)
            self.assertEqual(sync.version_from_go_mod("require go.etcd.io/etcd/api/v3 v3.5.15"), "v3.5.15")
            with mock.patch.object(sync, "LABEL", "etcd API"), mock.patch.object(sync, "run", return_value="require go.etcd.io/etcd/api/v3 v3.5.16"):
                with self.assertRaisesRegex(ValueError, "fetched Go master changed etcd API"):
                    sync.check_master_pin({"version": "v3.5.15"})

    def test_recorded_pin_must_match_its_immutable_go_revision(self):
        source = {"module": sync.MODULE, "go_master": "revision", "version": "v1.2.3", "go_mod_sha256": "hash"}
        with mock.patch.object(sync, "selected_source", return_value=source):
            sync.check_recorded_source(source)
            with self.assertRaisesRegex(ValueError, "differs from its Go baseline"):
                sync.check_recorded_source({**source, "version": "v1.2.2"})
            with self.assertRaisesRegex(ValueError, "not TiPB"):
                sync.check_recorded_source({**source, "module": "another-module"})


if __name__ == "__main__":
    unittest.main()
