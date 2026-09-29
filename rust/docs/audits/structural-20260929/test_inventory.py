#!/usr/bin/env python3
"""Test that the audit rejects missing artifacts, stale evidence and coverage gaps."""
import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('inventory', HERE / 'inventory.py')
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class InventoryChecks(unittest.TestCase):
    def test_nested_package_ownership(self):
        directories = {'rust/difftests', 'rust/difftests/parser-tests'}
        self.assertEqual(audit.owner('rust/difftests/parser-tests/tests/all.rs', directories),
                         'rust/difftests/parser-tests')
        self.assertEqual(audit.owner('rust/difftests/Cargo.toml', directories), 'rust/difftests')
        self.assertIsNone(audit.owner('rust/Cargo.toml', directories))

    def test_verifier_rejects_corruption(self):
        import gzip
        inventory = json.loads(gzip.decompress((HERE / 'inventory.json.gz').read_bytes()))
        original = audit.HERE
        try:
            with tempfile.TemporaryDirectory(prefix='tidb-audit-test-') as directory:
                audit.HERE = Path(directory)
                audit.write_json(audit.HERE / 'inventory.json.gz', inventory)
                with contextlib.redirect_stdout(io.StringIO()):
                    audit.verify()
                broken = copy.deepcopy(inventory)
                broken['go_shared_artifacts'].pop(next(iter(broken['go_shared_artifacts'])))
                audit.write_json(audit.HERE / 'inventory.json.gz', broken)
                with self.assertRaisesRegex(AssertionError, 'Go files missing'):
                    audit.verify()
                audit.write_json(audit.HERE / 'inventory.json.gz', inventory)
                audit.write_json(audit.HERE / 'coverage.json', {})
                with self.assertRaisesRegex(AssertionError, 'coverage ledger is incomplete'):
                    audit.verify()
                (audit.HERE / 'coverage.json').unlink()
                finding = json.loads((HERE / 'findings.json').read_text())[0]
                finding['evidence'][0]['needle'] = 'deliberately absent audit test marker 69743627'
                audit.write_json(audit.HERE / 'findings.json', [finding])
                with self.assertRaises(AssertionError):
                    audit.verify()
        finally:
            audit.HERE = original


if __name__ == '__main__':
    unittest.main()
