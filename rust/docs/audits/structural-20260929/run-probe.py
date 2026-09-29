#!/usr/bin/env python3
"""Collect Rust-only observations; use only with no concurrent source writers."""
from pathlib import Path
import gzip
import subprocess

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
source = ROOT / 'rust/crates/tidb-server/src/cluster_session_node/tests/prepared_transactions.rs'
relative = str(source.relative_to(ROOT))
original = source.read_bytes()
assert original == subprocess.check_output(['git', 'show', f'HEAD:{relative}'], cwd=ROOT), 'preserve existing source edits'
try:
    source.write_bytes(original + (HERE / 'rust-probe.txt').read_bytes())
    with (HERE / 'rust-probe.log').open('w') as log:
        result = subprocess.run([
            'cargo', 'test', '--offline', '--locked', '--manifest-path', 'rust/Cargo.toml',
            '-p', 'tidb-server', '--lib', 'audit_workspace_structural_boundaries',
            '--', '--test-threads=1', '--nocapture',
        ], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
finally:
    source.write_bytes(original)
log_path = HERE / 'rust-probe.log'
(HERE / 'rust-probe.log.gz').write_bytes(gzip.compress(log_path.read_bytes(), mtime=0))
raise SystemExit(result.returncode)
