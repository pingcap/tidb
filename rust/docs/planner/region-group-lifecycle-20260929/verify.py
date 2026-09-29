#!/usr/bin/env python3
"""Verify the grouping fixture, baseline failure inventory and receipt hashes."""
import gzip
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FIXTURE = ROOT.parents[2] / 'crates/tidb-unistore/testdata/region-group-go.tsv.gz'
rows = [line.split('\t') for line in gzip.decompress(FIXTURE.read_bytes()).decode().splitlines()]
assert len(rows) == 11232
assert all(len(row) == 6 and not row[4] and not row[5] for row in rows)
assert len({row[0] for row in rows}) == len({row[1] for row in rows}) == len(rows)
before = json.loads(gzip.decompress((ROOT / 'rust-before.json.gz').read_bytes()))
assert before['case_count'] == len(rows)
assert len(before['differences']) == 5824
names = {row[0] for row in rows}
failed = [difference.split(': Rust ', 1)[0] for difference in before['differences']]
assert len(set(failed)) == len(failed) and set(failed) <= names
for line in (ROOT / 'SHA256SUMS').read_text().splitlines():
    expected, path = line.split('  ', 1)
    assert hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == expected, path
print('Verified 11,232 distinct Go requests, 5,824 baseline differences and hashes.')
