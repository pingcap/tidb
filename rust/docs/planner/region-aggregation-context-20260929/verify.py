#!/usr/bin/env python3
"""Verify the pinned captures and the executable aggregate regression fixture."""
import gzip
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FIXTURE = ROOT.parents[2] / 'crates/tidb-unistore/testdata/region-aggregate-go.tsv.gz'


def rows(name):
    return [line.split('\t') for line in gzip.decompress((ROOT / name).read_bytes()).decode().splitlines()]


def valid(row):
    parts = row[0].split('/')
    return not (parts[0] == 'Count' and parts[1] not in ('Int', 'UInt')
                and parts[-1] in ('mode1', 'mode3'))


go = rows('go-baseline.tsv.gz')
rust = rows('rust-baseline.tsv.gz')
assert len(go) == len(rust) == 1932
assert all(g[:2] == r[:2] for g, r in zip(go, rust))
assert sum((g[5], g[7]) != (r[2], r[4]) for g, r in zip(go, rust)) == 513
assert sum(valid(g) and (g[5], g[7]) != (r[2], r[4]) for g, r in zip(go, rust)) == 387
expanded = rows('go-expanded.tsv.gz')
assert len(expanded) == 3297
filtered = [row for row in expanded if valid(row)]
assert len(filtered) == 3129
assert all(row[5] not in ('panic', 'build_error') for row in filtered)
expected = ''.join('\t'.join(row) + '\n' for row in filtered).encode()
assert gzip.decompress(FIXTURE.read_bytes()) == expected
for line in (ROOT / 'SHA256SUMS').read_text().splitlines():
    expected_hash, path = line.split('  ', 1)
    assert hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == expected_hash, path
print('Verified captures, 168 exclusions, 3,129 regression rows, baseline differences and hashes.')
