#!/usr/bin/env python3
"""Inventory pinned Go packages/Rust crates and verify structural-audit evidence.

Source mentions are mapping hints, never proof of implementation or parity.
Cargo reachability is a dependency relation, never proof of a live symbol call.
"""
import argparse
import collections
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
GO_REF = '12b639a1161cd5a60126a47277f5ad14c320fd4a'
RUST_REF = '2c120bc7face45b069dc197ab98a8f001724c140'


def git(*args):
    return subprocess.check_output(['git', '-C', str(REPO), *args])


def tree(ref):
    result = {}
    for row in git('ls-tree', '-rz', ref).split(b'\0'):
        if row:
            meta, name = row.split(b'\t', 1)
            mode, kind, oid = meta.decode().split()
            if kind == 'blob':
                result[name.decode()] = {'mode': mode, 'blob': oid}
    return result


class Blobs:
    def __init__(self):
        self.process = subprocess.Popen(['git', '-C', str(REPO), 'cat-file', '--batch'],
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    def read(self, oid):
        self.process.stdin.write((oid + '\n').encode())
        self.process.stdin.flush()
        header = self.process.stdout.readline().split()
        assert len(header) == 3 and header[1] == b'blob', header
        raw = self.process.stdout.read(int(header[2]))
        assert self.process.stdout.read(1) == b'\n'
        return raw

    def close(self):
        self.process.stdin.close()
        self.process.wait()
        self.process.stdout.close()
        assert self.process.returncode == 0


def owner(path, directories):
    parent = str(Path(path).parent)
    while parent != '.':
        if parent in directories:
            return parent
        parent = str(Path(parent).parent)
    return '.' if '.' in directories else None


def write_json(path, obj):
    raw = (json.dumps(obj, indent=2, sort_keys=True) + '\n').encode()
    path.write_bytes(gzip.compress(raw, mtime=0) if path.suffix == '.gz' else raw)


def build(metadata):
    go, rust = tree(GO_REF), tree(RUST_REF)
    packages = {str(Path(p).parent) for p in go if p.endswith('.go')}
    go_rows = {p: {'artifacts': {}, 'rust_mentions': [], 'semantic_status': 'not_fully_verified'}
               for p in sorted(packages)}
    shared = {}
    for path, meta in go.items():
        p = owner(path, packages)
        if p is None:
            shared[path] = meta
        else:
            go_rows[p]['artifacts'][path] = meta
    ws = set(metadata['workspace_members'])
    all_packages = {p['id']: p for p in metadata['packages']}
    root = next(p['id'] for p in metadata['packages'] if p['name'] == 'tidb-server')
    nodes = {n['id']: n for n in metadata['resolve']['nodes']}
    seen, pending = set(), [root]
    while pending:
        node = pending.pop()
        if node in seen:
            continue
        seen.add(node)
        for dep in nodes[node]['deps']:
            if any(k['kind'] != 'dev' for k in dep['dep_kinds']):
                pending.append(dep['pkg'])
    crates = {}
    for pid in sorted(ws):
        p = all_packages[pid]
        base = str(Path(p['manifest_path']).parent.relative_to(REPO))
        crates[p['name']] = {
            'directory': base,
            'server_nondev_dependency': pid in seen,
            'targets': [{'name': t['name'], 'kind': t['kind'],
                         'path': str(Path(t['src_path']).relative_to(REPO))} for t in p['targets']],
            'artifacts': {},
            'go_packages_mentioned': [], 'semantic_status': 'not_fully_verified',
        }
    crate_dirs = {v['directory']: k for k, v in crates.items()}
    for path, meta in rust.items():
        crate = crate_dirs.get(owner(path, crate_dirs))
        if crate:
            crates[crate]['artifacts'][path] = meta
    hints = re.compile(r'\b(?:pkg|cmd|br)/[A-Za-z0-9_./-]+')
    patterns = {
        'deferred_or_seed': re.compile(r'\b(?:SEED|DEFERRED|unported|unimplemented|later course)\b', re.I),
        'possible_unwired': re.compile(r'not (?:yet )?wired|no production caller|not on the (?:live|query) path', re.I),
        'explicit_failure_stub': re.compile(r'\b(?:todo!|unimplemented!)\s*\('),
    }
    candidates, ignored, source_count = [], [], 0
    blobs = Blobs()
    try:
        for path, meta in rust.items():
            if not path.startswith('rust/') or not path.endswith('.rs'):
                continue
            crate = crate_dirs.get(owner(path, crate_dirs))
            if crate is None:
                continue
            content = blobs.read(meta['blob']).decode(errors='replace')
            for number, line in enumerate(content.splitlines(), 1):
                if re.search(r'^\s*#\[ignore\b', line):
                    ignored.append({'path': path, 'line': number, 'text': line.strip(),
                                    'status': 'unverified_ignore_annotation'})
            if not path.startswith('rust/crates/') or '/src/' not in path:
                continue
            source_count += 1
            mentions = set()
            for match in hints.finditer(content):
                hint = match.group().rstrip('./-')
                package = hint if hint in packages else owner(hint, packages)
                if package:
                    mentions.add(package)
            crates[crate]['go_packages_mentioned'].extend(mentions)
            for package in mentions:
                go_rows[package]['rust_mentions'].append(path)
            for number, line in enumerate(content.splitlines(), 1):
                tags = [tag for tag, pattern in patterns.items() if pattern.search(line)]
                if tags:
                    candidates.append({'path': path, 'line': number, 'tags': tags,
                                       'text': line.strip(), 'status': 'unverified_text_match'})
    finally:
        blobs.close()
    for row in crates.values():
        row['go_packages_mentioned'] = sorted(set(row['go_packages_mentioned']))
    for row in go_rows.values():
        row['rust_mentions'] = sorted(set(row['rust_mentions']))
    inv = {
        'go_ref': GO_REF, 'rust_ref': RUST_REF, 'go_packages': go_rows,
        'go_shared_artifacts': shared, 'rust_workspace_packages': crates,
        'rust_nonworkspace_manifests': {p: m for p, m in rust.items()
                                      if p.startswith('rust/') and p.endswith('/Cargo.toml')
                                      and str(Path(p).parent) not in crate_dirs},
        'rust_shared_artifacts': {p: m for p, m in rust.items()
                                  if p.startswith('rust/') and owner(p, crate_dirs) is None},
        'scope': 'All tracked Go-package directories and workspace Cargo packages; no semantic-completion claims.',
    }
    write_json(HERE / 'inventory.json.gz', inv)
    write_json(HERE / 'candidates.json.gz', candidates)
    write_json(HERE / 'ignored-tests.json.gz', ignored)
    summary = {'go_packages': len(go_rows), 'go_package_artifacts': sum(len(x['artifacts']) for x in go_rows.values()),
               'go_shared_artifacts': len(shared), 'rust_workspace_packages': len(crates),
               'rust_source_files_scanned': source_count, 'source_mentioned_go_packages': sum(bool(x['rust_mentions']) for x in go_rows.values()),
               'server_nondev_workspace_dependencies': sum(x['server_nondev_dependency'] for x in crates.values()),
               'candidate_text_matches': len(candidates),
               'ignored_test_annotations': len(ignored),
               'rust_shared_artifacts': len(inv['rust_shared_artifacts']),
               'not_in_server_dependency_graph': sorted(k for k, x in crates.items() if not x['server_nondev_dependency'])}
    write_json(HERE / 'summary.json', summary)
    print(json.dumps(summary, indent=2))


def verify():
    inv = json.loads(gzip.decompress((HERE / 'inventory.json.gz').read_bytes()))
    go, rust = tree(inv['go_ref']), tree(inv['rust_ref'])
    all_go = dict(inv['go_shared_artifacts'])
    for package, row in inv['go_packages'].items():
        assert row['semantic_status'] == 'not_fully_verified'
        assert any(p.endswith('.go') and str(Path(p).parent) == package for p in row['artifacts'])
        assert not (all_go.keys() & row['artifacts'].keys())
        all_go.update(row['artifacts'])
    assert all_go == go, 'Go files missing, duplicated or stale'
    all_rust = dict(inv['rust_shared_artifacts'])
    crate_dirs = {v['directory'] for v in inv['rust_workspace_packages'].values()}
    expected_crates = {directory: {} for directory in crate_dirs}
    for path, meta in rust.items():
        directory = owner(path, crate_dirs)
        if directory:
            expected_crates[directory][path] = meta
    for row in inv['rust_workspace_packages'].values():
        assert row['artifacts'] == expected_crates[row['directory']]
        assert not (all_rust.keys() & row['artifacts'].keys())
        all_rust.update(row['artifacts'])
    assert all_rust == {p: m for p, m in rust.items() if p.startswith('rust/')}
    coverage_path = HERE / 'coverage.json'
    if coverage_path.exists():
        coverage = json.loads(coverage_path.read_text())
        assert set(coverage) == set(inv['rust_workspace_packages']), 'crate coverage ledger is incomplete'
        assert all(row['semantic_status'] == 'not_fully_verified' for row in coverage.values())
    findings_path = HERE / 'findings.json'
    if findings_path.exists():
        findings = json.loads(findings_path.read_text())
        ids = set()
        blobs = Blobs()
        try:
            for finding in findings:
                assert finding['id'] not in ids
                ids.add(finding['id'])
                assert finding['status'] in ('source_confirmed', 'runtime_confirmed', 'candidate', 'resolved')
                assert set(finding['go_packages']) <= inv['go_packages'].keys()
                assert {e['side'] for e in finding['evidence']} == {'go', 'rust'}
                for evidence in finding['evidence']:
                    files = go if evidence['side'] == 'go' else rust
                    assert evidence['path'] in files, evidence
                    text = blobs.read(files[evidence['path']]['blob']).decode()
                    assert evidence['needle'] in text, (finding['id'], evidence)
                    evidence['line'] = text[:text.index(evidence['needle'])].count('\n') + 1
                    evidence['blob'] = files[evidence['path']]['blob']
        finally:
            blobs.close()
        write_json(HERE / 'findings.json', findings)
        print('Verified evidence for', len(findings), 'findings.')
    checksums_path = HERE / 'checksums.json'
    if checksums_path.exists():
        for path, expected in json.loads(checksums_path.read_text()).items():
            assert hashlib.sha256((HERE / path).read_bytes()).hexdigest() == expected, path
    print('Verified every Go artifact and all Rust crate inventories against pinned Git trees.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['generate', 'verify'])
    parser.add_argument('--metadata', type=Path)
    args = parser.parse_args()
    if args.mode == 'generate':
        assert args.metadata, '--metadata must name full Cargo metadata for the pinned Rust revision'
        build(json.loads(args.metadata.read_text()))
    else:
        verify()
