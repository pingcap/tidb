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

"""Compare complete upstream descriptors with the generated Rust protocol owners.

Run from the repository root with --go-ref origin/master. Requires Cargo, protoc and
Go module-cache access; checked-in sources and generated Rust are never edited.
The resulting JSON distinguishes omitted contracts from deliberate opaque wire
representations, and records a reproducible keyspace-zero presence example.
"""
import argparse
import collections
import hashlib
import json
import pathlib
import re
import shutil
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[2]
OUT = ROOT / 'rust/docs/parity/current-audit/protocol-contracts-after.json'

def run(*args, **kwargs):
    return subprocess.check_output(args, cwd=ROOT, **kwargs)

def module(name, version):
    return pathlib.Path(json.loads(run('go', 'mod', 'download', '-json', name + '@' + version))['Dir'])

# Minimal protobuf wire reader: only descriptor fields are interpreted; generator
# options and source-location records are skipped. No language-runtime dependency.
def fields(data):
    result = collections.defaultdict(list)
    pos = 0
    def varint():
        nonlocal pos
        value = shift = 0
        while True:
            byte = data[pos]
            pos += 1
            value |= (byte & 127) << shift
            if byte < 128:
                return value
            shift += 7
    while pos < len(data):
        tag = varint()
        number, wire = tag >> 3, tag & 7
        if wire == 0:
            value = varint()
        elif wire == 2:
            length = varint()
            value = data[pos:pos + length]
            pos += length
        elif wire in (1, 5):
            length = 8 if wire == 1 else 4
            value = data[pos:pos + length]
            pos += length
        else:
            raise ValueError(f'unexpected descriptor wire type {wire}')
        result[number].append(value)
    return result

def one(record, field, default=None):
    return record.get(field, [default])[0]

def string(record, field):
    return one(record, field, b'').decode()

def declarations(data, packages):
    result = {}
    def enums(raw, parent):
        enum = fields(raw)
        name = parent + '.' + string(enum, 1)
        result[name] = {'kind': 'enum', 'values': {
            string(v, 1): one(v, 2) for v in map(fields, enum.get(2, []))}}
    def messages(raw, parent):
        msg = fields(raw)
        name = parent + '.' + string(msg, 1)
        oneofs = [string(fields(v), 1) for v in msg.get(8, [])]
        result[name] = {'kind': 'message', 'fields': {}}
        for rawfield in msg.get(2, []):
            f = fields(rawfield)
            result[name]['fields'][string(f, 1)] = {
                'number': one(f, 3), 'label': one(f, 4), 'type': one(f, 5),
                'type_name': string(f, 6),
                'oneof': oneofs[one(f, 9)] if 9 in f else None,
                'proto3_optional': bool(one(f, 17, 0)), 'default': string(f, 7)}
        for child in msg.get(3, []):
            messages(child, name)
        for child in msg.get(4, []):
            enums(child, name)
    for raw in fields(data).get(1, []):
        file = fields(raw)
        package = string(file, 2)
        if package not in packages:
            continue
        parent = '.' + package
        for msg in file.get(4, []):
            messages(msg, parent)
        for enum in file.get(5, []):
            enums(enum, parent)
        for rawservice in file.get(6, []):
            service = fields(rawservice)
            name = parent + '.' + string(service, 1)
            result[name] = {'kind': 'service', 'methods': {}}
            for rawmethod in service.get(2, []):
                m = fields(rawmethod)
                result[name]['methods'][string(m, 1)] = {
                    'input': string(m, 2), 'output': string(m, 3),
                    'client_streaming': bool(one(m, 5, 0)),
                    'server_streaming': bool(one(m, 6, 0))}
    return result

def opaque_message_representation(upstream, local):
    """Recognize only a length-delimited representation with matching presence."""
    if local is None or upstream['type'] != 11 or local['type'] != 12:
        return False
    expected = dict(upstream, type=12, type_name='')
    # An optional bytes field uses a synthetic oneof to preserve the presence
    # that a singular message already has. Real oneof membership must match.
    if upstream['oneof'] is None and local['proto3_optional']:
        expected.update(oneof=local['oneof'], proto3_optional=True)
    return expected == local


def compile_proto(directory, names, includes, output):
    run('protoc', '-I' + str(directory), *['-I' + str(p) for p in includes],
        '--include_imports', '--descriptor_set_out=' + str(output),
        *[str(directory / name) for name in names])
    return output.read_bytes()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--go-ref', required=True)
    args = parser.parse_args()
    revision = run('git', 'rev-parse', args.go_ref + '^{commit}').decode().strip()
    gomod = run('git', 'show', revision + ':go.mod').decode()
    def pin(name):
        matches = re.findall(r'^\s*' + re.escape(name) + r'\s+(v\S+)', gomod, re.M)
        if len(matches) != 1:
            raise ValueError(f'expected one {name} module pin')
        return matches[0]
    kvpin = pin('github.com/pingcap/kvproto')
    etcdpin = pin('go.etcd.io/etcd/api/v3')
    kv = module('github.com/pingcap/kvproto', kvpin)
    etcd = module('go.etcd.io/etcd/api/v3', etcdpin)
    packages = {'pdpb', 'tikvpb', 'backup', 'etcdserverpb', 'mvccpb', 'authpb', 'membershippb'}
    build = run('cargo', 'check', '--locked', '--manifest-path', 'rust/Cargo.toml',
                '-p', 'tidb-proto', '--message-format=json').decode()
    outputs = {record['out_dir'] for line in build.splitlines()
               if (record := json.loads(line)).get('reason') == 'build-script-executed'
               and 'tidb-proto' in record['package_id']}
    if len(outputs) != 1:
        raise ValueError(f'expected one tidb-proto build output, found {outputs}')
    generated = pathlib.Path(outputs.pop())
    native = ROOT / 'rust/third_party/tikv-client-rs/kvproto/src/generated/file_descriptor_set.bin'
    with tempfile.TemporaryDirectory(prefix='tidb-proto-audit-') as tmp:
        tmp = pathlib.Path(tmp)
        for source in etcd.rglob('*.proto'):
            target = tmp / 'etcd/api' / source.relative_to(etcd)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
        local = declarations(native.read_bytes(), {'pdpb', 'backup'})
        local.update(declarations((generated / 'tikv_transport_descriptor.bin').read_bytes(), {'tikvpb'}))
        local.update(declarations((generated / 'upstream_descriptor.bin').read_bytes(), packages))
        upstream = declarations(compile_proto(kv/'proto', ['pdpb.proto', 'tikvpb.proto', 'brpb.proto'], [kv/'include'], tmp/'kv.bin'), packages)
        run('protoc', '-I' + str(tmp/'etcd/api/etcdserverpb'), '-I' + str(tmp), '-I' + str(kv/'include'),
            '--include_imports', '--descriptor_set_out=' + str(tmp/'etcd.bin'),
            *map(str, sorted((tmp/'etcd').rglob('*.proto'))))
        upstream.update(declarations((tmp/'etcd.bin').read_bytes(), packages))
        differences = []
        for name, go in sorted(upstream.items()):
            rust = local.get(name)
            if rust is None:
                differences.append({'name': name, 'difference': 'missing ' + go['kind']})
                continue
            assert go['kind'] == rust['kind'], name
            member_key = {'message': 'fields', 'enum': 'values', 'service': 'methods'}[go['kind']]
            unmatched = dict(rust[member_key])
            for member, contract in go[member_key].items():
                local_member = member
                if member_key == 'fields':
                    local_member = next((key for key, value in rust[member_key].items()
                                         if value['number'] == contract['number']), None)
                actual = unmatched.pop(local_member, None)
                if actual != contract:
                    difference = 'missing ' + member_key[:-1] if actual is None else 'contract differs'
                    if member_key == 'fields' and opaque_message_representation(contract, actual):
                        # Existing opaque transport optimization. Message and bytes
                        # are length-delimited; compare presence/decoding separately.
                        difference = 'opaque message representation'
                    differences.append({'name': name + '.' + member,
                        'difference': difference, 'local_name': local_member,
                        'upstream': contract, 'local': actual})
            for member in unmatched:
                differences.append({'name': name + '.' + member, 'difference': 'extra ' + member_key[:-1]})
        for name in sorted(local.keys() - upstream.keys()):
            differences.append({'name': name, 'difference': 'extra ' + local[name]['kind']})
        wire_examples = {}
        for label, descriptor in [('local', native), ('upstream', tmp/'kv.bin')]:
            wire_examples[label] = run('protoc', '--descriptor_set_in=' + str(descriptor),
                '--encode=pdpb.KeyspaceScope', input=b'keyspace_id: 0').hex()
        inputs = {
            'native_descriptor': hashlib.sha256(native.read_bytes()).hexdigest(),
            'transport_descriptor': hashlib.sha256((generated/'tikv_transport_descriptor.bin').read_bytes()).hexdigest(),
            'upstream_descriptor': hashlib.sha256((generated/'upstream_descriptor.bin').read_bytes()).hexdigest(),
        }
        counts = {p: dict(collections.Counter(d['difference'] for d in differences if d['name'].split('.')[1] == p)) for p in sorted(packages)}
        report = {'go_master': revision, 'integration': run('git','rev-parse','HEAD').decode().strip(),
            'kvproto': kvpin, 'etcd_api': etcdpin, 'protoc': run('protoc','--version').decode().strip(),
            'scope': 'Descriptor declarations, fields matched by wire tag, oneofs, defaults, enums and RPCs of all replacement protocol owners. Field spelling/JSON names are not compared. Generator options, reserved ranges, runtime behavior and dependency-package acceptance are not certified. Message-to-bytes changes are representations, not automatically wire defects.',
            'keyspace_zero_wire_hex': wire_examples, 'inputs': inputs, 'counts': counts, 'differences': differences}
        OUT.write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(counts, indent=2))
        print('differences', len(differences))
        if any(item['difference'] != 'opaque message representation' for item in differences):
            raise ValueError('Rust protocol owners differ from upstream contracts')
        if wire_examples['local'] != wire_examples['upstream']:
            raise ValueError('keyspace zero loses presence')


if __name__ == "__main__":
    main()
