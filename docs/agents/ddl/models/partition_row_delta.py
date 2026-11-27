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

"""Finite row-protocol model; run from repository root with Python 3.

This checks abstract atomic transactions with write conflicts, not TiKV semantics.
Two target identities are assumed stable and injective. No indexes/GC/import RPCs
are modeled. --unsafe removes the Capture transaction drain before RowMerge.
--suite enumerates 513 bounded histories; --mutation deliberately breaks delete
capture or importer fencing to produce counterexamples. --replay-all checks
the historical full-value protocol. --reconcile checks dirty-key/source-read
repair. --startup includes a pre-S uncaptured writer; full-value replay is unsafe
there. Source tuples are canonical target projections, not a physical scanner.
"""
import argparse
import itertools
from dataclasses import dataclass, replace

OPS = (((0, 'a'),), ((0, None), (1, 'b')), ((1, None),), ((0, 'c'),))


@dataclass(frozen=True)
class State:
    phase: int = 0  # Capture, Ready, Merging, Direct
    imported: bool = False
    source: tuple = ('seed', None)
    baseline: tuple = ('seed', None)
    target: tuple = (None, None)
    delta: tuple = (None, None)  # None or (capture/dual/value, value); None value = Delete
    versions: tuple = (0, 0)
    op: int = 0
    pending: object = None  # (operation number, writer phase)
    cursor: int = 0
    merge: object = None  # (key, observed delta, observed version, source at transaction read)


def put(values, key, value):
    result = list(values)
    result[key] = value
    return tuple(result)


def successors(s, unsafe, ops=OPS, mutation=None, replay_all=False, reconcile=False):
    if s.op < len(ops) and s.pending is None:
        yield 'begin DML', replace(s, pending=(s.op, s.phase))
    if s.pending is not None:
        op, phase = s.pending
        src, dst, delta, versions = s.source, s.target, s.delta, s.versions
        for key, value in ops[op]:
            src = put(src, key, value)
            if phase >= 1:
                dst = put(dst, key, value)
            if phase < 3:
                if not (mutation == 'missing-delete' and value is None and phase == 0):
                    kind = 'dirty' if reconcile else ('value' if replay_all else ('capture' if phase == 0 else 'dual'))
                    stored = versions[key] + 1 if reconcile else value
                    delta = put(delta, key, (kind, stored))
                versions = put(versions, key, versions[key] + 1)
        yield 'commit DML', replace(s, source=src, target=dst, delta=delta,
                                   versions=versions, pending=None, op=op + 1)
    if mutation == 'late-import' and s.phase == 3 and s.target != s.baseline:
        yield 'late baseline replay after Direct', replace(s, target=s.baseline)
    if s.phase == 0:
        if not s.imported:
            yield 'import fixed baseline', replace(s, imported=True, target=s.baseline)
        if s.imported:
            yield 'publish Ready (importer fenced)', replace(s, phase=1)
    if s.phase == 1:
        if unsafe or s.pending is None or s.pending[1] != 0:
            yield 'drain Capture and enter Merging', replace(s, phase=2)
    if s.phase == 2:
        if s.cursor < 2 and s.merge is None:
            key = s.cursor
            marker = None if mutation == 'stale-scan' else s.delta[key]
            source_value = s.baseline[key] if mutation == 'stale-source' else s.source[key]
            yield 'read merge delta', replace(s, merge=(key, marker, s.versions[key], source_value))
        if s.merge is not None:
            key, value, version, source_value = s.merge
            # An absent marker is read-only: ordinary snapshot isolation does
            # not validate that read. Present markers are deleted (write-set).
            if value is not None and s.versions[key] != version:
                yield 'merge conflict; retry', replace(s, merge=None)
            elif value is None:
                yield 'advance empty range', replace(s, cursor=key + 1, merge=None)
            else:
                dst = s.target
                if reconcile:
                    dst = put(dst, key, source_value)
                elif replay_all or value[0] == 'capture':
                    dst = put(dst, key, value[1])
                yield 'commit merge and advance', replace(
                    s, target=dst, delta=put(s.delta, key, None),
                    versions=put(s.versions, key, version + 1), cursor=key + 1, merge=None)
        if s.cursor == 2:
            yield 'publish Direct', replace(s, phase=3)


def explore(ops, unsafe=False, mutation=None, replay_all=False, reconcile=False, initial=None):
    initial = initial or State()
    parents = {initial: None}
    queue = [initial]
    terminals = 0
    for s in queue:
        if s.phase == 3:
            if s.pending is None and s.op == len(ops):
                terminals += 1
            if s.source != s.target:
                trace = []
                node = s
                while parents[node] is not None:
                    prev, action = parents[node]
                    trace.append(action)
                    node = prev
                return len(parents), terminals, (trace[::-1], s)
        for action, next_state in successors(s, unsafe, ops, mutation, replay_all, reconcile):
            if next_state not in parents:
                parents[next_state] = (s, action)
                queue.append(next_state)
    assert terminals, 'no completed executions explored'
    return len(parents), terminals, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--unsafe', action='store_true')
    parser.add_argument('--mutation', choices=['missing-delete', 'late-import', 'stale-scan', 'stale-source'])
    parser.add_argument('--suite', action='store_true')
    parser.add_argument('--replay-all', action='store_true',
                        help='store no applied flag; replay every temp value transactionally')
    parser.add_argument('--reconcile', action='store_true',
                        help='store only dirty keys and reread source in the merge transaction')
    parser.add_argument('--startup', nargs='?', const='put', choices=('put', 'delete'),
                        help='pre-S stale Put or DeleteOnly partial Delete remains while S reads v2')
    args = parser.parse_args()
    assert not (args.replay_all and args.reconcile), 'choose replay or reconcile'
    histories = [OPS]
    if args.suite:
        alphabet = (((0, 'a'),), ((0, 'b'),), ((0, None),),
                    ((1, 'a'),), ((1, 'b'),), ((1, None),),
                    ((0, None), (1, 'a')), ((1, None), (0, 'b')))
        # Includes abstract operations whose source row may be absent. This is
        # an overapproximation of logical writes, not a SQL legality checker.
        histories = list(itertools.product(alphabet, repeat=3)) + [OPS]
    initial = None
    if args.startup:
        histories = [()]
        # Reachable at the capture-drain fence: new-schema writer logged v1,
        # then an old-schema RC writer committed v2 without capture. S is v2.
        stored = ('dirty', 1) if args.reconcile else ('value', None if args.startup == 'delete' else 'v1')
        initial = State(source=('v2', None), baseline=('v2', None),
                        delta=(stored, None), versions=(1, 0))
    states = terminals = 0
    for ops in histories:
        count, ends, failure = explore(ops, args.unsafe, args.mutation, args.replay_all, args.reconcile, initial)
        states += count
        terminals += ends
        if failure is not None:
            trace, state = failure
            print('COUNTEREXAMPLE:', ' -> '.join(trace))
            print('source:', state.source, 'target:', state.target)
            return 1
    print(f'PASS: {len(histories)} histories; {states} states; {terminals} terminal states; source == target in every Direct state')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
