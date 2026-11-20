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

"""Bounded read/prepare/commit model for the proposed shared-parent guard.

Two writers use a full snapshot and write-set version validation, not actual
TiKV locks or allocation caches. The default mode requires complete old/new copies. --lifecycle checks the
retirement drain after old-read users have already exited; it does not model
MDL itself, old-authority transactions, indexes, RPCs or live TiKV GC.
"""
import argparse
from copy import deepcopy


INITIAL = ((0, 7, 'A'), (1, 8, 'Z'), (2, 7, 'C'))
SCENARIOS = (
    (('insert', 2, 8, 'X'), ('insert', 3, 8, 'Y')),
    (('move', 0, 7, 3, 7), ('insert', 2, 8, 'X')),
    (('move', 0, 7, 2, 7), ('move', 1, 8, 3, 7)),
    (('delete', 2, 7), ('insert', 3, 7, 'X')),
    (('move', 0, 7, 1, 7), ('insert', 1, 7, 'X')),
    (('move', 0, 7, 3, 7), ('delete', 0, 7)),
    (('move', 0, 7, 1, 7), ('move', 1, 8, 0, 8)),
)


def invariant(rows):
    expected = {}
    values = []
    for (layout, physical, handle), (child, value) in rows.items():
        if layout == 'old':
            assert physical == child // 2, rows
            expected['new', child, handle] = (child, value)
        else:
            assert physical == child, rows
            values.append(value)
    new = {key: value for key, value in rows.items() if key[0] == 'new'}
    assert expected == new, (expected, new)
    # Business labels identify the fixture rows; no production RowToken assumed.
    assert len(values) == len(set(values)), values


def prepare(state, index, operation, guard, mirror=True):
    tx = state['tx'][index]
    snapshot = dict(state['rows'])
    view = dict(snapshot)
    writes = {}
    action = operation[0]
    value = None
    if action != 'insert':
        _, child, handle, *rest = operation
        record = view.get(('new', child, handle))
        if record is None:
            tx.update(status='prepared', writes={}, versions=dict(state['versions']))
            return
        value = record[1]
        # Public/retired writers may still delete old shadows, but never put.
        for key in (('new', child, handle), ('old', child // 2, handle)):
            writes[key] = None
            view.pop(key, None)
    if action == 'insert':
        _, destination, preferred, value = operation
    elif action == 'move':
        destination, preferred = operation[3:]
    if action != 'delete':
        candidate = preferred
        while (('new', destination, candidate) in view
               or (mirror and guard and ('old', destination // 2, candidate) in view)):
            candidate += 1
        writes['new', destination, candidate] = (destination, value)
        if mirror:
            writes['old', destination // 2, candidate] = (destination, value)
    tx.update(status='prepared', writes=writes, versions=dict(state['versions']))


def explore(operations, guard, conflicts):
    rows = {}
    for child, handle, value in INITIAL:
        rows['old', child // 2, handle] = (child, value)
        rows['new', child, handle] = (child, value)
    initial = {'rows': rows, 'versions': {},
               'tx': [dict(status='fresh', retries=0), dict(status='fresh', retries=0)]}
    totals = {'nodes': 0, 'terminals': 0, 'conflict_edges': 0, 'abort_edges': 0}

    def visit(state, trace):
        totals['nodes'] += 1
        try:
            invariant(state['rows'])
        except AssertionError:
            return trace
        if all(tx['status'] == 'done' for tx in state['tx']):
            totals['terminals'] += 1
            return None
        for index, tx in enumerate(state['tx']):
            if tx['status'] == 'done':
                continue
            if tx['status'] == 'fresh':
                child = deepcopy(state)
                prepare(child, index, operations[index], guard)
                failure = visit(child, [*trace, f'T{index} read/prepare'])
                if failure:
                    return failure
                continue
            # User rollback discards all staged mutations, even after preparation.
            child = deepcopy(state)
            child['tx'][index]['status'] = 'done'
            totals['abort_edges'] += 1
            failure = visit(child, [*trace, f'T{index} rollback'])
            if failure:
                return failure
            child = deepcopy(state)
            current = child['tx'][index]
            stale = any(state['versions'].get(key, 0) != tx['versions'].get(key, 0)
                        for key in tx['writes'])
            if stale and conflicts:
                assert tx['retries'] < 2, 'bounded model unexpectedly did not converge'
                current.update(status='fresh', retries=tx['retries'] + 1)
                totals['conflict_edges'] += 1
                label = f'T{index} write conflict/retry'
            else:
                for key, value in tx['writes'].items():
                    if value is None:
                        child['rows'].pop(key, None)
                    else:
                        child['rows'][key] = value
                    child['versions'][key] = child['versions'].get(key, 0) + 1
                current['status'] = 'done'
                label = f'T{index} commit'
            failure = visit(child, [*trace, label])
            if failure:
                return failure
        return None

    return totals, visit(initial, [])


def lifecycle(operations, early_gc=False):
    rows = {}
    for child, handle, value in INITIAL:
        rows['old', child // 2, handle] = (child, value)
        rows['new', child, handle] = (child, value)
    state = {'rows': rows, 'versions': {}, 'irreversible': False, 'gc': False,
             'tx': [dict(status='fresh', retries=0), dict(status='fresh', retries=0)]}
    counts = dict(nodes=0, terminals=0, gc_edges=0, retire_edges=0)

    def visit(current, trace):
        counts['nodes'] += 1
        try:
            if not current['irreversible']:
                invariant(current['rows'])
            else:
                # Preconditions: cutover MDL has already drained all old-read
                # users; new is now authoritative and rollback is forbidden.
                values = [value[1] for key, value in current['rows'].items() if key[0] == 'new']
                assert len(values) == len(set(values))
                if current['gc']:
                    assert not any(key[0] == 'old' for key in current['rows'])
        except AssertionError:
            return trace
        if current['gc'] and all(tx['status'] == 'done' for tx in current['tx']):
            counts['terminals'] += 1
            return None
        if not current['irreversible']:
            child = deepcopy(current)
            child['irreversible'] = True
            counts['retire_edges'] += 1
            failure = visit(child, [*trace, 'publish irreversible/new-only protocol'])
            if failure:
                return failure
        if current['irreversible'] and not current['gc']:
            pending = any(tx['status'] == 'prepared' and tx['mirror'] for tx in current['tx'])
            if early_gc or not pending:
                child = deepcopy(current)
                # Physical range GC does not act as a transaction lock on an
                # absent key a stale writer intends to create after cleanup.
                child['rows'] = {k: v for k, v in child['rows'].items() if k[0] != 'old'}
                child['gc'] = True
                counts['gc_edges'] += 1
                failure = visit(child, [*trace, 'GC old physical ranges'])
                if failure:
                    return failure
        for index, tx in enumerate(current['tx']):
            if tx['status'] == 'done':
                continue
            child = deepcopy(current)
            current_tx = child['tx'][index]
            if tx['status'] == 'fresh':
                mirror = not current['irreversible']
                current_tx['mirror'] = mirror
                prepare(child, index, operations[index], True, mirror)
                label = f'T{index} prepare mirror={mirror}'
            else:
                stale = any(current['versions'].get(k, 0) != tx['versions'].get(k, 0)
                            for k in tx['writes'])
                if stale:
                    assert tx['retries'] < 2
                    current_tx.update(status='fresh', retries=tx['retries'] + 1)
                    label = f'T{index} conflict; restart with current protocol'
                else:
                    for key, value in tx['writes'].items():
                        if value is None:
                            child['rows'].pop(key, None)
                        else:
                            child['rows'][key] = value
                        child['versions'][key] = child['versions'].get(key, 0) + 1
                    current_tx['status'] = 'done'
                    label = f'T{index} commit'
            failure = visit(child, [*trace, label])
            if failure:
                return failure
        return None

    return counts, visit(state, [])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--unguarded', action='store_true')
    parser.add_argument('--ignore-conflicts', action='store_true')
    parser.add_argument('--lifecycle', action='store_true')
    parser.add_argument('--early-gc', action='store_true')
    args = parser.parse_args()
    if args.lifecycle:
        counts = dict(nodes=0, terminals=0, gc_edges=0, retire_edges=0)
        for operations in SCENARIOS:
            totals, failure = lifecycle(operations, args.early_gc)
            for key in counts:
                counts[key] += totals[key]
            if failure:
                print('COUNTEREXAMPLE:', ' -> '.join(failure))
                return 1
        assert counts['terminals'] and counts['gc_edges']
        print(f'PASS: {len(SCENARIOS)} post-cutover retirement scenarios; {counts}')
        return 0
    counts = dict(nodes=0, terminals=0, conflict_edges=0, abort_edges=0)
    for operations in SCENARIOS:
        totals, failure = explore(operations, not args.unguarded, not args.ignore_conflicts)
        for key in counts:
            counts[key] += totals[key]
        if failure:
            print('OPERATIONS:', operations)
            print('COUNTEREXAMPLE:', ' -> '.join(failure))
            return 1
    assert counts['conflict_edges'] and counts['abort_edges']
    print(f'PASS: {len(SCENARIOS)} two-writer scenarios; {counts}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
