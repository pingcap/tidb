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

"""Executable pure-split admission prototype over evaluated integer bounds.

Not a SQL parser, TiDB planner or complete capability check. The production
adapter must use TiDB's typed bound/expression semantics and metadata allocator.
"""
from dataclasses import dataclass
import hashlib
import itertools
import json


class Rejected(ValueError):
    pass


@dataclass(frozen=True, order=True)
class Bound:
    kind: int  # -1 MINVALUE, 0 finite, 1 MAXVALUE
    number: int = 0

    def json(self, unsigned):
        if self.kind:
            return {'kind': 'MINVALUE' if self.kind < 0 else 'MAXVALUE'}
        return {'kind': 'uint64' if unsigned else 'int64', 'decimal': str(self.number)}


MIN = Bound(-1)


def upper(value, unsigned):
    if value == 'MAXVALUE':
        return Bound(1)
    if type(value) is not int:
        raise Rejected('bound must already be evaluated, not a SQL string or NULL')
    low, high = (0, 2**64 - 1) if unsigned else (-2**63, 2**63 - 1)
    if not low <= value <= high:
        raise Rejected('bound is outside the expression integer domain')
    return Bound(0, value)


def definitions(raw, unsigned, lower=MIN):
    result = []
    ids = set()
    previous = lower
    for physical_id, value in raw:
        if type(physical_id) is not int or physical_id <= 0 or physical_id in ids:
            raise Rejected('physical IDs must be distinct positive integers')
        bound = upper(value, unsigned)
        if bound <= previous:
            raise Rejected('bounds must be strictly increasing above inherited lower bound')
        result.append((physical_id, bound))
        ids.add(physical_id)
        previous = bound
    if not result:
        raise Rejected('empty definition list')
    return result


def build_plan(old_raw, selected, new_raw, unsigned=False):
    old = definitions(old_raw, unsigned)
    if not selected or len(set(selected)) != len(selected):
        raise Rejected('empty or repeated selected physical IDs')
    selected_set = set(selected)
    positions = [i for i, (pid, _) in enumerate(old) if pid in selected_set]
    if len(positions) != len(selected) or positions != list(range(min(positions), max(positions) + 1)):
        raise Rejected('selected old partitions must exist and be adjacent')
    first, last = positions[0], positions[-1]
    lower = old[first - 1][1] if first else MIN
    new = definitions(new_raw, unsigned, lower)
    if {pid for pid, _ in old} & {pid for pid, _ in new}:
        raise Rejected('target physical IDs must be fresh')
    if new[-1][1] != old[last][1]:
        raise Rejected('selected outer range changed')
    parent = first
    previous = lower
    groups = []
    for child_id, bound in new:
        if previous == old[parent][1] and parent < last:
            parent += 1
        if bound > old[parent][1]:
            raise Rejected('new interval crosses an old internal boundary')
        groups.append({'child': child_id, 'parent': old[parent][0],
                       'lower': previous.json(unsigned), 'upper': bound.json(unsigned)})
        previous = bound
    body = {'protocol': 1, 'unsigned': unsigned,
            'old': [{'id': pid, 'upper': b.json(unsigned)} for pid, b in old],
            'selected': [old[i][0] for i in positions], 'groups': groups}
    canonical = json.dumps(body, sort_keys=True, separators=(',', ':'))
    return {**body, 'bounds_digest': hashlib.sha256(canonical.encode()).hexdigest()}


def main():
    cases = 0

    def accept(old, selected, new, parents, unsigned=False):
        nonlocal cases
        plan = build_plan(old, selected, new, unsigned)
        assert [g['parent'] for g in plan['groups']] == parents, plan
        assert plan == build_plan(old, list(reversed(selected)), new, unsigned)
        assert json.loads(json.dumps(plan)) == plan
        cases += 1
        return plan

    def reject(old, selected, new, unsigned=False):
        nonlocal cases
        try:
            build_plan(old, selected, new, unsigned)
        except Rejected:
            cases += 1
            return
        raise AssertionError((old, selected, new))

    sample = accept([(1, 100), (2, 200), (3, 300)], [1, 2],
                    [(11, 50), (12, 100), (13, 150), (14, 200)], [1, 1, 2, 2])
    accept([(1, 100), (2, 200)], [2], [(11, 150), (12, 200)], [2, 2])
    accept([(1, 100), (2, 'MAXVALUE')], [1, 2],
           [(11, 50), (12, 100), (13, 150), (14, 'MAXVALUE')], [1, 1, 2, 2])
    accept([(1, -10), (2, 10)], [1, 2],
           [(11, -20), (12, -10), (13, 0), (14, 10)], [1, 1, 2, 2])
    accept([(1, 2**63 + 1), (2, 2**64 - 1)], [1, 2],
           [(11, 2**63), (12, 2**63 + 1), (13, 2**64 - 1)], [1, 1, 2], True)
    accept([(1, 100), (2, 200)], [1, 2], [(11, 100), (12, 200)], [1, 2])
    reject([(1, 100), (2, 200)], [1, 2], [(11, 50), (12, 150), (13, 200)])
    reject([(1, 100), (2, 200)], [1, 2], [(11, 50), (12, 100), (13, 250)])
    reject([(1, 100), (2, 200)], [1, 2], [(11, 50), (12, 100), (13, 180)])
    reject([(1, 100), (2, 200), (3, 300)], [1, 3], [(11, 50), (12, 300)])
    reject([(1, 100), (2, 200)], [1, 1], [(11, 50), (12, 100)])
    reject([(1, 100), (2, 200)], [9], [(11, 50), (12, 100)])
    reject([(1, 100), (2, 200)], [2], [(11, 50), (12, 200)])
    reject([(1, 100), (2, 200)], [1, 2], [(11, 100), (12, 100), (13, 200)])
    reject([(1, 100), (2, 200)], [1, 2], [(1, 100), (12, 200)])
    reject([(1, 100), (2, 200)], [1, 2], [(11, '100'), (12, 200)])
    reject([(1, 2**64)], [1], [(11, 2**64)], True)
    reject([(1, 100)], [1], [(11, -1), (12, 100)], True)

    checked = accepted = 0
    # Independent finite oracle: all old internal cuts must be retained.
    subsets = [tuple(x for x, yes in zip(range(1, 5), mask) if yes)
               for mask in itertools.product((False, True), repeat=4)]
    for old_cuts, new_cuts in itertools.product(subsets, repeat=2):
        old_bounds = (*old_cuts, 'MAXVALUE')
        new_bounds = (*new_cuts, 'MAXVALUE')
        old = list(enumerate(old_bounds, 1))
        new = list(enumerate(new_bounds, 101))
        expected = set(old_cuts) <= set(new_cuts)
        try:
            plan = build_plan(old, [pid for pid, _ in old], new)
        except Rejected:
            assert not expected, (old, new)
        else:
            assert expected, (old, new)
            accepted += 1
            for value in range(6):
                parent = next(pid for pid, b in old if b == 'MAXVALUE' or value < b)
                child = next(pid for pid, b in new if b == 'MAXVALUE' or value < b)
                assert next(g['parent'] for g in plan['groups'] if g['child'] == child) == parent
        checked += 1
    print(f'PASS: {cases} boundary/admission cases; {checked} refinement pairs; {accepted} admitted')
    print('SAMPLE parent map:', [(g['child'], g['parent']) for g in sample['groups']])


if __name__ == '__main__':
    main()
