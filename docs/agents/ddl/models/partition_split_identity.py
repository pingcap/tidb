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

"""Finite checks for a map-free nonclustered split proposal.

Forward check enumerates valid source layouts, including repeated handles in
separate parents. Reverse check assumes atomic transactions and checks each
mutation against a full old/new row-set oracle. It does not implement TiKV,
MVCC, allocation caches, row codecs, indexes, or concurrent read/write schedules.
"""
import argparse
import itertools


def forward_layouts():
    checked = merge_collisions = 0
    # Two old parents, two handles each; each slot is absent, left, or right.
    for values in itertools.product((None, 0, 1), repeat=4):
        source = {(parent, handle): child
                  for (parent, handle), child in zip(
                      itertools.product(range(2), range(2)), values)
                  if child is not None}
        split = {}
        merged = {}
        collision = False
        for (parent, handle), child in source.items():
            key = parent * 2 + child, handle
            assert key not in split
            split[key] = (parent, handle)
            if (0, handle) in merged:
                collision = True
            merged[0, handle] = (parent, handle)
        assert len(split) == len(source)
        checked += 1
        merge_collisions += collision
    return checked, merge_collisions


def mutate(old, new, operation, guarded):
    old, new = dict(old), dict(new)
    action, child, handle = operation
    if action == 'insert':
        # Source handle is implicit. Only the hidden handle is retried; ordinary
        # user column values / LAST_INSERT_ID are outside this model.
        while (child, handle) in new or (guarded and handle in old):
            handle += 1
        new[child, handle] = 'row'
        old[handle] = (child, 'row')
    elif (child, handle) in new:
        value = new.pop((child, handle))
        old.pop(handle, None)
        if action == 'move':
            destination = 1 - child
            # Updating the partition key may preserve this handle in the model.
            # A real DML path that generates another handle must guard that one.
            assert (destination, handle) not in new
            new[destination, handle] = value
            old[handle] = (destination, value)
    return old, new


def reverse_histories(guarded):
    alphabet = tuple(itertools.product(('insert', 'delete', 'move'), range(2), (0,)))
    checked = transitions = 0
    lengths = (4,) if guarded else range(1, 5)
    histories = itertools.chain.from_iterable(
        itertools.product(alphabet, repeat=length) for length in lengths)
    for history in histories:
        old = {0: (0, 'seed')}
        new = {(0, 0): 'seed'}
        for step, operation in enumerate(history):
            try:
                old, new = mutate(old, new, operation, guarded)
            except AssertionError:
                return checked, transitions, history[:step + 1]
            expected = {(child, handle): value
                        for handle, (child, value) in old.items()}
            if expected != new:
                return checked, transitions, history[:step + 1]
            transitions += 1
        checked += 1
    return checked, transitions, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--unguarded', action='store_true')
    args = parser.parse_args()
    layouts, collisions = forward_layouts()
    print(f'FORWARD: {layouts} split layouts injective; {collisions} merged layouts collide')
    histories, transitions, failure = reverse_histories(not args.unguarded)
    if failure:
        print('COUNTEREXAMPLE:', failure)
        return 1
    print(f'PASS: {histories} reverse histories; {transitions} atomic transitions; old/new row sets agree')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
