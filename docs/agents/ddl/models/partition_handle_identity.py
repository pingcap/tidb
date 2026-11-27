"""Bounded identity model for the proposed nonclustered protocol.

Atomic claim transactions and the true allocator contract are assumed. Checks
claim order, original-handle collisions, move-before-scan, deleted reservations,
post-cut insert/move and rollback reconstruction. Does not model TiKV, indexes,
row bytes/checksum, arbitrary slot reuse, or transaction-level claim conflicts.
"""
from copy import deepcopy
from itertools import permutations

INITIAL = {0: ('O1', 7, 'N1'), 1: ('O2', 7, 'N1')}
EVENTS = ('scan0', 'scan1', 'move0', 'delete1', 'cut', 'insert2', 'move_after_cut', 'rollback')


def target(state, token, pid, preferred):
    key = token, pid
    if key in state['forward']:
        return state['forward'][key]
    candidate = preferred
    while (pid, candidate) in state['reverse']:
        candidate = state['allocator']
        state['allocator'] += 1
    state['forward'][key] = candidate
    state['reverse'][pid, candidate] = token
    return candidate


def claim_source(state, slot, token):
    if slot in state['owners']:
        assert state['owners'][slot] == token
    else:
        state['owners'][slot] = token


def scan(state, token):
    old_pid, old_handle, pid = INITIAL[token]
    claim_source(state, (old_pid, old_handle), token)
    handle = target(state, token, pid, old_handle)
    # A scanner must not replace newer DML live links with its old snapshot.
    state['live'].setdefault(token, (old_pid, old_handle, pid, handle, True))


def check(state):
    for (token, pid), handle in state['forward'].items():
        assert state['reverse'][pid, handle] == token
    assert len(state['forward']) == len(state['reverse'])
    for token, (old_pid, old_handle, pid, handle, present) in state['live'].items():
        assert state['owners'][old_pid, old_handle] == token
        assert state['reverse'][pid, handle] == token
        if present:
            assert state['oracle'][token] == (old_pid, old_handle, pid)
        else:
            assert token not in state['oracle']
    if state['cut']:
        assert set(state['oracle']) <= set(state['live'])
        # Includes the old physical identity needed for rollback, not just count.
        old = {token: (value[0], value[1], value[2])
               for token, value in state['live'].items() if value[4]}
        assert old == state['oracle']


def event(state, name):
    if name.startswith('scan'):
        scan(state, int(name[-1]))
    elif name == 'move0':
        scan(state, 0)
        slot = ('O3', 8)
        claim_source(state, slot, 0)
        handle = target(state, 0, 'N2', 8)
        state['live'][0] = (*slot, 'N2', handle, True)
        state['oracle'][0] = (*slot, 'N2')
    elif name == 'delete1':
        scan(state, 1)
        value = state['live'][1]
        state['live'][1] = (*value[:4], False)
        del state['oracle'][1]
    elif name == 'cut':
        state['cut'] = True
    elif name == 'insert2':
        # Try to use a source slot referenced by the deleted row: skip it.
        source_slot = ('O2', 7)
        if source_slot in state['owners']:
            source_slot = ('O2', 20)
        claim_source(state, source_slot, 2)
        handle = target(state, 2, 'N1', 7)
        state['live'][2] = (*source_slot, 'N1', handle, True)
        state['oracle'][2] = (*source_slot, 'N1')
    elif name == 'move_after_cut':
        # R resolves token; Live supplies current old identity, not birth slot.
        value = state['live'][0]
        token = state['reverse'][value[2], value[3]]
        assert token == 0
        slot = ('O4', 9)
        claim_source(state, slot, token)
        handle = target(state, token, 'N1', 9)
        state['live'][token] = (*slot, 'N1', handle, True)
        state['oracle'][token] = (*slot, 'N1')
    elif name == 'rollback':
        assert state['cut']
    else:
        raise AssertionError(name)
    check(state)


def main():
    schedules = transitions = 0
    for order in permutations(EVENTS):
        pos = {name: order.index(name) for name in order}
        if not (pos['scan0'] < pos['cut'] and pos['scan1'] < pos['cut']
                and pos['move0'] < pos['move_after_cut']
                and pos['cut'] < pos['insert2']
                and pos['delete1'] < pos['insert2']
                and pos['cut'] < pos['move_after_cut']
                and pos['insert2'] < pos['rollback']
                and pos['move_after_cut'] < pos['rollback']):
            continue
        state = dict(forward={}, reverse={}, owners={}, live={},
                     oracle=deepcopy(INITIAL), allocator=8, cut=False)
        for name in order:
            event(state, name)
            transitions += 1
        schedules += 1
    assert schedules
    print(f'PASS: {schedules} schedules; {transitions} transitions; identity and rollback invariants')


if __name__ == '__main__':
    main()
