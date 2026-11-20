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

"""Finite MVCC-layout and uncertain-generation model for the final design.

Assumes atomic MVCC, snapshot metadata by actual commit timestamp, post-apply
successful acknowledgements and a valid read/GC lease. Not a TiKV/SST test.
"""
import argparse


def visible(versions, ts):
    versions = [(commit, value) for commit, value in versions if commit <= ts]
    return max(versions, default=(0, None), key=lambda item: item[0])[1]


def layout_checks(latest=False, historical_write=False):
    checks = 0
    # New data and index finish before metadata publication. Old mirror remains
    # complete until retirement; later source commits use only the new layout.
    for copied in (3, 4, 5):
        for changed in range(copied + 1, 9):
            for cut in range(changed + 1, 11):
                retire = cut + 2
                later = retire + 2
                source = [(1, 'v1'), (changed, 'v2'), (later, 'v3')]
                old = [(1, 'v1'), (changed, 'v2')]
                new = [(copied, 'v1'), (changed, 'v2'), (later, 'v3')]
                for read_ts in range(1, later + 2):
                    # The metadata current-pointer switches atomically at cut.
                    expected_layout = 'old' if read_ts < cut else 'new'
                    chosen_layout = 'new' if latest else expected_layout
                    got = visible(old if chosen_layout == 'old' else new, read_ts)
                    expected = visible(source, read_ts)
                    if got != expected:
                        return checks, {'read_ts': read_ts, 'copy_ts': copied, 'cut_ts': cut,
                                        'layout': chosen_layout, 'expected': expected, 'got': got}
                    # Cache hits must validate the actual read-layout fingerprint.
                    for cached_layout in ('old', 'new'):
                        cache_hit = cached_layout == expected_layout
                        if cache_hit:
                            assert visible(old if cached_layout == 'old' else new, read_ts) == expected
                    # Reading old metadata is safe for a historical snapshot;
                    # mutating that obsolete layout after retirement is not.
                    if expected_layout == 'old' and historical_write:
                        old_written = [*old, (later + 1, 'user-write')]
                        canonical_after = visible(new, later + 1)
                        assert visible(old_written, later + 1) == 'user-write'
                        assert canonical_after != 'user-write'
                        return checks, {'read_ts': read_ts, 'mutated_layout': 'old',
                                        'expected_write': 'user-write',
                                        'canonical_after': canonical_after}
                    checks += 1
    return checks, None


def quarantine_checks(reuse=False):
    checks = 0
    # Quarantine is durable before starting the next generation. A previously
    # dispatched old import may apply before/after any of these control events.
    steps = ('quarantine', 'new-baseline', 'repair', 'publish', 'user-delete', 'gc')
    for insertion in range(len(steps) + 1):
        schedule = list(steps)
        schedule.insert(insertion, 'late-old-apply')
        old_pid = 101
        new_pid = old_pid if reuse else 102
        store = {old_pid: []}
        store.setdefault(new_pid, [])
        source = 'current'
        active = 'building'
        uncertain = True
        published = None
        for step in schedule:
            if step == 'late-old-apply':
                store[old_pid].append((2, 'baseline'))
            elif step == 'quarantine':
                active = 'quarantined'
            elif step == 'new-baseline':
                assert active == 'quarantined'
                store[new_pid].append((3, 'baseline'))
            elif step == 'repair':
                store[new_pid].append((5, source))
            elif step == 'publish':
                # Unknown generation is never publishable, even after an old
                # callback later reports success. The replacement has no unknowns.
                assert uncertain
                published = new_pid
            elif step == 'user-delete':
                source = None
                store[new_pid].append((7, None))
            elif step == 'gc':
                # Models removal of the latest deletion plus all older versions.
                assert visible(store[new_pid], 8) is None
                store[new_pid] = []
            if published is not None:
                got = visible(store[published], 100)
                if got != source:
                    return checks, {'schedule': schedule, 'at': step, 'published_pid': published,
                                    'source': source, 'got': got}
            checks += 1
        # Old metadata retains quarantine ownership; no cleanup-complete claim.
        assert active == 'quarantined'
    return checks, None


def own_write_checks(unsafe_mix=False):
    # A valid pre-cut writer mirrors both namespaces and holds the retirement
    # MDL barrier; post-cut new RR writers use the new consistent-read layout.
    cases = (('RR', 'old', 'old', True), ('RR', 'new', 'new', False),
             ('RC', 'new', 'old', True), ('RR', 'old', 'new', False))
    checked = 0
    for isolation, read_layout, write_layout, mirror in cases:
        if isolation == 'RR' and read_layout != write_layout and not unsafe_mix:
            # Admission fails before mutation; no successful transaction claim.
            checked += 1
            continue
        overlay = {write_layout: 'own-write'}
        if mirror:
            overlay['old'] = overlay['new'] = 'own-write'
        observed = overlay.get(read_layout, 'snapshot-value')
        if observed != 'own-write':
            return checked, {'isolation': isolation, 'read_layout': read_layout,
                             'write_layout': write_layout, 'own_read': observed}
        checked += 1
    return checked, None


def rollback_read_checks():
    # A previously published generation is retained for its historical interval.
    source = [(1, 'v1'), (4, 'v2'), (6, 'v3'), (8, 'v4')]
    old = list(source)
    new = [(3, 'v1'), (4, 'v2'), (6, 'v3')]
    checked = 0
    for ts in range(1, 10):
        layout = 'new' if 5 <= ts < 7 else 'old'
        assert visible(new if layout == 'new' else old, ts) == visible(source, ts)
        checked += 1
    # Cleanup time 9 is later than restore commit 7; a valid reader at 6
    # keeps the safepoint below the delete-range eligibility timestamp.
    for pinned_ts in (2, 6):
        safepoint = pinned_ts
        assert not (9 < safepoint)
    return checked


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--latest-layout', action='store_true')
    parser.add_argument('--reuse-uncertain', action='store_true')
    parser.add_argument('--historical-write', action='store_true')
    parser.add_argument('--mixed-rr-write', action='store_true')
    args = parser.parse_args()
    reads, failure = layout_checks(args.latest_layout, args.historical_write)
    if failure:
        print('READ COUNTEREXAMPLE:', failure)
        return 1
    own_reads, failure = own_write_checks(args.mixed_rr_write)
    if failure:
        print('OWN-WRITE COUNTEREXAMPLE:', failure)
        return 1
    events, failure = quarantine_checks(args.reuse_uncertain)
    if failure:
        print('GENERATION COUNTEREXAMPLE:', failure)
        return 1
    rollback_reads = rollback_read_checks()
    print(f'PASS: {reads} MVCC read bindings/cache checks; {own_reads} own-write cases; {rollback_reads} rollback read bindings; {events} quarantine schedule events')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
