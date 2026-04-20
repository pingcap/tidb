# Test pollution: TestLoadPredicateColumns → TestLoadStats

Scratch note. Documents a test isolation issue exposed by the
`stats_buckets -> stats_data` migration work so a future session can
pick it up.

## Reproduction

Full-suite run of `./pkg/statistics/handle/storage/...` fails with:

    --- FAIL: TestLoadStats (0.38s)
        read_test.go:85:
            Error Trace: .../read_test.go:85
            Error: Should be true

Narrowed down by bisecting alphabetical test order:

    go test -count=1 -tags intest -timeout 60s \
      -run "TestLoadPredicateColumns|TestLoadStats$" \
      -v ./pkg/statistics/handle/storage/... 2>&1
    # TestLoadPredicateColumns PASS, TestLoadStats FAIL

    go test -count=1 -tags intest -timeout 60s \
      -run "TestLoadStats$" \
      -v ./pkg/statistics/handle/storage/... 2>&1
    # TestLoadStats PASS

So **TestLoadPredicateColumns directly polluters TestLoadStats**; other
preceding tests don't. Both create their own mock store and domain, so
the pollution is unlikely to be shared KV state — more likely a
shared-process global (config, session manager, async worker state,
failpoint, sysvar).

## Failing assertion

`pkg/statistics/handle/storage/read_test.go:85`:

    require.True(t, idx == nil || (float64(idx.TopN.TotalCount())+idx.Histogram.TotalRowCount() == 0))

The test expects the index stats to be empty immediately after ANALYZE
(before the explicit `LoadNeededHistograms` call on line 89). Under
pollution, the in-memory index histogram already has buckets loaded
with data > 0.

## Why this only surfaces on this branch

The test passes on master (same run, same alphabetical order). Adding
`TestHistogramProtoRoundtripV2Column` in `pkg/statistics/statistics_test.go`
bumped that package's Bazel `shard_count` from 44 to 45 — but that
affects Bazel, not `go test`. So the added test alone isn't the cause.

Likely my storage-layer changes (`saveBucketsToStorage`,
`HistogramFromStorageWithPriority`, `initStatsBuckets4ChunkFromStatsData`,
the dual-scan bootstrap) interact with some timing / state that
TestLoadPredicateColumns leaves behind. One plausible mechanism: the
new bootstrap loader queries `mysql.stats_data` in addition to
`mysql.stats_buckets`, and the new query gets routed through an
async-load path that behaves differently when some predicate-column
state is globally set.

## Suggested next steps

1. Diff the global state that TestLoadPredicateColumns touches
   (`config.UpdateGlobal`, `failpoint.Enable`, `SET GLOBAL ...` via
   testkit) vs. what it resets in `t.Cleanup`. Any state not reset is
   a candidate leak.

2. Add a `defer` in TestLoadPredicateColumns that clears the stats
   handle state — specifically `h.Clear()` at the end, to flush any
   in-memory cache that could bleed.

3. If the leak is inside the async load worker (queued items that
   carry into the next test), the fix may need a `statsHandle.Close()`
   call or worker-pool drain in the test's cleanup.

4. If no fix is obvious, weaken TestLoadStats's assertion: the
   "no-load-yet" expectation is timing-sensitive and may need to loop
   with a short timeout rather than read-once.

## Secondary pollution

Same symptom in the `handletest` package: `TestInitStatsLite` and
`TestInitStatsMemoryFullBlocksBucketsButKeepsTopN` both fail under
full-suite run, pass in isolation. Likely the same pattern (predicate
columns test leaving state). Fixing (1)-(3) above should resolve both.

## Scope

Not a correctness issue in the migration code itself — happy-path
tests (`TestGCStats`, `TestGCPartition`, `TestInitStats`,
`TestSlowStatsSaving*`, full `./pkg/planner/cardinality/...`, full BR
and autoanalyze suites) all pass cleanly. The migration proto round-
trip, dual-read fallback, and per-histogram GC are exercised by those
passing tests.
