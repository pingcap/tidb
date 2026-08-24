# Per-statement cost parity with the Go node

Status: living document. Each unit here is a MEASURED gap against a Go
tidb-server on the SAME TiUP cluster, closed by porting Go's structure
rather than by local tuning. Add a unit only with a before/after receipt
from `scripts/run-sysbench-ladder.sh`.

## How a unit is found

One number cannot be acted on. The ladder's `us/stmt` column is a
client-side figure that contains everything: wire, dispatch, planning,
the TiKV round trip. Partitioning it is the whole method:

1. A temporary probe around the server's statement entry point
   (`execute_general`) gives the SERVER-side p50.
2. A second probe around the single TiKV call (`begin_get`) gives the
   RPC-only p50. The difference is our own compute; the client-observed
   figure minus the server p50 is wire and protocol.
3. The Go node publishes the same RPC figure for free:
   `tidb_tikvclient_request_seconds_{sum,count}{type="Get"}` on its
   status port. Poll it during the Go leg and divide the deltas -- that
   is client-go's cost for the identical request against the identical
   store, and therefore the floor our transport is aiming at.
4. `sample <pid>` on the serving thread for 10s, aggregated with
   `rustfilt`, names the frames. Read it as a TREE: a leaf's cost only
   matters relative to its siblings under the same parent.

Measured 2026-08-23, prepared point select: client 211 us, server-side
178 us, RPC alone 105 us. So the wire cost ~35 us, our compute ~75 us,
and the transport was 30 us above client-go's 75 us for the same call.

## Closed units

### The connection worker's stack (2026-08-23)

The planner's recursion is guarded by `stacker::maybe_grow(red_zone =
2 MB, segment = 16 MB)`. The connection workers ran on the DEFAULT 2 MB
thread stack -- exactly the red zone -- so the check failed on every
select and each statement mmap'ed and munmap'ed a fresh 16 MB segment.
In the profile, `stacker::_grow` -> `__munmap`/`__mprotect` was ~11% of
the serving thread, and the growth frames sat directly above the
planner in every sample.

Go never pays it because a goroutine's grown stack PERSISTS for the
goroutine's life. The port of that semantics for a dedicated thread is
a large reserved stack (`SQL_WORKER_STACK_BYTES`, 32 MB of address
space committed as touched), which leaves `maybe_grow` as the safety
net it was written to be rather than a per-statement toll.

Receipt: point_select ps-auto 211 -> 181 us/stmt, ps-disable 228 -> 202.

Note the coupling with thread reaping: a joinable thread's stack stays
mapped until `join`, so a large reserved stack is only safe on a node
that reaps finished connection threads instead of deferring every join
to shutdown.

### One preprocess walk per statement (2026-08-23)

The dispatch funnel asked three table-shaped questions per statement --
MDL related-table recording, the `AS OF TIMESTAMP` interception, and
the `LastTxnInfo` "did this read a stored table" check -- and each one
walked the AST separately. Because the visitor API takes `&mut`, each
walk first deep-CLONED the whole statement.

Go asks them once: `Preprocess` (`pkg/planner/core/preprocess.go`)
visits the AST a single time and every per-statement fact falls out of
that pass. `binding::scan_statement_tables` is that shape -- one
in-place walk returning the lowercased name list and the as-of flag,
shared by all three consumers, no clone. The as-of interception is now
gated on the flag, so a statement without the clause never enters it.

Receipt (3-sample medians, with the stack unit): point_select ps-auto
178 us/stmt, read_only ps-auto 250, write_only ps-auto 363, read_write
ps-auto 372; every ladder rung green including rung 8's hostile-DDL
leg.

## Open units

- **The transport's synchronous receipt round-trip.** Every TiKV call
  sends a `WorkerCommand::BatchSubmit` to the transport worker and then
  BLOCKS on a reply channel for the publication receipt, before the
  separate completion wait for the response itself. The profile shows
  that first hop as `batch_submit_inner` -> `recv` ->
  `semaphore_wait_trap`, ~12% of the call. client-go's `batchCommandsClient`
  hands the request to the stream and waits ONCE, on the response.
  Closing this means letting the caller bind its publication from the
  completion side rather than from a synchronous receipt -- the receipt
  exists for lifecycle diagnostics (`batch_stream_generation`,
  `batch_request_id_watermark`), which the tests read, so the unit is a
  restructuring of those observers, not a deletion.
  Target: the ~30 us between our 105 us Get and client-go's 75 us.

- **The text protocol's parse.** ps-disable stays ~25 us above ps-auto
  in every workload; Go's gap between the two is smaller. Parsing is
  the difference, and Go caches nothing there either -- so this is a
  parser-cost comparison, not a caching one.
