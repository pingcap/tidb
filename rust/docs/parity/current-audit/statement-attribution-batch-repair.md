# Statement attribution batch: O11 and O18

Three source-confirmed gaps are repaired together: ordinary physical compile failures no longer increment TopSQL execution counters; completed statements consume stable deduplicated database/table visits; actual text parsing and physical planning durations reach statement summaries. Existing compile metrics consume the same measured plan-ready duration. These are maintenance repairs to existing producers, not complete package transcreation.

Parent `77759b60eea08dd3b287768c188fcf119fadf3fc`, editable hparser-integration; freshly fetched Go master `93a01d31f6da205ae4bf376825293903a6899fdb`; native unchanged `19a56ccda1e128218cd33c69709038219aced9bc`. All execution is in Codex Cloud. No push.

## Shared ownership and behavior

Go executor/adapter.go starts statistics after successful construction and SummaryStmt consumes StatementContext.Tables plus DurationParse/DurationCompile. planner/core/planbuilder.go::GetDBTableInfo deduplicates existing privilege visits; the Rust consumer reuses its maintained visit producer rather than adding an AST scanner. The existing producer's broader A01 gaps remain.

A statement-owned shared phase state distinguishes physical plan-ready from executor-ready. Both Session context constructors attach it; clones, inner readers, rebuilt attempts and cluster pre-lock notifications share once-only execution admission independently of the breakpoint latch. Physical compile duration stops before executor construction, avoiding the fused DML wrapper's execution time. If execution begins without a plan-ready notification, compile timing stays unverified/zero rather than attributing already-executed work to compilation. Administrative fused compile metrics retain their prior fallback; complete phase parity remains open.

Text parsing is measured at the parser and transferred across frontend parse/execute boundaries. Prepared execution retains the outer command's actual parsing where it occurs and never invents a parse of the retained body. Go total cost includes parsing before ExecuteStmt; it is added exactly once. Table visits retain stable first occurrence, exclude dynamic empty pairs, and reset with each statement. Cached prepared and routed completion share their original identity; streaming closes still publish once. No useful test or safety fallback is removed.

## Evidence and remaining scope

Three real-session regressions fail before and pass after. Seven new cases also cover cached prepared attribution, frontend parse transfer, stale parse prevention, pre-consumed breakpoints and routed pre-lock failure. Baseline follow-on lock poisoning is distinguished from independent failures; assertions now release shared summary locks before checking values.

**51 distinct Rust cases pass**, with zero ignored in focused runs; all-target checking, make lint and locked server build pass. The first link failed at 100% disk with a bus error. Recorded old/failed ELF outputs and regenerable Go compilation cache were removed; the build then passed. Source, current test executables and recovery evidence were preserved.

**12 real MySQL/unistore checks pass** across text, named prepared and binary prepared requests, cache reuse, measured phases and table reset. The server exits cleanly. Go reader.go converts absent TABLE_NAMES to SQL NULL; the initial smoke expectation of an empty string was corrected. An initial --load-privileges startup was not supported by this unistore authentication door; the final smoke uses a mode-0600, passwordless synthetic test account on loopback, with no copied credentials. See [validation](statement-attribution-batch-validation.json) for exact commands, hashes and excluded attempts.

O11 and O18 remain **partial**. Complete SQL/plan registration, digest/profiling/reporting transport, runtime enable/fast-plan policy, complete visits/metadata and administrative/EXPLAIN/external compile phases remain unresolved. Full Go package/platform/generated/fixture acceptance, live multi-node TiKV and performance were not verified. Other 55 unresolved findings retain prior evidence, not fresh runtime reproduction. Counts remain **86 tracked, 29 repaired, 57 unresolved (40 open, 17 partial)**.

The [living ExecPlan](../../statement-attribution-batch-execplan.md) records steps and recovery. Local commits must pass the actual locked server-build hook. Cloud statement-attribution/final-handoff.json records final hook, bundle and draft readback; fresh-task restoration remains unverified. Nothing is pushed.
