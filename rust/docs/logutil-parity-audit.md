# pkg/util/logutil + pingcap/log parity audit (baseline a85e0fd5df)

Full-file audit of Go `pkg/util/logutil` (general_logger.go, hex.go, log.go,
slow_query_logger.go) and its pinned `github.com/pingcap/log` dependency
against `rust/crates/tidb-log` and `rust/crates/tidb-util/src/logutil`.

## Fixed this batch (behavior)

1. `tidb-util/logutil/file_sink.rs`: the unset max-size fallback is Go's
   `DefaultLogMaxSize = 300` (MB), not lumberjack's own 100.
2. `tidb-log/log.rs` FileSink rotation: backups are now named
   `<base>-<local timestamp><ext>` in lumberjack's
   `2006-01-02T15-04-05.000` format (was `<file>.<sequence>`), and
   retention counts only `<base>-<parsed timestamp><ext>[.gz]` files
   ordered by the PARSED timestamp — sibling files sharing the prefix are
   no longer deletion candidates. Reachable through lightning's file
   logging. Regression: `test_rotate_backup_name_uses_timestamp_format`.
3. `tidb-log` `Level::parse` accepts the empty string as info and
   lowercase input, matching zapcore's `UnmarshalText`.

## Verified matching (highlights)

- text_encoder: timestamp `2006/01/02 15:04:05.000 -07:00`, capital
  levels, `[section]` separators, quoting/escaping, errorVerbose toggle,
  JSON key order — golden-tested byte-identical.
- zap_text_core: enabled gate, sync-on->=Error, With-clone.
- config structs: all pingcap/log Config and FileLogConfig fields with
  identical toml/json names incl. error-output-path and timeout.
- global logger init/ReplaceGlobals-with-restore, SetLevel/DynamicLevel
  (case-insensitive parse, ""=info), ReplaceLogger flow,
  general/slow-query shared-vs-dedicated sink wiring and errVerbose
  re-init, sampler (zap NewSamplerWithOptions 4096 FNV buckets).
- hex.go's proto pretty-print EXISTS at tidb-util/logutil/hex.rs with
  matching goldens; Go's `Hex()` has zero production callers at the
  baseline and so does the Rust port — the reflection surface is truly
  unreachable.
- Deliberately unported (declared, verified unreachable): gRPC logger
  init + GRPC_DEBUG, tikv log-context key, ctx plumbing
  (WithLogger/WithKeyValue/trace), opentracing Event/Eventf/SetTag.

## Accepted narrowings / open items

- The Logger drops Go's `zap.AddStacktrace(FatalLevel)` stack field (no
  fatal-level methods yet), ignores buffering/caller-skip config values,
  and always reports "write log" on sync failures.
- buildOptions' disable-caller/error-output-path/sampling/development are
  not yet honored by the tidb-util Logger (only init_logger/replace_logger
  callers, tests today) — recorded as an open item.
- `OldSlowLogTimeFormat` is unported pending the slow-log parser surface.

## Validation

- `cargo test -p tidb-log` (lib 7, integration 11 incl. the new backup-name
  regression), `cargo fmt`, `git diff --check`, `make lint`.
