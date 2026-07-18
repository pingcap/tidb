# Integration plan inventory

`corpus/coverage/integration_plan_inventory.tsv` is the source-backed queue
for the plan differential ring. It contains every Go-accepted `EXPLAIN`
statement found in the checked mysqltest fixture inventory, not a hand-picked
benchmark corpus. Each row carries the fixture path and line range, its
one-based ordinal where a runner input has multiple statements, the canonical
Go `Restore` bytes, and all expected-result artifacts belonging to the fixture.

The classifier is anchored in TiDB's actual parser: `pkg/parser/set_explain_parser.go`
builds `ast.ExplainStmt`; `pkg/parser/ast/misc.go` restores it. The generator
selects only Go oracle restores beginning with canonical `EXPLAIN`, so
`DESCRIBE` (which becomes `SHOW COLUMNS`) and `PLAN REPLAYER DUMP EXPLAIN`
remain separate obligations rather than being mislabeled as plan outputs.

Run from `rust/`:

```sh
cargo run -j 12 -p difftest --bin integration_parser_inventory -- --check
cargo run -j 12 -p difftest --bin integration_parser_golden -- --check
cargo run -j 12 -p difftest --bin integration_plan_inventory -- --check
```

After an intentional upstream fixture or Go parser-oracle refresh, regenerate
the derived manifest only after the two prerequisite artifacts are current:

```sh
cargo run -j 12 -p difftest --bin integration_plan_inventory -- --write
```

This inventory proves neither plan output parity nor plan-digest parity. It is
the complete input/result-artifact queue the future `tidb-planner` oracle must
replay with TiDB session state, schema, statistics, and deterministic settings.
