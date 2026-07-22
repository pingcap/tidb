# Active package: `pkg/parser/mysql`

## Done when

- Every production, test, and support artifact under `pkg/parser/mysql` has a
  Rust equivalent, and the Go, Rust, differential, lint, and live checks pass.

## Now

- `pkg/parser/ast`, `pkg/parser/format`, `pkg/parser/opcode`, and
  `pkg/parser/util`, `pkg/parser/auth`, and `pkg/parser/charset` are closed as
  complete packages. `pkg/parser/duration` is also closed after correcting its
  Unicode digit classification to match Go exactly. `pkg/parser/terror` is
  closed as a complete package.
- Transcreate `pkg/parser/mysql` as one complete package.

The package is open.
