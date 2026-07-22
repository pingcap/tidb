# Active package: `pkg/parser/terror`

## Done when

- Every production, test, and support artifact under `pkg/parser/terror` has a
  Rust equivalent, and the Go, Rust, differential, lint, and live checks pass.

## Now

- `pkg/parser/ast`, `pkg/parser/format`, `pkg/parser/opcode`, and
  `pkg/parser/util`, `pkg/parser/auth`, and `pkg/parser/charset` are closed as
  complete packages. `pkg/parser/duration` is also closed after correcting its
  Unicode digit classification to match Go exactly.
- Transcreate `pkg/parser/terror` as one complete package.

The package is open.
