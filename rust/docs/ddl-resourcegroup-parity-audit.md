# pkg/ddl/resourcegroup parity audit (baseline a85e0fd5df)

Full audit of Go `pkg/ddl/resourcegroup` (errors.go, group.go) against
`rust/crates/tidb-ddl-resourcegroup`. Baseline scope confirmed: the
package contains only these two files (checker-map/TTL surfaces live in
domain BundleManager, outside this package).

## Result: no behavior-breaking divergences

- `MaxGroupNameLength = 32`; byte-length name check on both sides.
- All 9 error texts byte-identical (pinned by the new
  `error_display_texts_match_go_literals` drift-guard test).
- `NewGroupFromOptions` flow identical: nil-options guard, runaway
  all-zero rule check, NONE action, SWITCH_GROUP empty-name, watch
  built only for non-WatchNone, background settings, RU-mode token
  bucket with the duplicated-mode (CPU/IO limiter) check, Unknown-
  ResourceGroupMode fallthrough; priority cast wraps like Go's uint32
  conversion.

## Documented narrowings

- Proto3 repeated-field `JobTypes`: Rust normalizes None to an empty
  vec — nil and empty marshal identically (commented at the site).
- Go's 9 package-level error vars become a Copy enum + Display/Error
  (no wrapping existed in Go to preserve); model reads snapshot
  RwLock-guarded fields where Go reads plain struct fields.

## Validation

- `cargo test -p tidb-ddl-resourcegroup` (incl. the new drift guard),
  `cargo fmt`, `git diff --check`, `make lint`.
