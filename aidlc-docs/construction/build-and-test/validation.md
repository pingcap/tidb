# Build and test record

The WIP focused profile passed during construction:

```text
cargo test --locked -p tidb-expr --lib builtin_cast_semantics::tests::union_unsigned_integer_cast_clamps_negative_values
cargo test --locked -p tidb-planner --lib plan_builder::set_opr_tests::union_unsigned_widening_uses_the_in_union_cast_signature
```

The full Ready profile is recorded in the parity receipt after the final
rebase against `origin/hparser-integration`.

Observed before the final rebase: focused expression/planner tests, the
planner set-operation owner suite (35/35), all-target compilation, formatting,
and `git diff --check` passed. The broad owner test retained one unrelated
loopback-PD label-delivery failure; the expression nextest run retained one
unrelated loopback HTTP JSON-schema fixture failure. Strict clippy was blocked
by pre-existing diagnostics in unrelated `tidb-mysql`, generated protobuf, and
other workspace code.
