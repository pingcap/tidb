# Code generation plan: UNION `inUnion`

1. Add `CastType::UnsignedInUnion` as an internal-only AST carrier that
   restores as `UNSIGNED` and is never produced by the SQL parser.
2. Thread an `in_union` boolean through the Rust cast builder. Keep all
   existing wrappers on `false`; expose `build_cast_to_in_union` for UNION.
3. Map `cast_unsigned_in_union` through result-type and scalar dispatch to the
   evaluator. Clamp negative numeric/string integer inputs to zero while
   preserving temporal-source behavior.
4. Route `build_projection4_union` through the new helper.
5. Test ordinary-vs-UNION function names and values, plus planner projection
   shape. Keep the Go string-as-decimal test documented as a separate gap
   because that target is not an unsigned-integer cast.
