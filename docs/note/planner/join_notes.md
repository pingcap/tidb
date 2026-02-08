# Join Notes

## 2026-02-04: Join Reorder and FullSchema

### Key Points

- Join reorder can build new join nodes. If `FullSchema` and `FullNames` are not set, later column
  resolution may search the wrong output schema and trigger "Can't find column".
- Treat join output schema as the operator output. Keep `FullSchema` consistent with child outputs
  when a join is constructed or reordered.
