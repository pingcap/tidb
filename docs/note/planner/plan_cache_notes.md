## Plan Cache Notes

### Schema Consistency
- Plan cache matching depends on stable column references across logical and physical plans.
- When a transformation replaces columns or schemas, rebind derived expressions to the new schema before caching or matching.

### Join Condition Columns
- Join reordering and projection pruning must keep columns referenced by join conditions in the operator schema (or FullSchema), or resolve-index will fail during plan cache usage.
