# Divergence: the AFFINITY table option is silently ignored

## Go oracle

`handleTableOptions` (`pkg/ddl/create_table.go:1003-1010`) validates the
level through `model.NewTableAffinityInfoWithLevel` — a bad level fails
with `ErrInvalidTableAffinity` — and records `tbInfo.Affinity`. Both
CREATE and ALTER reach this loop. `ShowCreateTable`
(`pkg/executor/show.go:1565-1566`) then prints the version-gated
`/*T![affinity] AFFINITY='<level>' */` marker, and `SHOW AFFINITY`
(`show.go:296`) is a dedicated statement.

## Port state

The executor drops the option entirely: `create table t (id int primary
key) affinity = 'bogus'` succeeds (verified 2026-09-06) and SHOW CREATE
prints no AFFINITY marker. `KvTable` carries only a `has_affinity` bool;
the model `TableInfo` mirror has the full field.

## Plan (queued behind the sibling affinity stream)

`cluster_session.rs` (server tier) already syncs
`kv_table.set_has_affinity(table.affinity.is_some())` from the model — the
affinity stream is owned by the sibling implementing the server tier.
Wiring the executor side (record the option into the model TableInfo,
validate the level, print the marker) would collide with that in-flight
work. When the sibling lands:

1. add `table_affinity_option` extraction mirroring
   `table_compression_option` (ddl.rs), last-wins;
2. validate via the model's `NewTableAffinityInfoWithLevel` port,
   surfacing `ErrInvalidTableAffinity`;
3. store on the model TableInfo (not the bool) so the server sync lights up;
4. print `/*T![affinity] AFFINITY='<level>' */` in show.rs after the
   ON COMMIT clause (Go position, show.go:1565).
