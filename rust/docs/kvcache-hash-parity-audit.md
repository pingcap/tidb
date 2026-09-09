# kvcache + hash parity audit: Go `pkg/util/kvcache`, `pkg/parser/util/hash64.go`

Audit date: 2026-09-05. Method: function-level mapping plus a
line-level comparison of the eviction loop.

## kvcache — VERIFIED

`SimpleLruCache` mirrors Go `SimpleLRUCache`'s full API (capacity +
memory-guard construction, on-evict callback, get/peek/delete/
delete_all/size/values, set_capacity). `put` reproduces Go's exact
branching: existing keys update in place and move to front; the
quota-0 path evicts a single oldest entry only on capacity overflow;
the quota>0 path loops while above `quota * (1 - guard)` or over
capacity, re-sampling process memory ONLY when the memory condition
held on that iteration, and a failed probe clears the whole cache —
Go's `DeleteAll` on `InstanceMemUsed` error.

## hash — VERIFIED

`tidb-hash`'s `IHasher` is the dependency-inversion contract of
`pkg/parser/util/hash64.go` (bool/int/int64/uint64/float64/rune/string/
byte) with the same method set.
