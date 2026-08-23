# Reading Go, not recalling it

TiDB's Rust transcreation has one acceptance test that outranks every
other: **the same behaviour as Go**. That makes the Go sources evidence,
and evidence has to be read.

## The problem this solves

Two of the three sources this port is transcreated from are already in
this checkout:

| Source | Where | Status |
| --- | --- | --- |
| TiDB itself | `pkg/**` | in this repo |
| TiKV's protobuf contracts | `rust/crates/tidb-proto` | generated here |
| **client-go** | not vendored anywhere | **must be fetched** |

TiDB delegates every transaction, region, lock and retry decision to
`github.com/tikv/client-go/v2`, and that module is not vendored in this
tree. There is not necessarily a Go toolchain on the workstation either,
so `go mod download` may not be available. The practical result is that
it is easy to write "client-go does X" from memory.

That has been wrong in practice. On 2026-08-23 seven such claims were
re-checked against the real source: five held exactly, and two were
wrong in the same direction -- the port was STRICTER than Go, refusing
shapes Go accepts:

* `getTxnStatus` stores the returned primary lock with a bare
  `status.primaryLock = cmdResp.LockInfo` and only ever asks it
  `UseAsyncCommit`. The port ran it through an admission gate, so a read
  that met a live pessimistic transaction failed outright.
* `SetSnapshotTS` clears `resolvedLocks` and deliberately leaves
  `committedLocks` standing. The port cleared both.

Neither was caught by a unit test or by a live cluster run. Both were
caught in minutes by reading the file.

## Fetching it

```bash
bash rust/scripts/fetch-go-oracle.sh
```

It reads the pin out of `go.mod` -- so it cannot drift from what TiDB
actually builds against -- and checks that commit out under
`rust/.oracle/client-go`, which is gitignored. The oracle is a
reference, never a dependency, and must not be committed into TiDB.

## Using it

The rule is the one that found those two bugs: **compare the
implementation, not the signature.** A function existing with a
matching name says nothing; what it does with each branch is the
contract.

* Read the whole function, and the callers that give its return value
  meaning. `status.primaryLock` looks like it deserves validation until
  you see that the only question ever asked of it is
  `UseAsyncCommit`.
* Cite file and line in the code comment, so the next reader can check
  the claim instead of trusting it: `lock_resolver.go:1080`, not
  "client-go stores it raw".
* When Go and this port differ deliberately, say so and say why. A
  conservative divergence is fine; an unexamined one is not.
* Prefer the local `pkg/**` when the behaviour is TiDB's own --
  `KeyNeedToLock`, `handlePessimisticLockError` and the MDL loops all
  live here, and need no fetch.
