// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
Package checkpoint computes a downstream-safe checkpoint for CRR.

The calculator advances in rounds. In each round it reads the current upstream
global checkpoint `c`, scans new meta files with `flushTS > syncedTS`, waits
until every file referenced by those meta files exists downstream, then returns
`c`.

`syncedTS` is the replication-complete checkpoint. It advances only after the
calculator has verified that all files discovered in the round are already
present downstream. `lastCheckpoint` is the last upstream global checkpoint
returned by the calculator.

The calculator tracks synced progress per store and publishes the global
`syncedTS` as the minimum synced `flushTS` across all alive stores. This is
necessary because meta file names are globally ordered by
`(flushTS, storeID, extraTags)`, while `flushTS` is only monotonic within each
individual store.

For example:

  - `0672E0E5956C00020000000000000004-<...>.meta`
  - `0672E0E5A00000000000000000000002-<...>.meta`

Meta file names are ordered as `{flushTS:016X}{storeID:016X}-<...>.meta`.

As an example:

0672E0E5956C00020000000000000004-<...>.meta
|flushTS ------||storeID ------|  ->  flushTS = 0x0672E0E5956C0002, storeID = 4

If one alive store is only known synced through `0x0672E0E5956C0002`, while
another is synced through `0x0672E0E5A0000000`, then the global `syncedTS`
must stay at `min(0x0672E0E5956C0002, 0x0672E0E5A0000000)`.

This algorithm relies on these invariants:

  - meta file names are ordered by `(flushTS, storeID, extraTags)`
  - `flushTS` is the leading ordering key in the meta file name
  - for each individual store, its own meta files have monotonically
    increasing `flushTS`

Downstream access is intentionally limited to existence checks of known object
paths. The calculator must not read downstream object contents.
*/
package checkpoint
