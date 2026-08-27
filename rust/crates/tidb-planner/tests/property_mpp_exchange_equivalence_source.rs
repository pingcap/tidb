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

//! Port ledger for `pkg/planner/property/physical_property_test.go`
//! (`pkg/planner.part22` item 1267 on `origin/master`):
//! `TestNeedEnforceExchangerWithHashByEquivalence`.
//!
//! Re-derived contract: `PhysicalProperty.NeedMPPExchangeByEquivalence`
//! (pkg/planner/property/physical_property.go:484-503) takes the CHILD-supplied
//! partition columns and returns whether an exchange enforcer is needed. For
//! each REQUIRED column `p.MPPPartitionCols` it computes the equivalence-class
//! closure over the FD set (`FDSet.ClosureOfEquivalence`); every supplied key
//! must land inside some required column's closure with matching collation id
//! (`checkEquivalence`, physical_property.go:505-511). The first supplied key
//! outside every closure forces an exchange (true); only full coverage lets it
//! stay false. The subset shortcut: if the child supplies MORE columns than the
//! parent requires but all required ones are covered by what the child supplies,
//! the exchanger can still be eliminated — comment block :496-501.
//!
//! Six Go rows (:31-160), fixtures `buildTPCHQ3FD` (:162-176): FDs
//! (1)-->(2-6,8), ()-->(7), (9)-->(10-17), {10,21}-->(19-33) with equivalence
//! unions (9,18) and (1,10); `buildFD` (:178-187): 2~4, 3~4; `buildFD2`
//! (:188-198): 2~4, 2~5:
//! 1. required [18,13,16], supplied [9]: false — 9~18 covers 18; 13/16 cover
//!    themselves.
//! 2. same required, supplied [9,13]: false.
//! 3. same required, supplied [9,17]: true — 17 is in no closure.
//! 4. same required, supplied [1,17]: true.
//! 5. buildFD, required [1,2,3], supplied [1,2,4,5]: true — 5 lies outside
//!    every closure even though 4 covers 2 and 3.
//! 6. buildFD2, required [1,2], supplied [1,2,5]: false — 5 joins closure(2).
//!
//! go-parity-gap: the crate's `PhysicalProperty`
//! (src/physical_property.rs:195) deliberately omits the MPP field family
//! ("MPP partitioning ... belong to planner layers that are not built here")
//! and no carrier for the check exists anywhere under crates/, so this row
//! stays documentary until MPP partition properties are ported. Note the Rust
//! side DOES own the underlying closure primitive already:
//! `tidb_funcdep::FdSet::closure_of_equivalence` (fd_graph.rs:223).

/// GO PORT of
/// `pkg/planner/property/physical_property_test.go:26
/// TestNeedEnforceExchangerWithHashByEquivalence` — see module docs for the
/// six-row expectation table re-derived from physical_property.go:484-511.
#[test]
#[ignore = "go-parity-gap: PhysicalProperty carries no MPPPartitionCols and NeedMPPExchangeByEquivalence has no crate carrier yet"]
fn need_mpp_exchange_by_equivalence_six_case_fd_table() {}
