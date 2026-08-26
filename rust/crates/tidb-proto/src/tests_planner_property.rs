//! Port of Go `pkg/planner/property/physical_property_test.go`, read from
//! `origin/master`.
//!
//! That file holds ONE test, `TestNeedEnforceExchangerWithHashByEquivalence`.
//! It pins `PhysicalProperty.NeedMPPExchangeByEquivalence(hashCols, fd)`
//! (`pkg/planner/property/physical_property.go`): for every required
//! `MPPPartitionCols` entry, the FD set's equivalence closure
//! (`FDSet.ClosureOfEquivalence`) is computed once; then every child-supplied
//! hash column must fall inside SOME required column's closure, guarded by
//! `checkEquivalence`'s collation rule (`key.CollateID < 0` requires an exact
//! `CollateID` match; `>= 0` passes — every row of the Go table uses the zero
//! value, i.e. the non-negative branch). A miss on any single key means that
//! key would mix the data distribution, so an MPP exchange IS needed (`true`);
//! all keys matching means the exchange can be eliminated (`false`). Note the
//! deliberate nuance the table pins: a plain strict FD
//! (`(9)-->(10-17)`) is NOT enough — case 3 requires the exchange even though
//! column 17 is functionally determined by hash column 9, because only
//! *equivalence* closures count.
//!
//! None of the exercised surface exists on the Rust side yet:
//! - `NeedMPPExchangeByEquivalence` and `checkEquivalence` have no port.
//!   `tidb-planner/src/physical_property.rs` deliberately omits
//!   `MPPPartitionCols` (its header defers MPP partitioning and FD sets to
//!   future planner layers), and `tidb-planner/src/enforce.rs` records
//!   `EnforceExchanger` — the caller of this predicate — as unported.
//! - The FD primitives the three Go builders use do exist as
//!   `tidb-funcdep::FdSet::{add_equivalence, add_strict,
//!   closure_of_equivalence}`, but this crate does not depend on that crate
//!   and, more importantly, the decision function under test is still missing.
//!
//! Following the workspace precedent for behavior that is not ported yet, the
//! test is recorded as an ignored go-parity gap rather than approximated:
//! re-implementing Go's closure/subset walk inside a test body would pin our
//! derivation, not Go's function.

/// Go `TestNeedEnforceExchangerWithHashByEquivalence`
/// (`pkg/planner/property/physical_property_test.go`): six table-driven cases
/// deciding whether an MPP exchange sender must be enforced between the child
/// hash partitioning and the parent's required `MPPPartitionCols`.
///
/// Expected values are the return of
/// `prop.NeedMPPExchangeByEquivalence(hashCols, fd)` where `prop` carries only
/// `MPPPartitionCols` and every column enters with `CollateID == 0`:
///
/// | # | fd builder     | required MPP cols | hash cols    | expected |
/// |---|----------------|-------------------|--------------|----------|
/// | 1 | buildTPCHQ3FD  | [18, 13, 16]      | [9]          | false    |
/// | 2 | buildTPCHQ3FD  | [18, 13, 16]      | [9, 13]      | false    |
/// | 3 | buildTPCHQ3FD  | [18, 13, 16]      | [9, 17]      | true     |
/// | 4 | buildTPCHQ3FD  | [18, 13, 16]      | [1, 17]      | true     |
/// | 5 | buildFD        | [1, 2, 3]         | [1, 2, 4, 5] | true     |
/// | 6 | buildFD2       | [1, 2]            | [1, 2, 5]    | false    |
///
/// The three FD fixtures, re-derived from the Go source:
/// - `buildTPCHQ3FD`: `(1)-->(2-6,8)`, `()-->(7)`, `(9)-->(10-17)`,
///   `(1,10)==(1,10)`, `(10,21)-->(19-33)`, plus equivalence unions
///   `{9,18}` and `{1,10}`. Required-column closures are therefore
///   18 -> {9,18}, 13 -> {13}, 16 -> {16}: hash 9 satisfies required 18
///   (case 1), hash 13 additionally satisfies required 13 (case 2), while 17
///   and 1 satisfy nothing (cases 3, 4 force the exchange — 17 is only
///   *determined* by 9, never equivalent).
/// - `buildFD`: equivalences `(2)==(4)` and `(3)==(4)`. Closures for the
///   required [1, 2, 3] are {1}, {2,4}, {3,4}; every hash key except 5 lands
///   in some closure, and the outside key 5 mixes the distribution (case 5
///   expects `true` despite all three required columns being coverable).
/// - `buildFD2`: equivalences `(2)==(4)` and `(2)==(5)`. Closures for the
///   required [1, 2] are {1} and {2,4,5}; all hash keys [1, 2, 5] land inside
///   them, so the exchange is eliminated (case 6 expects `false`).
#[test]
#[ignore = "go-parity-gap: NeedMPPExchangeByEquivalence/MPPPartitionColumn/FD-closure exchange pruning is not ported to the Rust workspace yet"]
fn need_enforce_exchanger_with_hash_by_equivalence() {
    // The six case rows live verbatim in Go physical_property_test.go; see the
    // module and test docs for the fixtures' intended FD content.
}
