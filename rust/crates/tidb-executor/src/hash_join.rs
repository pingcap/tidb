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

//! The equal-condition analysis, hash-key encoding and BUILD-SIDE CONTAINER
//! behind [`JoinExec`]'s hash path.
//!
//! Go source: `pkg/executor/join/hash_table_v1.go`'s `hashRowContainer`
//! (which is what [`BuildTable`] is), `pkg/executor/join/hash_join_v1.go`'s
//! `BuildWorkerV1.BuildHashTableForList`, and the `LogicalJoin.EqualConditions`
//! split the planner performs above all of it. v1 rather than v2 because this
//! executor is v1-shaped; see `crate::join`'s module doc for that
//! determination and what it excludes.
//!
//! # What the hash table is, and is not
//!
//! The table here is a PROBE ACCELERATOR, not a second implementation of `=`.
//! Every candidate pair a bucket produces is still handed to the join's own
//! `ON` evaluation, exactly as the nested loop hands it every pair. That
//! leaves one -- and only one -- correctness obligation on this module:
//!
//! > if `eq(a, b)` evaluates TRUE, then `key(a) == key(b)`.
//!
//! False POSITIVES (two unequal values that collide into one bucket) cost a
//! condition evaluation and are then rejected; false NEGATIVES would silently
//! drop rows. So every rule below is written to be conservative: a key shape
//! this module cannot encode EXACTLY makes the whole join fall back to the
//! nested loop rather than hash a guess.
//!
//! # Why a class, and why it must match on both sides
//!
//! `t.a = s.a` does not mean "the bytes are equal": MySQL picks a comparison
//! DOMAIN from the operand types, and `eval_binary_full` reproduces that
//! dispatch (float dominates decimal dominates integer; two byte-valued
//! operands compare under a collation instead). An `INT = DOUBLE` join key
//! compares in the float domain, where `1` and `1.0` are equal but hash
//! differently under any exact integer encoding -- a false negative.
//!
//! The gate is therefore that BOTH key columns have the same
//! [`EvalType`], which pins the domain, and that the domain is one this
//! module can encode injectively. Mixed domains keep the nested loop.
//!
//! # NULL
//!
//! `eq` is never TRUE when either operand is `NULL` -- not even for two
//! `NULL`s -- so a `NULL` key never matches anything, including another
//! `NULL`. That is the same rule `tidb-exec`'s `join_key_eq` states. Here it
//! is expressed structurally: a row whose key contains `NULL` produces no
//! key at all, so it is never inserted into the table and never probes it.
//! A NaN `DOUBLE` is treated the same way, because `NaN = NaN` is false.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::{Arc, Mutex};
use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_in_disk::DiskError;
use tidb_chunk::column::Column;
use tidb_chunk::list::RowPtr;
use tidb_chunk::row::Row;
use tidb_chunk::row_container::{RowContainer, SpillDiskAction};
use tidb_codec::{JoinKeyBytes, JoinKeyColumn};
use tidb_datatype::{Collation, Datum, EvalType, FieldType};
use tidb_expr::expression::Expression;
use tidb_util::memory::Tracker;
use tidb_util::spill_storage::SpillStorage;

/// Go's hash join feeds encoded key columns to `hash/fnv.New64`. The complete
/// encoded key is still compared after a bucket hit, so this hash is only a
/// fast bucket selector and collisions cannot change SQL results.
#[derive(Default)]
pub(crate) struct FastBytesHasher {
    hash: u64,
    initialized: bool,
}

impl Hasher for FastBytesHasher {
    fn finish(&self) -> u64 {
        if self.initialized {
            self.hash
        } else {
            0xcbf29ce484222325
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = if self.initialized {
            self.hash
        } else {
            self.initialized = true;
            0xcbf29ce484222325
        };
        for byte in bytes {
            hash = hash.wrapping_mul(0x100000001b3);
            hash ^= u64::from(*byte);
        }
        self.hash = hash;
    }
}

pub(crate) type FastBytesMap<V> = HashMap<Vec<u8>, V, BuildHasherDefault<FastBytesHasher>>;

#[derive(Default)]
pub(crate) struct IdentityU64Hasher(u64);

impl Hasher for IdentityU64Hasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = FastBytesHasher::default();
        hash.write(bytes);
        self.0 = hash.finish();
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

type HashBuckets<V> = HashMap<u64, V, BuildHasherDefault<IdentityU64Hasher>>;

/// One distinct exact-integer key: its value, its first row's pointer inline
/// so a unique key (the dimension-join case, where `first == last`) needs no
/// chain-entry read, and its chain.
#[derive(Clone, Copy, Debug)]
struct ExactKey {
    key: i128,
    first_ptr: RowPtr,
    head: ChainHead,
}

/// The exact-integer table, shaped like Go's v2 `subTable`: a power-of-two
/// array of 8-byte tagged slots over the distinct keys, at most three
/// quarters full. A probe reads one slot -- the array stays cache-resident
/// for a dimension table (2 MB for 150k keys) -- and, only when the slot's
/// hash tag matches, the key it names. The `HashMap<i128, _>` this
/// replaces kept a 32-byte slot per key, an 8 MB table for the same keys
/// that missed cache and TLB on every probe row.
#[derive(Debug)]
struct ExactIntTable {
    /// `tag << 32 | key index + 1`; zero is an empty slot.
    slots: Vec<u64>,
    mask: usize,
    keys: Vec<ExactKey>,
}

const EXACT_INT_MIN_SLOTS: usize = 64;
/// The slot bits that hold the hash tag.
const TAG_MASK: u64 = 0xFFFF_FFFF_0000_0000;

/// The key's hash: its two 64-bit halves folded with a multiply, then
/// murmur3's `fmix64`. The finalizer is what keeps consecutive dimension
/// keys out of consecutive slots (a byte-wise FNV left them differing only
/// in their low bits, and linear probing turned that adjacency into full
/// clusters); the fold is a bijection on the low half for the small values
/// integer keys usually are, so those never collide. Three multiplies per
/// key, where the FNV pass over sixteen bytes was sixteen dependent ones.
fn exact_int_hash(key: i128) -> u64 {
    let low = key as u64;
    let high = (key >> 64) as u64;
    let mut hash = low ^ high.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^ (hash >> 33)
}

impl ExactIntTable {
    fn new() -> Self {
        Self {
            slots: vec![0; EXACT_INT_MIN_SLOTS],
            mask: EXACT_INT_MIN_SLOTS - 1,
            keys: Vec::new(),
        }
    }

    /// The slot's tag: the hash's high half, never zero so a filled slot is
    /// never mistaken for an empty one.
    fn tag(hash: u64) -> u64 {
        ((hash >> 32) | 1) << 32
    }

    /// The index of `key` in `keys`, if present.
    #[inline(always)]
    fn find(&self, key: i128) -> Option<usize> {
        let hash = exact_int_hash(key);
        self.find_from(hash as usize & self.mask, Self::tag(hash), key)
    }

    /// The serial probe from `position` on.
    #[inline(always)]
    fn find_from(&self, mut position: usize, tag: u64, key: i128) -> Option<usize> {
        loop {
            let slot = self.slots[position];
            if slot == 0 {
                return None;
            }
            if slot & TAG_MASK == tag {
                let index = (slot as u32 as usize) - 1;
                if self.keys[index].key == key {
                    return Some(index);
                }
            }
            position = (position + 1) & self.mask;
        }
    }

    /// [`Self::find`] for a whole probe chunk, in two passes: the first
    /// hashes every key and loads its home slot, the second checks the
    /// slot's tag and key. Neither pass's loads depend on one another, so
    /// the CPU overlaps their cache misses instead of paying one full miss
    /// per probe row against a table larger than the cache (the row-at-a-
    /// time probe of TPC-H q9's 1.5M orders rows against 300k order keys
    /// spent 14% of the node's CPU waiting on that slot load). A key whose
    /// home slot holds another key finishes with the serial probe.
    fn find_many(
        &self,
        keys: &[Option<i128>],
        home_slots: &mut Vec<(u64, u64)>,
        found: &mut Vec<Option<usize>>,
    ) {
        home_slots.clear();
        home_slots.reserve(keys.len());
        for key in keys {
            let slot = match key {
                Some(key) => {
                    let hash = exact_int_hash(*key);
                    (hash, self.slots[hash as usize & self.mask])
                }
                None => (0, 0),
            };
            home_slots.push(slot);
        }
        found.clear();
        found.resize(keys.len(), None);
        for index in 0..keys.len() {
            let Some(key) = keys[index] else {
                continue;
            };
            let (hash, slot) = home_slots[index];
            if slot == 0 {
                continue;
            }
            let tag = Self::tag(hash);
            if slot & TAG_MASK == tag {
                let key_index = (slot as u32 as usize) - 1;
                if self.keys[key_index].key == key {
                    found[index] = Some(key_index);
                    continue;
                }
            }
            found[index] = self.find_from((hash as usize + 1) & self.mask, tag, key);
        }
    }

    fn place(slots: &mut [u64], mask: usize, hash: u64, index: usize) {
        let mut position = hash as usize & mask;
        while slots[position] != 0 {
            position = (position + 1) & mask;
        }
        slots[position] = Self::tag(hash) | (index as u64 + 1);
    }

    /// Adds a key that `find` did not have.
    fn push(&mut self, entry: ExactKey) {
        if (self.keys.len() + 1) * 4 > self.slots.len() * 3 {
            let slots = self.slots.len() * 2;
            self.slots = vec![0; slots];
            self.mask = slots - 1;
            for (index, existing) in self.keys.iter().enumerate() {
                Self::place(
                    &mut self.slots,
                    self.mask,
                    exact_int_hash(existing.key),
                    index,
                );
            }
        }
        let index = self.keys.len();
        let hash = exact_int_hash(entry.key);
        self.keys.push(entry);
        Self::place(&mut self.slots, self.mask, hash, index);
    }

    fn bytes(&self) -> usize {
        self.slots.capacity() * std::mem::size_of::<u64>()
            + self.keys.capacity() * std::mem::size_of::<ExactKey>()
    }
}

/// Go v1 `entry`: one build-row pointer and the next entry under the same
/// key (`CHAIN_END` closes the chain).
#[derive(Clone, Copy, Debug)]
struct ChainEntry {
    ptr: RowPtr,
    next: u32,
}

const CHAIN_END: u32 = u32::MAX;

/// A key's chain: its first entry, and its last so the next build row is
/// appended in input order (see the ordering note on [`BuildTable`]).
#[derive(Clone, Copy, Debug)]
struct ChainHead {
    first: u32,
    last: u32,
}

/// Go v1 `entryStore`: every chain entry of the table in one slab, so a new
/// key or a new row under a key costs no allocation of its own.
#[derive(Default)]
struct ChainStore {
    entries: Vec<ChainEntry>,
}

/// The bytes Go's `entryStore` has charged once it holds `entries`: slabs
/// of 16-byte entries (`unsafe.Sizeof(entry{})`) that start at 64 entries
/// and double up to 8192 (`initialEntrySliceLen`, `maxEntrySliceLen`), each
/// charged whole when allocated (`GetStore`). The slab is Go's memory
/// contract for the query quota; the Rust slab is one vector.
fn go_entry_store_bytes(entries: usize) -> usize {
    const ENTRY_BYTES: usize = 16;
    const INITIAL_SLICE_LEN: usize = 64;
    const MAX_SLICE_LEN: usize = 8192;
    let mut charged = INITIAL_SLICE_LEN;
    let mut slab = INITIAL_SLICE_LEN;
    while charged < entries {
        slab = (slab * 2).min(MAX_SLICE_LEN);
        charged += slab;
    }
    charged * ENTRY_BYTES
}

impl ChainStore {
    /// Appends `ptr` under `head` (a new chain when `None`) and returns the
    /// head to keep in the map.
    fn push(&mut self, head: Option<ChainHead>, ptr: RowPtr) -> Result<ChainHead, BuildError> {
        let index = u32::try_from(self.entries.len())
            .ok()
            .filter(|index| *index != CHAIN_END)
            .ok_or(BuildError::Key)?;
        self.entries.push(ChainEntry {
            ptr,
            next: CHAIN_END,
        });
        Ok(match head {
            Some(head) => {
                self.entries[head.last as usize].next = index;
                ChainHead {
                    first: head.first,
                    last: index,
                }
            }
            None => ChainHead {
                first: index,
                last: index,
            },
        })
    }

    fn chain(&self, head: Option<&ChainHead>) -> Chain<'_> {
        Chain {
            entries: &self.entries,
            next: head.map_or(CHAIN_END, |head| head.first),
        }
    }
}

/// The build rows chained under one key, in build order. `Copy`, so a
/// caller can walk it more than once.
#[derive(Clone, Copy)]
pub(crate) struct Chain<'a> {
    entries: &'a [ChainEntry],
    next: u32,
}

impl<'a> Chain<'a> {
    pub(crate) fn empty() -> Self {
        Chain {
            entries: &[],
            next: CHAIN_END,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.next == CHAIN_END
    }

    /// The number of rows in the chain, walked.
    pub(crate) fn len(&self) -> usize {
        (*self).count()
    }

    pub(crate) fn first(&self) -> Option<RowPtr> {
        let mut chain = *self;
        chain.next()
    }

    /// The chain's only row, `None` when it holds none or more than one.
    pub(crate) fn single(&self) -> Option<RowPtr> {
        let mut chain = *self;
        let first = chain.next()?;
        chain.next().is_none().then_some(first)
    }
}

impl Iterator for Chain<'_> {
    type Item = RowPtr;

    fn next(&mut self) -> Option<RowPtr> {
        if self.next == CHAIN_END {
            return None;
        }
        let entry = self.entries[self.next as usize];
        self.next = entry.next;
        Some(entry.ptr)
    }
}

/// The comparison domain a hash join key column is encoded in.
///
/// One variant per arm of `eval_binary_full`'s comparison dispatch that this
/// module can encode injectively.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyClass {
    /// Both keys are integral. Go `types.CompareInt` (and this tier's
    /// `integer_cmp`) never reinterprets one side's bit pattern in the
    /// other's domain, so the key is the value's exact `i128` position:
    /// `-1` and `2^64-1` land in different buckets, as they must.
    Int,
    /// Both keys are floating point; the comparison promotes to `f64`.
    Real,
    /// Both keys are exact decimals; the comparison is exact and
    /// scale-insensitive, so the key is Go `MyDecimal.ToHashKey`.
    Decimal,
    /// Both keys are byte-valued (`CHAR`/`VARCHAR`/`BINARY`/`ENUM`/`SET`).
    /// The key is the comparison collation's SORT KEY, which is what makes a
    /// case-insensitive join match `'a'` with `'A'` and a PAD SPACE
    /// collation match `'a'` with `'a  '` -- the same rule the `GROUP BY`
    /// path applies in `hash_agg.rs`'s `group_key_part`.
    Str(Collation),
}

impl KeyClass {
    /// The class two key columns of `field` type share, or `None` when their
    /// comparison domain is one this module refuses to encode.
    ///
    /// `collation` is the one the expression derivation stamped on the `eq`
    /// itself, which is the collation its own evaluation runs under -- so a
    /// string key hashes under exactly the rule that will later be asked to
    /// confirm the match.
    ///
    /// `Datetime`/`Timestamp`/`Duration`/`Json`/`VectorFloat32` are
    /// deliberately absent: their equality involves timezone resolution,
    /// fractional-second precision, or structural folding that this unit has
    /// not proven injective, and an unproven key is a dropped row.
    pub(crate) fn of(left: &FieldType, right: &FieldType, collation: Collation) -> Option<Self> {
        let eval_type = left.eval_type();
        if eval_type != right.eval_type() {
            return None;
        }
        match eval_type {
            EvalType::Int => Some(KeyClass::Int),
            EvalType::Real => Some(KeyClass::Real),
            EvalType::Decimal => Some(KeyClass::Decimal),
            EvalType::String => Some(KeyClass::Str(collation)),
            _ => None,
        }
    }
}

/// One `probe.col = build.col` or `probe.col <=> build.col` conjunct the hash
/// table indexes.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EquiKey {
    /// The key column's offset inside a LEFT-child row.
    pub(crate) left: usize,
    /// The key column's offset inside a RIGHT-child row.
    pub(crate) right: usize,
    /// The domain both offsets are encoded in.
    pub(crate) class: KeyClass,
    /// Whether NULL is a matchable key value (`<=>`) instead of an
    /// unmatchable row (`=`). Go carries this as `PhysicalHashJoin.IsNullEQ`.
    pub(crate) null_safe: bool,
}

/// The `ON` clause split the way Go's planner splits `LogicalJoin`:
/// `EqualConditions` that the hash table can index, and everything else.
pub(crate) struct EquiSplit {
    /// The indexable `col = col` conjuncts, in `ON` order (Go prints them in
    /// this order inside `equal:[...]`).
    pub(crate) keys: Vec<EquiKey>,
    /// One flag per flattened `ON` conjunct: `true` where that conjunct
    /// became an [`EquiKey`]. `EXPLAIN` renders the `true` positions as
    /// `equal:[...]` and the rest as `other cond:`, so the printed plan and
    /// the executed plan cannot drift apart.
    pub(crate) equal_mask: Vec<bool>,
}

/// Flattens an `AND` tree into its conjuncts, left to right.
///
/// Go's `expression.SplitCNFItems`; the driver hands the join a single
/// rewritten `ON` expression, so the split happens here rather than at the
/// plan layer.
pub(crate) fn split_conjuncts(expr: &Expression) -> Vec<&Expression> {
    let mut out = Vec::new();
    push_conjuncts(expr, &mut out);
    out
}

fn push_conjuncts<'a>(expr: &'a Expression, out: &mut Vec<&'a Expression>) {
    if let Expression::ScalarFunction(f) = expr {
        if f.func_name.lowercase() == "and" && f.args.len() == 2 {
            push_conjuncts(&f.args[0], out);
            push_conjuncts(&f.args[1], out);
            return;
        }
    }
    out.push(expr);
}

/// Splits `conditions` (the join's `ON` clause) into hash keys and the rest.
///
/// `left_width` is the number of columns the LEFT child contributes; the
/// join evaluates its conditions against the concatenated row, so a column
/// whose index is below `left_width` belongs to the left child and any other
/// to the right.
///
/// A conjunct becomes a key only when it is literally `eq(<col>, <col>)` or
/// `nulleq(<col>, <col>)` with one column from each side. A cast, a computed
/// expression, or two columns from the SAME side is left in the residual set:
/// the first two would need the key to be built from an evaluated expression
/// (Go does that; this unit does not), and the third is a filter, not a join
/// key.
/// TRUE when some conjunct is an equality whose two argument trees read
/// columns from OPPOSITE sides of the join -- exactly the conjuncts Go's
/// `updateEQCond` (`logical_join.go`) turns into join KEYS, injecting child
/// projections when a side is not a bare column. Such a join runs Go's
/// KEYED hash join even when [`split_equi`] extracts nothing here (the
/// cast-typed keys of `planner/core/join_key_type_cast`), and the recorded
/// keyed match order reads FORWARD; only a join with no cross-side
/// equality at all is Go's v1 cross join with the reversed single-chain
/// order. See [`BuildTable`]'s doc for the measurements.
///
/// One of these joins CAN leave the nested path now: an index-family hint
/// naming the signed-int side probes that side's handle with the computed
/// `cast(str AS SIGNED)` key produced by the logical planner, running
/// [`crate::join::JoinExec`]'s index strategy. Its emission is outer-major
/// in outer order -- the same forward order this marker pins for the
/// nested path -- so the boundary stays order-consistent. The hash
/// statements themselves still run here: this port carries the rewritten
/// key on the index plan alone rather than making it a split key.
pub(crate) fn has_cross_side_equality(conditions: &[Expression], left_width: usize) -> bool {
    fn sides(expr: &Expression, left_width: usize, has: &mut (bool, bool)) {
        match expr {
            Expression::Column(column) => {
                if usize::try_from(column.index).is_ok_and(|index| index < left_width) {
                    has.0 = true;
                } else {
                    has.1 = true;
                }
            }
            Expression::ScalarFunction(f) => {
                for arg in &f.args {
                    sides(arg, left_width, has);
                }
            }
            Expression::Constant(_) | Expression::CorrelatedColumn(_) => {}
        }
    }
    conditions.iter().flat_map(split_conjuncts).any(|conjunct| {
        let Expression::ScalarFunction(f) = conjunct else {
            return false;
        };
        let name = f.func_name.lowercase();
        if (name != "eq" && name != "nulleq") || f.args.len() != 2 {
            return false;
        }
        let mut first = (false, false);
        let mut second = (false, false);
        sides(&f.args[0], left_width, &mut first);
        sides(&f.args[1], left_width, &mut second);
        matches!(
            (first, second),
            ((true, false), (false, true)) | ((false, true), (true, false))
        )
    })
}

pub(crate) fn split_equi(conditions: &[Expression], left_width: usize) -> EquiSplit {
    let mut keys = Vec::new();
    let mut equal_mask = Vec::new();
    for condition in conditions {
        for conjunct in split_conjuncts(condition) {
            let key = equi_key(conjunct, left_width);
            equal_mask.push(key.is_some());
            keys.extend(key);
        }
    }
    EquiSplit { keys, equal_mask }
}

pub(crate) fn equi_key(conjunct: &Expression, left_width: usize) -> Option<EquiKey> {
    // Go keeps IN equalities in OtherConditions unless the planner explicitly
    // builds null-aware keys. Ordinary hashing would discard UNKNOWN pairs.
    if crate::joiner::is_eq_cond_from_in(conjunct) {
        return None;
    }
    let Expression::ScalarFunction(f) = conjunct else {
        return None;
    };
    let name = f.func_name.lowercase();
    let null_safe = if name == "eq" {
        false
    } else if name == "nulleq" {
        true
    } else {
        return None;
    };
    if f.args.len() != 2 {
        return None;
    }
    let (Expression::Column(a), Expression::Column(b)) = (&f.args[0], &f.args[1]) else {
        return None;
    };
    let (a_index, b_index) = (
        usize::try_from(a.index).ok()?,
        usize::try_from(b.index).ok()?,
    );
    let (left_col, right_col, left, right) = match (a_index < left_width, b_index < left_width) {
        (true, false) => (a, b, a_index, b_index - left_width),
        (false, true) => (b, a, b_index, a_index - left_width),
        // Both columns on one side: a filter this join applies, not a key
        // that pairs the two children.
        _ => return None,
    };
    let class = KeyClass::of(
        left_col.ret_type.as_ref()?,
        right_col.ret_type.as_ref()?,
        f.derived_collation(),
    )?;
    Some(EquiKey {
        left,
        right,
        class,
        null_safe,
    })
}

/// Encodes one key column of one row, or `None` when the value can never
/// satisfy `eq` (a `NULL`, or a NaN `DOUBLE`).
///
/// # Errors
/// [`KeyError`] when the datum is outside the statically determined class.
/// The class comes from the key columns' own field types and the chunks are
/// typed, so this is an invariant violation rather than a data condition --
/// it is surfaced instead of guessed, because a guess here silently drops
/// rows.
fn key_part(class: KeyClass, datum: &Datum) -> Result<Option<Vec<u8>>, KeyError> {
    if matches!(datum, Datum::Null) {
        return Ok(None);
    }
    Ok(match class {
        // `integer_cmp` orders a signed and an unsigned operand on one number
        // line without reinterpreting either bit pattern, so the exact `i128`
        // position of the value IS its equality class.
        KeyClass::Int => Some(datum_int_key(datum).ok_or(KeyError)?.to_be_bytes().to_vec()),
        // A float comparison promotes an integral operand to `f64`, so an
        // integral datum in a float key column encodes as its `f64` value.
        // `-0.0 == 0.0` is TRUE, so the two must share a key; `NaN = NaN` is
        // FALSE, so a NaN never gets one.
        KeyClass::Real => {
            let value = match datum {
                Datum::Real(value) | Datum::Float32(value) => *value,
                Datum::Int(value) => *value as f64,
                Datum::UInt(value) => *value as f64,
                _ => return Err(KeyError),
            };
            if value.is_nan() {
                None
            } else if value == 0.0 {
                Some(0.0f64.to_be_bytes().to_vec())
            } else {
                Some(value.to_be_bytes().to_vec())
            }
        }
        // Decimal comparison is exact and promotes an integral operand to a
        // scale-0 decimal. `MyDecimal.ToHashKey` is Go's own answer to
        // "equal decimals, equal bytes" -- it normalizes away the trailing
        // zeros that make `1.0` and `1.00` differ in representation.
        KeyClass::Decimal => {
            let value = match datum {
                Datum::Decimal(value) => value.clone(),
                Datum::Int(value) => tidb_datatype::Decimal::from_int(*value),
                Datum::UInt(value) => tidb_datatype::Decimal::from_uint(*value),
                _ => return Err(KeyError),
            };
            Some(value.to_hash_key().map_err(|_| KeyError)?.0)
        }
        // Two byte-valued operands compare under the collation the
        // expression derivation stamped on this `eq`; its sort key is equal
        // exactly when the collation calls the values equal.
        KeyClass::Str(collation) => match datum.as_raw_bytes() {
            Some(bytes) => Some(collation.key(bytes)),
            None => return Err(KeyError),
        },
    })
}

/// A datum outside its key column's statically determined class.
#[derive(Debug)]
pub(crate) struct KeyError;

/// The whole key of one row: every key column's encoding, length-prefixed so
/// two columns cannot borrow each other's bytes (`('ab', 'c')` and
/// `('a', 'bc')` are different keys).
///
/// `None` when any part is `None` -- one unmatched key column is enough to
/// make the row match nothing.
pub(crate) fn row_key(
    keys: &[EquiKey],
    row: &[Datum],
    offset: impl Fn(&EquiKey) -> usize,
) -> Result<Option<Vec<u8>>, KeyError> {
    row_key_by(keys, |key| row[offset(key)].clone())
}

/// Encodes a row key through a lazy key-column accessor. Index lookup joins
/// use this when the source row has many projected columns but the join key is
/// narrow; decoding only the key columns avoids materializing the rest of the
/// inner row before the chunk-backed output path consumes it.
pub(crate) fn row_key_by(
    keys: &[EquiKey],
    mut datum: impl FnMut(&EquiKey) -> Datum,
) -> Result<Option<Vec<u8>>, KeyError> {
    // Integer and floating-point key parts are fixed-width. Reserving their
    // framing up front avoids the repeated reallocations that otherwise show
    // up in every probe of a TPC-H index/hash join. Variable-width decimal and
    // string keys still grow naturally below.
    let mut encoded = Vec::with_capacity(keys.len().saturating_mul(24));
    for key in keys {
        let value = datum(key);
        if matches!(value, Datum::Null) {
            if key.null_safe {
                // A real part length can never be u64::MAX. This gives NULL
                // one collision-free key identity without inventing a value
                // in any SQL comparison domain.
                encoded.extend_from_slice(&u64::MAX.to_be_bytes());
                continue;
            }
            return Ok(None);
        }
        let part_start = encoded.len();
        encoded.resize(part_start + 8, 0);
        let value_start = encoded.len();
        let present = match key.class {
            KeyClass::Int => {
                encoded.extend_from_slice(&datum_int_key(&value).ok_or(KeyError)?.to_be_bytes());
                true
            }
            KeyClass::Real => {
                let value = match value {
                    Datum::Real(value) | Datum::Float32(value) => value,
                    Datum::Int(value) => value as f64,
                    Datum::UInt(value) => value as f64,
                    _ => return Err(KeyError),
                };
                if value.is_nan() {
                    false
                } else {
                    encoded
                        .extend_from_slice(&(if value == 0.0 { 0.0 } else { value }).to_be_bytes());
                    true
                }
            }
            _ => match key_part(key.class, &value)? {
                Some(part) => {
                    encoded.extend_from_slice(&part);
                    true
                }
                None => false,
            },
        };
        if !present {
            encoded.truncate(part_start);
            return Ok(None);
        }
        let part_len = encoded.len() - value_start;
        encoded[part_start..value_start].copy_from_slice(&(part_len as u64).to_be_bytes());
    }
    Ok(Some(encoded))
}

/// Go `hashChunkSelected`: hashes the encoded key directly into one FNV-64
/// bucket without retaining a temporary byte slice. The bucket hit is only a
/// candidate; [`equi_keys_equal`] performs Go's `matchJoinKey` collision
/// check before any row is emitted.
pub(crate) fn row_hash(
    keys: &[EquiKey],
    row: &[Datum],
    offset: impl Fn(&EquiKey) -> usize,
) -> Result<Option<u64>, KeyError> {
    row_hash_by(keys, |key| row[offset(key)].clone())
}

pub(crate) fn row_hash_by(
    keys: &[EquiKey],
    mut datum: impl FnMut(&EquiKey) -> Datum,
) -> Result<Option<u64>, KeyError> {
    let mut encoded = FastBytesHasher::default();
    for key in keys {
        let value = datum(key);
        if matches!(value, Datum::Null) {
            if key.null_safe {
                encoded.write(&u64::MAX.to_be_bytes());
                continue;
            }
            return Ok(None);
        }
        let present = match key.class {
            KeyClass::Int => {
                encoded.write(&16u64.to_be_bytes());
                encoded.write(&datum_int_key(&value).ok_or(KeyError)?.to_be_bytes());
                true
            }
            KeyClass::Real => {
                let value = match value {
                    Datum::Real(value) | Datum::Float32(value) => value,
                    Datum::Int(value) => value as f64,
                    Datum::UInt(value) => value as f64,
                    _ => return Err(KeyError),
                };
                if value.is_nan() {
                    false
                } else {
                    encoded.write(&8u64.to_be_bytes());
                    encoded.write(&(if value == 0.0 { 0.0 } else { value }).to_be_bytes());
                    true
                }
            }
            _ => match key_part(key.class, &value)? {
                Some(part) => {
                    encoded.write(&(part.len() as u64).to_be_bytes());
                    encoded.write(&part);
                    true
                }
                None => false,
            },
        };
        if !present {
            return Ok(None);
        }
    }
    Ok(Some(encoded.finish()))
}

/// Chunk-backed hash construction used by the build worker. Fixed-width key
/// columns are already typed in the chunk, so decoding them into temporary
/// `Datum`s would only add work and allocations before the FNV write.
pub(crate) fn row_hash_chunk(
    keys: &[EquiKey],
    row: Row<'_>,
    types: &[FieldType],
    offset: impl Fn(&EquiKey) -> usize,
) -> Result<Option<u64>, KeyError> {
    let mut encoded = FastBytesHasher::default();
    for key in keys {
        let at = offset(key);
        if row.is_null(at) {
            if key.null_safe {
                encoded.write(&u64::MAX.to_be_bytes());
                continue;
            }
            return Ok(None);
        }
        match key.class {
            KeyClass::Int => {
                encoded.write(&16u64.to_be_bytes());
                encoded.write(
                    &chunk_int_key(row, at, &types[at])
                        .ok_or(KeyError)?
                        .to_be_bytes(),
                );
            }
            KeyClass::Real => {
                let value = match types[at].code() {
                    tidb_datatype::FieldTypeCode::Float => f64::from(row.get_float32(at)),
                    _ => row.get_float64(at),
                };
                if value.is_nan() {
                    return Ok(None);
                }
                encoded.write(&8u64.to_be_bytes());
                encoded.write(&(if value == 0.0 { 0.0 } else { value }).to_be_bytes());
            }
            KeyClass::Decimal if is_decimal_cell(&types[at]) => {
                // Go's `HashChunkSelected` hashes a DECIMAL column through
                // `MyDecimal.ToHashKey` on the packed words; the datum path
                // below would first print the digits into a `Decimal`.
                let part = row.get_my_decimal(at).to_hash_key().map_err(|_| KeyError)?;
                encoded.write(&(part.len() as u64).to_be_bytes());
                encoded.write(&part);
            }
            _ => {
                let value = row.get_datum(at, &types[at]);
                let Some(part) = key_part(key.class, &value)? else {
                    return Ok(None);
                };
                encoded.write(&(part.len() as u64).to_be_bytes());
                encoded.write(&part);
            }
        }
    }
    Ok(Some(encoded.finish()))
}

/// Hashes a chunk's numeric key columns in Go's column-major order while
/// retaining the byte framing used by [`row_hash_chunk`]. The FNV state still
/// belongs to each logical row, but each key column is resolved once and then
/// scanned across the whole batch, so fixed-width reads do not rebuild a row
/// cursor for every key part.
///
/// `None` means that one of the key columns needs the general datum path
/// (currently hybrid columns). Callers can then fall back to
/// [`row_hash_chunk`] without changing the accepted key domain.
pub(crate) fn row_hash_chunk_batched(
    chunk: &Chunk,
    keys: &[EquiKey],
    types: &[FieldType],
    offset: impl Fn(&EquiKey) -> usize,
    selected: &[bool],
) -> Result<Option<Vec<Option<u64>>>, KeyError> {
    for key in keys {
        let at = offset(key);
        let field_type = types.get(at).ok_or(KeyError)?;
        let supported = match key.class {
            KeyClass::Int => !field_type.is_hybrid(),
            KeyClass::Real => matches!(
                field_type.code(),
                tidb_datatype::FieldTypeCode::Float | tidb_datatype::FieldTypeCode::Double
            ),
            KeyClass::Decimal => is_decimal_cell(field_type),
            // ENUM/SET/BIT are hybrid cells whose comparison representation
            // depends on their metadata. Plain byte-valued columns already
            // expose the exact payload used by `key_part`, so they can stay
            // in the column-major path without materializing a `Datum` per
            // row.
            KeyClass::Str(_) => is_raw_string_cell(field_type),
        };
        if !supported {
            return Ok(None);
        }
    }

    let rows = chunk.num_rows();
    let selection = chunk.sel();
    let mut hashers = (0..rows)
        .map(|_| FastBytesHasher::default())
        .collect::<Vec<_>>();
    let mut active = vec![true; rows];
    if !selected.is_empty() {
        for row_idx in 0..rows {
            if !selected[row_idx] {
                active[row_idx] = false;
            }
        }
    }

    // Go's HashChunkSelected walks one key column at a time. Keep that
    // column-major order, but split the selected and unselected cases so the
    // hot row loop does not repeatedly branch through an optional selection
    // vector or remap a physical index for every key part.
    for key in keys {
        let at = offset(key);
        let field_type = types.get(at).ok_or(KeyError)?;
        let column = chunk.column(at);
        // Hold one backing-data read guard for the entire key column, as Go's
        // `Int64s`/`Float64s`/`GetBytes` accessors do. The old per-row typed
        // access reacquired that guard for every cell, which made the Rust
        // column-major loop pay synchronization and bounds work that Go
        // avoids.
        column.with_raw(|raw| {
            if let Some(selection) = selection {
                for (row_idx, &physical_row) in selection.iter().enumerate() {
                    if !active[row_idx] {
                        continue;
                    }
                    hash_chunk_key_row(
                        key,
                        field_type,
                        &column,
                        &raw,
                        row_idx,
                        physical_row,
                        &mut hashers,
                        &mut active,
                    )?;
                }
            } else {
                for row_idx in 0..rows {
                    if !active[row_idx] {
                        continue;
                    }
                    hash_chunk_key_row(
                        key,
                        field_type,
                        &column,
                        &raw,
                        row_idx,
                        row_idx,
                        &mut hashers,
                        &mut active,
                    )?;
                }
            }
            Ok::<(), KeyError>(())
        })?;
    }
    Ok(Some(
        hashers
            .into_iter()
            .zip(active)
            .map(|(hasher, active)| active.then(|| hasher.finish()))
            .collect(),
    ))
}

/// Hash one already-selected physical cell while retaining the comparison
/// framing used by [`row_hash_chunk`] and [`row_hash`]. The helper is kept
/// inline so the two selection-specialized loops above compile to the same
/// straight-line typed reads without duplicating their match arms.
#[inline(always)]
fn hash_chunk_key_row(
    key: &EquiKey,
    field_type: &FieldType,
    column: &Column,
    raw: &JoinKeyBytes<'_>,
    row_idx: usize,
    physical_row: usize,
    hashers: &mut [FastBytesHasher],
    active: &mut [bool],
) -> Result<(), KeyError> {
    let hasher = &mut hashers[row_idx];
    if column.is_null(physical_row) {
        if key.null_safe {
            hasher.write(&u64::MAX.to_be_bytes());
        } else {
            active[row_idx] = false;
        }
        return Ok(());
    }
    match key.class {
        KeyClass::Int => {
            hasher.write(&16u64.to_be_bytes());
            let raw = raw.row(physical_row);
            let value = if field_type.is_unsigned() {
                i128::from(u64::from_ne_bytes(raw.try_into().map_err(|_| KeyError)?))
            } else {
                i128::from(i64::from_ne_bytes(raw.try_into().map_err(|_| KeyError)?))
            };
            hasher.write(&value.to_be_bytes());
        }
        KeyClass::Real => {
            let raw = raw.row(physical_row);
            let value = if field_type.code() == tidb_datatype::FieldTypeCode::Float {
                f64::from(f32::from_ne_bytes(raw.try_into().map_err(|_| KeyError)?))
            } else {
                f64::from_ne_bytes(raw.try_into().map_err(|_| KeyError)?)
            };
            if value.is_nan() {
                active[row_idx] = false;
                return Ok(());
            }
            hasher.write(&8u64.to_be_bytes());
            hasher.write(&(if value == 0.0 { 0.0 } else { value }).to_be_bytes());
        }
        KeyClass::Decimal => {
            let value = tidb_datatype::MyDecimal::from_raw_bytes(
                raw.row(physical_row).try_into().map_err(|_| KeyError)?,
            )
            .map_err(|_| KeyError)?;
            let part = value.to_hash_key().map_err(|_| KeyError)?;
            hasher.write(&(part.len() as u64).to_be_bytes());
            hasher.write(&part);
        }
        KeyClass::Str(collation) => {
            let part = collation.immutable_key(raw.row(physical_row));
            hasher.write(&(part.len() as u64).to_be_bytes());
            hasher.write(part.as_ref());
        }
    }
    Ok(())
}

/// Whether a chunk column stores Go `MyDecimal` cells, so a decimal join key
/// can be read as one instead of through a datum.
fn is_decimal_cell(field_type: &FieldType) -> bool {
    field_type.code() == tidb_datatype::FieldTypeCode::NewDecimal
}

/// Plain byte-valued cells use the same payload that [`key_part`] reads from
/// a materialized datum. Hybrid ENUM/SET/BIT cells are intentionally excluded:
/// Go hashes their ordinal/name representation through field metadata, which
/// is not the raw variable-column payload.
fn is_raw_string_cell(field_type: &FieldType) -> bool {
    matches!(
        field_type.code(),
        tidb_datatype::FieldTypeCode::String
            | tidb_datatype::FieldTypeCode::Varchar
            | tidb_datatype::FieldTypeCode::VarString
            | tidb_datatype::FieldTypeCode::Blob
            | tidb_datatype::FieldTypeCode::TinyBlob
            | tidb_datatype::FieldTypeCode::MediumBlob
            | tidb_datatype::FieldTypeCode::LongBlob
    )
}

/// Returns the exact signed comparison-domain value for the one-key integer
/// fast path. The caller has already proved that the key is not NULL-safe, so
/// NULL is an unmatchable row and can be represented by `None`.
pub(crate) fn exact_int_key_chunk(
    row: Row<'_>,
    offset: usize,
    field_type: &FieldType,
) -> Option<i128> {
    chunk_int_key(row, offset, field_type)
}

/// Extracts one exact integer key column for a complete logical chunk. Go's
/// exact-key path reads the column vector once and then probes every row;
/// doing the same here avoids rebuilding a [`Row`] and reacquiring the packed
/// data guard for each lookup. Hybrid integer cells stay on the scalar datum
/// path because their variable-width storage is not an eight-byte integer.
pub(crate) fn exact_int_keys_chunk(
    chunk: &Chunk,
    offset: usize,
    field_type: &FieldType,
) -> Vec<Option<i128>> {
    let rows = chunk.num_rows();
    let column = chunk.column(offset);
    let mut keys = Vec::with_capacity(rows);
    if field_type.is_hybrid() {
        keys.extend(
            (0..rows).map(|row| exact_int_key_chunk(chunk.get_row(row), offset, field_type)),
        );
        return keys;
    }
    column.with_raw(|raw| {
        if let Some(selection) = chunk.sel() {
            keys.extend(selection.iter().map(|&physical| {
                if column.is_null(physical) {
                    None
                } else if field_type.is_unsigned() {
                    Some(i128::from(u64::from_ne_bytes(
                        raw.row(physical)
                            .try_into()
                            .expect("non-hybrid integer key cells are eight bytes wide"),
                    )))
                } else {
                    Some(i128::from(i64::from_ne_bytes(
                        raw.row(physical)
                            .try_into()
                            .expect("non-hybrid integer key cells are eight bytes wide"),
                    )))
                }
            }));
        } else {
            keys.extend((0..rows).map(|row| {
                if column.is_null(row) {
                    None
                } else if field_type.is_unsigned() {
                    Some(i128::from(u64::from_ne_bytes(
                        raw.row(row)
                            .try_into()
                            .expect("non-hybrid integer key cells are eight bytes wide"),
                    )))
                } else {
                    Some(i128::from(i64::from_ne_bytes(
                        raw.row(row)
                            .try_into()
                            .expect("non-hybrid integer key cells are eight bytes wide"),
                    )))
                }
            }));
        }
    });
    keys
}

/// The integer a `KeyClass::Int` column holds at `offset`, or `None` for SQL
/// NULL and for a value outside the class.
///
/// Go `EncodeHashChunkRowIdx` dispatches the hash key on the column's MYSQL
/// TYPE, not on its eval type, and `mysql.TypeBit` has an arm of its own:
/// `BinaryLiteral(row.GetBytes(idx)).ToInt(ctx)`. That arm exists because a
/// HYBRID type -- Go `FieldType.Hybrid()`, so `BIT`/`ENUM`/`SET` -- COMPARES
/// as an integer while `getFixedLen` gives it a VARIABLE-length cell. The
/// fixed 8-byte `GetInt64` accessor therefore reads off the end of one: a
/// `bit(64)` holding `1` is four bytes wide here, and asking it for eight
/// panicked the whole statement. Go takes the same branch in
/// `Column.EvalInt` before it ever reaches `row.GetInt64`.
fn chunk_int_key(row: Row<'_>, offset: usize, field_type: &FieldType) -> Option<i128> {
    if row.is_null(offset) {
        return None;
    }
    if field_type.is_hybrid() {
        return datum_int_key(&row.get_datum(offset, field_type));
    }
    Some(if field_type.is_unsigned() {
        i128::from(row.get_uint64(offset))
    } else {
        i128::from(row.get_int64(offset))
    })
}

/// The same value read from an already-decoded datum.
///
/// Every hybrid arm is UNSIGNED in Go -- `BinaryLiteral.ToInt` returns
/// `uint64`, and `Enum`/`Set` carry a `uint64` ordinal/mask -- so widening to
/// `i128` places each one at its own exact position on the number line, which
/// is what makes `bit(64)` holding `-1` match `18446744073709551615` in a
/// `BIGINT UNSIGNED` and NOT match `-1` in a signed one.
///
/// A literal wider than eight significant bytes answers `u64::MAX`, which is
/// Go's value alongside the `ErrTruncatedWrongVal` it logs rather than raises.
fn datum_int_key(datum: &Datum) -> Option<i128> {
    Some(match datum {
        Datum::Int(value) => i128::from(*value),
        Datum::UInt(value) => i128::from(*value),
        Datum::Bit(literal) => i128::from(literal.to_int().value()),
        Datum::Enum(value, _) => i128::from(value.value()),
        Datum::Set(value, _) => i128::from(value.value()),
        _ => return None,
    })
}

/// Compares a materialized row on one side with a typed chunk row on the
/// other. Hash probing uses this before decoding a complete build row: Go's
/// `matchJoinKey` only needs the equality columns, while the old Rust path
/// decoded every projected column before doing the same check.
pub(crate) fn equi_keys_equal_row(
    keys: &[EquiKey],
    datums: &[Datum],
    datums_are_left: bool,
    row: Row<'_>,
    row_types: &[FieldType],
) -> Result<bool, KeyError> {
    for key in keys {
        let row_at = if datums_are_left { key.right } else { key.left };
        let datum_at = if datums_are_left { key.left } else { key.right };
        let datum = datums.get(datum_at).ok_or(KeyError)?;
        if row.is_null(row_at) {
            if key.null_safe && matches!(datum, Datum::Null) {
                continue;
            }
            return Ok(false);
        }
        if matches!(datum, Datum::Null) {
            return Ok(false);
        }
        let field_type = row_types.get(row_at).ok_or(KeyError)?;
        let equal = match key.class {
            KeyClass::Int => {
                chunk_int_key(row, row_at, field_type).ok_or(KeyError)?
                    == datum_int_key(datum).ok_or(KeyError)?
            }
            KeyClass::Real => {
                let value = match field_type.code() {
                    tidb_datatype::FieldTypeCode::Float => f64::from(row.get_float32(row_at)),
                    _ => row.get_float64(row_at),
                };
                let other = match datum {
                    Datum::Real(other) | Datum::Float32(other) => *other,
                    Datum::Int(other) => *other as f64,
                    Datum::UInt(other) => *other as f64,
                    _ => return Err(KeyError),
                };
                value == other
            }
            KeyClass::Decimal if is_decimal_cell(field_type) => {
                let row_key = row
                    .get_my_decimal(row_at)
                    .to_hash_key()
                    .map_err(|_| KeyError)?;
                match key_part(key.class, datum)? {
                    Some(datum_key) => row_key.as_slice() == datum_key.as_slice(),
                    None => false,
                }
            }
            KeyClass::Decimal | KeyClass::Str(_) => {
                let row_datum = row.get_datum(row_at, field_type);
                let (left_datum, right_datum) = if datums_are_left {
                    (datum, &row_datum)
                } else {
                    (&row_datum, datum)
                };
                // Compare the two EXTRACTED datums directly. `equi_keys_equal`
                // would re-index its slices by the KEY OFFSETS -- positions in
                // a complete child row -- and these single-datum views have no
                // such positions: indexing `from_ref(x)[key.left]` panicked
                // for every key past offset 0 (e.g. `m1.b = m2.b` over
                // `(a int, b varchar(32))`, where the varchar key sits at
                // offset 1 of each child).
                key_part(key.class, left_datum)? == key_part(key.class, right_datum)?
            }
        };
        if !equal {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Compares the equality keys of two typed chunk rows without materializing
/// either complete row. The hash join keeps both sides in chunks and calls
/// `matchJoinKey` on the key columns; doing the same here is
/// important for wide probe rows, where building a `Vec<Datum>` per row would
/// decode and allocate columns the equality check never reads.
pub(crate) fn equi_keys_equal_chunk_rows(
    keys: &[EquiKey],
    left: Row<'_>,
    left_types: &[FieldType],
    right: Row<'_>,
    right_types: &[FieldType],
) -> Result<bool, KeyError> {
    for key in keys {
        let left_type = left_types.get(key.left).ok_or(KeyError)?;
        let right_type = right_types.get(key.right).ok_or(KeyError)?;
        let left_null = left.is_null(key.left);
        let right_null = right.is_null(key.right);
        if left_null || right_null {
            if key.null_safe && left_null && right_null {
                continue;
            }
            return Ok(false);
        }
        let equal = match key.class {
            KeyClass::Int => {
                chunk_int_key(left, key.left, left_type).ok_or(KeyError)?
                    == chunk_int_key(right, key.right, right_type).ok_or(KeyError)?
            }
            KeyClass::Real => {
                let number = |row: Row<'_>, offset: usize, field_type: &FieldType| {
                    if field_type.code() == tidb_datatype::FieldTypeCode::Float {
                        f64::from(row.get_float32(offset))
                    } else {
                        row.get_float64(offset)
                    }
                };
                number(left, key.left, left_type) == number(right, key.right, right_type)
            }
            KeyClass::Decimal if is_decimal_cell(left_type) && is_decimal_cell(right_type) => {
                let left = left
                    .get_my_decimal(key.left)
                    .to_hash_key()
                    .map_err(|_| KeyError)?;
                let right = right
                    .get_my_decimal(key.right)
                    .to_hash_key()
                    .map_err(|_| KeyError)?;
                left == right
            }
            KeyClass::Decimal | KeyClass::Str(_) => {
                let left = left.get_datum(key.left, left_type);
                let right = right.get_datum(key.right, right_type);
                key_part(key.class, &left)? == key_part(key.class, &right)?
            }
        };
        if !equal {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The materialized build side: Go v1's `hashRowContainer` -- a
/// [`RowContainer`] holding the row DATA and, per key, the [`RowPtr`]s that
/// carry it IN BUILD ORDER.
///
/// The order is not incidental, and the two Go hash joins READ IT IN
/// OPPOSITE DIRECTIONS. v1's `Put` inserts each row at the HEAD of its
/// bucket's chain (`hash_table_v1.go:634` `newEntry.Next = oldEntry`), so
/// a probe row sees its matches NEWEST-FIRST -- and a cross join, which
/// only v1 executes (`CanUseHashJoinV2` refuses empty keys), shows exactly
/// that in `partition_pruner`'s recorded `... on true ... order by t1.id,
/// t1.a`: t2's rows 7 and 8 arrive 8 then 7 under the tied sort keys. The
/// KEYED equi-joins the corpus records ran v2, and their visible orders
/// read FORWARD: flipping these buckets to newest-first was measured at
/// 15 -> 21 corpus divergences (join_key_type_cast +3,
/// executor/jointest/join +4) and reverted. WHY v2 shows forward order is
/// NOT established -- its probe also walks a chain head-first
/// (`inner_join_probe.go:47-57`), so the reversal must be cancelled
/// upstream of the chain (build-side segment enumeration, partition
/// assembly, or the recorded server's join choice); until someone reads
/// that far, the recordings are the authority. Keyed buckets therefore
/// iterate forward; only the cross-join path (`next_nested`) reverses.
///
/// # Why the rows live in a container and the pointers do not
///
/// This split IS Go's spill mechanism. `hashRowContainer.hashTable` maps a
/// hash key to `chunk.RowPtr{ChkIdx, RowIdx}` and stays in memory for the
/// whole join; the `chunk.RowContainer` underneath holds the chunks and is
/// the only part that moves to disk. So a spill costs the join nothing in
/// lookup structure: `probe` still answers from memory, and only the
/// dereference of a pointer becomes a disk read. Spilling the hash table too
/// is what v2 does, with a completely different partitioned machinery.
pub(crate) struct BuildTable {
    rows: RowContainer,
    buckets: HashBuckets<ChainHead>,
    exact_int_buckets: Option<ExactIntTable>,
    /// The chain entries both maps point into (Go v1 `entryStore`).
    chains: ChainStore,
    /// Go v1's `outerMatchedStatus` / v2's row-table used flag. Present only
    /// when an outer join builds its preserved side, so the post-probe scan
    /// can emit build rows that never satisfied the complete ON condition.
    matched: Option<Mutex<Vec<Vec<u8>>>>,
    bucket_bytes: i64,
    /// Sum of the capacities of the per-chunk matched bitmaps.
    matched_bitmap_capacity: usize,
}

// Go v1 allocates 320 concurrent-map shards before the first build row. On
// the 64-bit target TiDB supports, `NewConcurrentMapHashTable` accounts:
//
// * 48 bytes for `concurrentMapHashTable`;
// * 320 * (56-byte shard + 184-byte empty Swiss map);
// * a 32-byte `entryStore` and its first 64 * 16-byte entries.
//
// These sizes are the current `unsafe.Sizeof`/`MemAwareMap.Bytes` values of
// `pkg/executor/join/hash_table_v1.go` and `concurrent_map.go`. The Rust map is
// not sharded, but query-quota behavior belongs to the transcreated Go
// package contract, so the fixed Go allocation is charged alongside the
// Rust map's retained dynamic allocation.
const GO_V1_HASH_TABLE_FIXED_BYTES: usize = 48 + 320 * (56 + 184) + 32 + 64 * 16;

impl BuildTable {
    /// An empty container over the build side's types, chunked at
    /// `chunk_size` -- Go `newHashRowContainer`, which passes
    /// `SessionVars.MaxChunkSize`.
    pub(crate) fn new(
        field_types: &[FieldType],
        chunk_size: usize,
        spill_storage: Arc<SpillStorage>,
        track_matches: bool,
        use_exact_int: bool,
    ) -> Self {
        BuildTable {
            rows: RowContainer::new(field_types, chunk_size, spill_storage),
            buckets: HashBuckets::default(),
            exact_int_buckets: use_exact_int.then(ExactIntTable::new),
            chains: ChainStore::default(),
            matched: track_matches.then(|| Mutex::new(Vec::<Vec<u8>>::new())),
            bucket_bytes: 0,
            matched_bitmap_capacity: 0,
        }
    }

    /// Go `hashRowContainer.PutChunkSelected` with no selection vector: index
    /// every row of `chunk` by its key, then hand the chunk to the container.
    ///
    /// The chunk index is read BEFORE the add, exactly as Go reads
    /// `chkIdx := uint32(c.rowContainer.NumChunks())` before `Add` -- the
    /// pointer must name the slot this chunk is about to take, and it stays
    /// valid across a spill because the spill copies the chunks to disk in
    /// order and later adds append to the same numbering.
    ///
    /// # Errors
    /// [`BuildError::Key`] from [`key_part`]; [`BuildError::Disk`] when the
    /// container is spilled and the write fails.
    pub(crate) fn index_chunk(
        &mut self,
        chunk: Chunk,
        keys: &[EquiKey],
        types: &[FieldType],
        build_is_left: bool,
    ) -> Result<(), BuildError> {
        self.index_chunk_selected(chunk, keys, types, build_is_left, &[])
    }

    /// Go `PutChunkSelected` keeps every build row for the preserved-side
    /// scan but indexes only rows accepted by the outer-side filter.
    pub(crate) fn index_chunk_selected(
        &mut self,
        chunk: Chunk,
        keys: &[EquiKey],
        types: &[FieldType],
        build_is_left: bool,
        selected: &[bool],
    ) -> Result<(), BuildError> {
        let offset = |key: &EquiKey| if build_is_left { key.left } else { key.right };
        let exact_int = self.exact_int_buckets.as_ref().and_then(|_| {
            keys.first()
                .filter(|key| keys.len() == 1 && key.class == KeyClass::Int && !key.null_safe)
        });
        let chk_idx = u32::try_from(self.rows.num_chunks()).map_err(|_| BuildError::Key)?;
        if let Some(matched) = &self.matched {
            let bitmap = vec![0; chunk.num_rows().div_ceil(8)];
            self.matched_bitmap_capacity = self
                .matched_bitmap_capacity
                .saturating_add(bitmap.capacity());
            matched
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(bitmap);
        }
        let batched_hashes = if exact_int.is_none() {
            row_hash_chunk_batched(&chunk, keys, types, |key| offset(key), selected)
                .map_err(|_| BuildError::Key)?
        } else {
            None
        };
        let exact_keys = exact_int.map(|key| {
            let at = offset(key);
            exact_int_keys_chunk(&chunk, at, &types[at])
        });
        let chains = &mut self.chains;
        let buckets = &mut self.buckets;
        let mut exact_buckets = self.exact_int_buckets.as_mut();
        for row_idx in 0..chunk.num_rows() {
            if !selected.is_empty() && !selected[row_idx] {
                continue;
            }
            // An ordinary equality key containing NULL is not indexed. A
            // NULL-safe key is indexed under row_key's dedicated NULL
            // identity, matching Go's `ignoreNulls[keyIdx]` path. Every row
            // is still stored because the container owns the build data.
            if exact_int.is_some() {
                if let Some(key) = exact_keys.as_ref().and_then(|keys| keys[row_idx]) {
                    let row_idx = u32::try_from(row_idx).map_err(|_| BuildError::Key)?;
                    let pointer = RowPtr { chk_idx, row_idx };
                    let table = exact_buckets
                        .as_deref_mut()
                        .expect("exact integer buckets initialized");
                    match table.find(key) {
                        Some(index) => {
                            let exact = &mut table.keys[index];
                            exact.head = chains.push(Some(exact.head), pointer)?;
                        }
                        None => {
                            let head = chains.push(None, pointer)?;
                            table.push(ExactKey {
                                key,
                                first_ptr: pointer,
                                head,
                            });
                        }
                    }
                }
                continue;
            }
            let key = match batched_hashes.as_ref() {
                Some(hashes) => hashes[row_idx],
                None => row_hash_chunk(keys, chunk.get_row(row_idx), types, |key| offset(key))
                    .map_err(|_| BuildError::Key)?,
            };
            if let Some(key) = key {
                let row_idx = u32::try_from(row_idx).map_err(|_| BuildError::Key)?;
                let pointer = RowPtr { chk_idx, row_idx };
                match buckets.entry(key) {
                    Entry::Occupied(mut entry) => {
                        *entry.get_mut() = chains.push(Some(*entry.get()), pointer)?;
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(chains.push(None, pointer)?);
                    }
                }
            }
        }
        self.rows.add(chunk).map_err(BuildError::disk)?;
        // Go adds the chunk to RowContainer before it charges the hash-table
        // delta. That order lets the registered spill action release build
        // rows before the non-spillable bucket memory is checked again.
        self.refresh_bucket_memory();
        Ok(())
    }

    /// The build rows that could match `key`, in build order.
    pub(crate) fn probe(&self, key: u64) -> Chain<'_> {
        self.chains.chain(self.buckets.get(&key))
    }

    pub(crate) fn probe_exact_int(&self, key: i128) -> Chain<'_> {
        self.chains.chain(
            self.exact_int_buckets
                .as_ref()
                .and_then(|table| table.find(key).map(|index| &table.keys[index].head)),
        )
    }

    /// [`Self::probe_exact_int`] for every key of a probe chunk at once
    /// (`ExactIntTable::find_many`); the caller reads each row's chain back
    /// with [`Self::exact_int_chain`]. Requires [`Self::has_exact_int`].
    pub(crate) fn probe_exact_int_many(
        &self,
        keys: &[Option<i128>],
        home_slots: &mut Vec<(u64, u64)>,
        found: &mut Vec<Option<usize>>,
    ) {
        self.exact_int_buckets
            .as_ref()
            .expect("exact integer buckets initialized")
            .find_many(keys, home_slots, found);
    }

    /// The one build row under the key at `index` in the exact-integer
    /// table, from the key entry alone: `None` when the key chains more than
    /// one row (`Chain::single` without the chain-entry read).
    pub(crate) fn exact_int_single_at(&self, index: usize) -> Option<RowPtr> {
        let exact = &self
            .exact_int_buckets
            .as_ref()
            .expect("exact integer buckets initialized")
            .keys[index];
        (exact.head.first == exact.head.last).then_some(exact.first_ptr)
    }

    /// The chain of the key at `index` in the exact-integer table (empty for
    /// `None`, a key the table does not hold).
    pub(crate) fn exact_int_chain(&self, index: Option<usize>) -> Chain<'_> {
        let table = self
            .exact_int_buckets
            .as_ref()
            .expect("exact integer buckets initialized");
        self.chains
            .chain(index.map(|index| &table.keys[index].head))
    }

    pub(crate) fn has_exact_int(&self) -> bool {
        self.exact_int_buckets.is_some()
    }

    /// Whether every exact-integer bucket names at most one build row.
    ///
    /// The bounded parallel probe path retains one result chunk per Go-shaped
    /// worker. Restricting that first parallel slice to a unique build key
    /// keeps each worker's output bounded by its input chunk while covering
    /// primary/unique-key dimension joins such as TPC-H q13.
    pub(crate) fn exact_int_is_unique(&self) -> bool {
        self.exact_int_buckets.as_ref().is_some_and(|table| {
            table
                .keys
                .iter()
                .all(|exact| exact.head.first == exact.head.last)
        })
    }

    /// Marks one preserved build row as matched after every ON conjunct has
    /// succeeded. A hash-key collision or a rejected residual condition must
    /// not set this bit.
    pub(crate) fn mark_matched(&self, ptr: RowPtr) {
        self.mark_matched_all(std::slice::from_ref(&ptr));
    }

    /// [`Self::mark_matched`] for every pointer a probe worker reported,
    /// under one lock.
    pub(crate) fn mark_matched_all(&self, ptrs: &[RowPtr]) {
        let Some(chunks) = self.matched.as_ref() else {
            return;
        };
        let mut chunks = chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for ptr in ptrs {
            let Some(chunk) = chunks.get_mut(ptr.chk_idx as usize) else {
                continue;
            };
            let row = ptr.row_idx as usize;
            chunk[row / 8] |= 1 << (row % 8);
        }
    }

    /// Whether a preserved build row has produced at least one joined row.
    pub(crate) fn is_matched(&self, ptr: RowPtr) -> bool {
        let Some(chunks) = self.matched.as_ref() else {
            return false;
        };
        let chunks = chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(chunk) = chunks.get(ptr.chk_idx as usize) else {
            return false;
        };
        let row = ptr.row_idx as usize;
        chunk[row / 8] & (1 << (row % 8)) != 0
    }

    /// First build row in input order, for Go's post-probe row-table scan.
    pub(crate) fn first_ptr(&self) -> Option<RowPtr> {
        (self.rows.num_chunks() != 0).then_some(RowPtr::new(0, 0))
    }

    /// Next build row in input order, including rows whose NULL key kept them
    /// out of every hash bucket.
    pub(crate) fn next_ptr(&self, ptr: RowPtr) -> Option<RowPtr> {
        let chunk_index = ptr.chk_idx as usize;
        let next_row = ptr.row_idx as usize + 1;
        if next_row < self.rows.num_rows_of_chunk(chunk_index) {
            return Some(RowPtr::new(ptr.chk_idx, next_row as u32));
        }
        let next_chunk = chunk_index + 1;
        (next_chunk < self.rows.num_chunks()).then_some(RowPtr::new(next_chunk as u32, 0))
    }

    /// Materializes the row `ptr` names, reading it back from the spill file
    /// when the container has spilled.
    ///
    /// Go `GetMatchedRowsAndPtrs` calls `GetRowAndAppendToChunkIfInDisk`:
    /// while the table is in memory the returned row remains a guarded live
    /// view and `buf` stays empty; after spill, `buf` is the reusable landing
    /// chunk for the decoded row. Datum materialization happens from either
    /// view without changing that ownership contract.
    ///
    /// # Errors
    /// [`DiskError`] when the spill file cannot be read.
    pub(crate) fn row(
        &self,
        ptr: RowPtr,
        buf: &mut Chunk,
        types: &[FieldType],
    ) -> Result<Vec<Datum>, DiskError> {
        buf.reset();
        let loaded = self.rows.get_row_and_append_to_chunk_if_in_disk(ptr, buf)?;
        Ok(loaded.row(buf).get_datum_row(types))
    }

    /// Visits a stored row without materializing columns that the caller does
    /// not need. In-memory rows remain shallow views; spilled rows use the
    /// same reusable buffer as [`Self::row`].
    pub(crate) fn with_row<T>(
        &self,
        ptr: RowPtr,
        buf: &mut Chunk,
        f: impl FnOnce(Row<'_>) -> T,
    ) -> Result<T, DiskError> {
        self.rows.with_row(ptr, buf, f)
    }

    /// Visits build rows in one lock-held batch, retaining separate disk and
    /// callback error channels for the join worker.
    pub(crate) fn with_rows<E>(
        &self,
        ptrs: &[RowPtr],
        buf: &mut Chunk,
        f: impl FnMut(Row<'_>) -> Result<(), E>,
    ) -> Result<Result<(), E>, DiskError> {
        self.rows.with_rows(ptrs, buf, f)
    }

    /// Visits one stored build chunk while retaining its in-memory read
    /// guard. Batch join output uses this to copy selected rows directly from
    /// the source columns; callers only use the fast path for an unselected,
    /// in-memory chunk and keep the row callback path for spilled data.
    pub(crate) fn with_chunk<T>(
        &self,
        chunk_index: usize,
        f: impl FnOnce(&Chunk) -> T,
    ) -> Result<T, DiskError> {
        let chunk = self.rows.get_chunk(chunk_index)?;
        Ok(f(&chunk))
    }

    /// Go `hashRowContainer.GetMemTracker`, which the build worker attaches
    /// to the join's own tracker under `LabelForBuildSideResult`.
    pub(crate) fn mem_tracker(&self) -> &Arc<Tracker> {
        self.rows.mem_tracker()
    }

    /// Go `hashRowContainer.GetDiskTracker`.
    pub(crate) fn disk_tracker(&self) -> &Arc<tidb_util::disk::Tracker> {
        self.rows.disk_tracker()
    }

    /// Go `hashRowContainer.ActionSpill`.
    pub(crate) fn action_spill(&mut self) -> Arc<SpillDiskAction> {
        self.rows.action_spill()
    }

    /// Whether the build side has moved to disk (Go
    /// `AlreadySpilledSafeForTest`).
    pub(crate) fn already_spilled(&self) -> bool {
        self.rows.already_spilled()
    }

    /// Go `hashRowContainer.Close`.
    pub(crate) fn close(&mut self) {
        self.rows.mem_tracker().consume(-self.bucket_bytes);
        self.bucket_bytes = 0;
        self.buckets = HashBuckets::default();
        self.exact_int_buckets = None;
        self.chains = ChainStore::default();
        self.matched = None;
        self.matched_bitmap_capacity = 0;
        self.rows.close();
    }

    fn refresh_bucket_memory(&mut self) {
        let map_slots = self
            .buckets
            .capacity()
            .saturating_mul(std::mem::size_of::<(u64, ChainHead)>() + std::mem::size_of::<usize>());
        let retained = GO_V1_HASH_TABLE_FIXED_BYTES
            .saturating_add(map_slots)
            .saturating_add(std::mem::size_of::<HashBuckets<ChainHead>>())
            .saturating_add(go_entry_store_bytes(self.chains.entries.len()))
            .saturating_add(
                self.exact_int_buckets
                    .as_ref()
                    .map_or(0, ExactIntTable::bytes),
            )
            .saturating_add(self.matched.as_ref().map_or(0, |chunks| {
                chunks
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Vec<u8>>())
                    + self.matched_bitmap_capacity
            }));
        let retained = i64::try_from(retained).unwrap_or(i64::MAX);
        self.rows
            .mem_tracker()
            .consume(retained.saturating_sub(self.bucket_bytes));
        self.bucket_bytes = retained;
    }
}

/// What indexing one build chunk can go wrong with.
#[derive(Debug)]
pub(crate) enum BuildError {
    /// A datum outside its key column's statically determined class.
    Key,
    /// The spill file rejected the write.
    Disk(String),
}

impl BuildError {
    fn disk(error: DiskError) -> Self {
        BuildError::Disk(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    /// The flat exact-integer table finds every key it was given (across
    /// its growth steps, with duplicates chained rather than re-added), and
    /// none it was not, over the whole `i128` key domain the join uses.
    #[test]
    fn exact_int_table_finds_its_keys_and_only_those() {
        let mut table = super::ExactIntTable::new();
        let mut chains = super::ChainStore::default();
        let keys: Vec<i128> = (0..5_000i128)
            .map(|i| i * 7 - 2_500)
            .chain([
                i128::from(u64::MAX),
                i128::from(i64::MIN),
                1 << 70,
                -(1 << 70),
            ])
            .collect();
        for (round, &key) in keys.iter().cycle().take(keys.len() * 2).enumerate() {
            let pointer = super::RowPtr {
                chk_idx: round as u32 / 1024,
                row_idx: round as u32 % 1024,
            };
            match table.find(key) {
                Some(index) => {
                    let head = table.keys[index].head;
                    table.keys[index].head = chains.push(Some(head), pointer).unwrap();
                }
                None => {
                    let head = chains.push(None, pointer).unwrap();
                    table.push(super::ExactKey {
                        key,
                        first_ptr: pointer,
                        head,
                    });
                }
            }
        }
        assert_eq!(table.keys.len(), keys.len());
        for (index, &key) in keys.iter().enumerate() {
            let found = table.find(key).expect("every inserted key is found");
            assert_eq!(table.keys[found].key, key);
            let head = table.keys[found].head;
            assert_eq!(
                chains.chain(Some(&head)).len(),
                2,
                "each key was added twice"
            );
            assert_eq!(
                chains.chain(Some(&head)).first(),
                Some(table.keys[found].first_ptr)
            );
            assert_eq!(table.keys[found].first_ptr.row_idx, index as u32 % 1024);
        }
        for missing in [1i128, 4, 3, 12_345, i128::MAX, i128::MIN, (1 << 70) + 1] {
            assert!(
                table.find(missing).is_none(),
                "{missing} was never inserted"
            );
        }
        assert!(
            table.slots.len() >= keys.len() * 4 / 3,
            "the table stays at most three quarters full"
        );
        // The batched probe answers exactly as the serial one, hits, misses
        // and NULL keys alike.
        let probe: Vec<Option<i128>> = keys
            .iter()
            .map(|&key| Some(key))
            .chain([None, Some(1), None, Some(12_345), Some(i128::MAX)])
            .chain((0..3_000i128).map(|i| Some(i * 5 - 7_000)))
            .collect();
        let mut home_slots = Vec::new();
        let mut found = Vec::new();
        table.find_many(&probe, &mut home_slots, &mut found);
        assert_eq!(found.len(), probe.len());
        for (key, index) in probe.iter().zip(&found) {
            assert_eq!(*index, key.and_then(|key| table.find(key)), "key {key:?}");
        }
    }

    use super::*;
    use tidb_datatype::{Decimal, FieldTypeCode};

    fn int_key(datum: &Datum) -> Option<Vec<u8>> {
        key_part(KeyClass::Int, datum).unwrap()
    }

    #[test]
    fn fast_hasher_matches_go_hash_fnv_new64() {
        let mut hasher = FastBytesHasher::default();
        hasher.write(b"hello");
        assert_eq!(hasher.finish(), 0x7b49_5389_bdbd_d4c7);
    }

    /// The signed/unsigned boundary the nested loop's `join_key_eq` calls
    /// out: neither `-1` nor `2^63` may be reinterpreted into the other's
    /// domain, so they must not share a bucket with anything on the far side.
    #[test]
    fn int_keys_do_not_reinterpret_across_signedness() {
        assert_ne!(int_key(&Datum::Int(-1)), int_key(&Datum::UInt(u64::MAX)));
        assert_ne!(
            int_key(&Datum::Int(i64::MIN)),
            int_key(&Datum::UInt(1 << 63))
        );
        // The same VALUE in either domain still collides, as it must.
        assert_eq!(int_key(&Datum::Int(7)), int_key(&Datum::UInt(7)));
    }

    /// A `NULL` key never matches -- not even another `NULL`.
    #[test]
    fn null_produces_no_key() {
        assert!(int_key(&Datum::Null).is_none());
        assert!(key_part(KeyClass::Real, &Datum::Null).unwrap().is_none());
        assert!(key_part(KeyClass::Decimal, &Datum::Null).unwrap().is_none());
    }

    #[test]
    fn null_safe_row_key_gives_null_one_matchable_identity() {
        let mut key = EquiKey {
            left: 0,
            right: 0,
            class: KeyClass::Int,
            null_safe: true,
        };
        let null = row_key(&[key], &[Datum::Null], |key| key.left)
            .unwrap()
            .expect("NULL is a key under <=>");
        assert_eq!(
            null,
            row_key(&[key], &[Datum::Null], |key| key.left)
                .unwrap()
                .unwrap()
        );
        assert_ne!(
            null,
            row_key(&[key], &[Datum::Int(0)], |key| key.left)
                .unwrap()
                .unwrap()
        );

        key.null_safe = false;
        assert!(row_key(&[key], &[Datum::Null], |key| key.left)
            .unwrap()
            .is_none());
    }

    #[test]
    fn lazy_row_key_accessor_encodes_only_requested_key_columns() {
        let keys = [
            EquiKey {
                left: 1,
                right: 1,
                class: KeyClass::Int,
                null_safe: false,
            },
            EquiKey {
                left: 3,
                right: 3,
                class: KeyClass::Int,
                null_safe: false,
            },
        ];
        let mut calls = Vec::new();
        let encoded = row_key_by(&keys, |key| {
            calls.push(key.left);
            Datum::Int(if key.left == 1 { 7 } else { 11 })
        })
        .unwrap()
        .expect("non-null key parts produce a key");
        assert_eq!(calls, vec![1, 3]);
        assert_eq!(
            encoded,
            row_key(
                &keys,
                &[Datum::Null, Datum::Int(7), Datum::Null, Datum::Int(11)],
                |key| key.left,
            )
            .unwrap()
            .unwrap()
        );
    }

    #[test]
    fn typed_chunk_key_equality_matches_materialized_key_equality() {
        let types = vec![FieldType::new(FieldTypeCode::Long)];
        let mut chunk = Chunk::new_with_capacity(&types, 1);
        chunk.append_int64(0, 7);
        let key = EquiKey {
            left: 0,
            right: 0,
            class: KeyClass::Int,
            null_safe: false,
        };
        let row = chunk.get_row(0);
        assert!(equi_keys_equal_row(&[key], &[Datum::Int(7)], true, row, &types,).unwrap());
        assert!(!equi_keys_equal_row(&[key], &[Datum::Int(8)], true, row, &types,).unwrap());
    }

    /// A DECIMAL chunk cell hashes and compares through the word-native
    /// `MyDecimal::to_hash_key`; it must agree with the datum encoding of the
    /// same value at any written scale, on the hash, the chunk-to-chunk
    /// recheck, and the datum-to-chunk recheck.
    #[test]
    fn decimal_chunk_cells_hash_and_compare_like_their_datums() {
        let key = EquiKey {
            left: 0,
            right: 0,
            class: KeyClass::Decimal,
            null_safe: false,
        };
        let cells = [
            "1.50",
            "1.5000",
            "-1.50",
            "0.00",
            "-0.0",
            "1234567890.1234567890",
            "1.51",
        ];
        let types = vec![FieldType::new(FieldTypeCode::NewDecimal)];
        let mut chunk = Chunk::new_with_capacity(&types, cells.len());
        for cell in cells {
            let value = tidb_datatype::MyDecimal::from_string(cell.as_bytes()).0;
            chunk.append_my_decimal(0, &value);
        }
        for (left_at, left_text) in cells.iter().enumerate() {
            let left_datum = Datum::Decimal(Decimal::from_signed_literal(left_text));
            assert_eq!(
                row_hash_chunk(&[key], chunk.get_row(left_at), &types, |key| key.left).unwrap(),
                row_hash(&[key], std::slice::from_ref(&left_datum), |key| key.left).unwrap(),
                "{left_text}: chunk hash differs from the datum hash"
            );
            for (right_at, right_text) in cells.iter().enumerate() {
                let right_datum = Datum::Decimal(Decimal::from_signed_literal(right_text));
                let expected = matches!(
                    (&left_datum, &right_datum),
                    (Datum::Decimal(l), Datum::Decimal(r)) if l == r
                );
                assert_eq!(
                    equi_keys_equal_chunk_rows(
                        &[key],
                        chunk.get_row(left_at),
                        &types,
                        chunk.get_row(right_at),
                        &types,
                    )
                    .unwrap(),
                    expected,
                    "{left_text} vs {right_text}: chunk rows"
                );
                assert_eq!(
                    equi_keys_equal_row(
                        &[key],
                        std::slice::from_ref(&left_datum),
                        true,
                        chunk.get_row(right_at),
                        &types,
                    )
                    .unwrap(),
                    expected,
                    "{left_text} vs {right_text}: datum against chunk row"
                );
            }
        }
    }

    #[test]
    fn direct_row_hash_matches_hashing_the_complete_encoding() {
        let keys = [
            EquiKey {
                left: 0,
                right: 0,
                class: KeyClass::Int,
                null_safe: false,
            },
            EquiKey {
                left: 1,
                right: 1,
                class: KeyClass::Str(Collation::Utf8Mb4Bin),
                null_safe: false,
            },
        ];
        let row = [Datum::Int(42), Datum::new_string("key")];
        let encoded = row_key(&keys, &row, |key| key.left).unwrap().unwrap();
        let mut expected = FastBytesHasher::default();
        expected.write(&encoded);
        assert_eq!(
            row_hash(&keys, &row, |key| key.left).unwrap(),
            Some(expected.finish())
        );
    }

    #[test]
    fn bucket_hits_recheck_complete_equality_keys() {
        let key = EquiKey {
            left: 0,
            right: 0,
            class: KeyClass::Int,
            null_safe: false,
        };
        let types = vec![FieldType::new(FieldTypeCode::LongLong).with_unsigned(true)];
        let mut chunk = Chunk::new_with_capacity(&types, 2);
        chunk.append_uint64(0, 7);
        chunk.append_uint64(0, u64::MAX);
        assert!(
            equi_keys_equal_row(&[key], &[Datum::Int(7)], true, chunk.get_row(0), &types,).unwrap()
        );
        assert!(
            !equi_keys_equal_row(&[key], &[Datum::Int(8)], true, chunk.get_row(0), &types,)
                .unwrap()
        );
        assert!(
            !equi_keys_equal_row(&[key], &[Datum::Int(-1)], true, chunk.get_row(1), &types,)
                .unwrap()
        );
    }

    /// `NaN = NaN` is FALSE, and `-0.0 = 0.0` is TRUE.
    #[test]
    fn real_key_follows_float_equality() {
        assert!(key_part(KeyClass::Real, &Datum::Real(f64::NAN))
            .unwrap()
            .is_none());
        assert_eq!(
            key_part(KeyClass::Real, &Datum::Real(-0.0)).unwrap(),
            key_part(KeyClass::Real, &Datum::Real(0.0)).unwrap()
        );
        // An integral datum in a float key column promotes, as the
        // comparison itself does.
        assert_eq!(
            key_part(KeyClass::Real, &Datum::Int(3)).unwrap(),
            key_part(KeyClass::Real, &Datum::Real(3.0)).unwrap()
        );
    }

    /// The property the whole module rests on for decimals: equal values
    /// hash equal, regardless of the scale they were written with.
    #[test]
    fn decimal_key_agrees_with_decimal_equality() {
        let values = [
            "1",
            "1.0",
            "1.00",
            "0",
            "-0",
            "0.000",
            "-1.5",
            "-1.500",
            "12345678901234567890",
            "0.1",
            "0.10",
        ];
        for a in values {
            for b in values {
                let da = Decimal::from_signed_literal(a);
                let db = Decimal::from_signed_literal(b);
                let ka = key_part(KeyClass::Decimal, &Datum::Decimal(da.clone())).unwrap();
                let kb = key_part(KeyClass::Decimal, &Datum::Decimal(db.clone())).unwrap();
                assert_eq!(
                    da == db,
                    ka == kb,
                    "{a} vs {b}: equality and hash key disagree"
                );
            }
        }
        // An integral datum in a decimal key column promotes to scale 0.
        assert_eq!(
            key_part(KeyClass::Decimal, &Datum::Int(1)).unwrap(),
            key_part(
                KeyClass::Decimal,
                &Datum::Decimal(Decimal::from_signed_literal("1.000"))
            )
            .unwrap()
        );
    }

    /// A multi-column key must not let one column borrow the next one's
    /// bytes.
    #[test]
    fn multi_column_keys_are_unambiguous() {
        let keys = [
            EquiKey {
                left: 0,
                right: 0,
                class: KeyClass::Str(Collation::Utf8Mb4Bin),
                null_safe: false,
            },
            EquiKey {
                left: 1,
                right: 1,
                class: KeyClass::Str(Collation::Utf8Mb4Bin),
                null_safe: false,
            },
        ];
        let offset = |key: &EquiKey| key.left;
        let ab_c = row_key(
            &keys,
            &[Datum::new_string("ab"), Datum::new_string("c")],
            offset,
        )
        .unwrap();
        let a_bc = row_key(
            &keys,
            &[Datum::new_string("a"), Datum::new_string("bc")],
            offset,
        )
        .unwrap();
        assert_ne!(ab_c, a_bc);
    }
}
