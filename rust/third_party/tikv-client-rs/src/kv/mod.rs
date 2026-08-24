// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::fmt;

mod bound_range;
pub mod codec;
mod key;
mod key_flags;
mod kvpair;
mod store_vars;
mod types;
mod value;
mod variables;

pub use bound_range::BoundRange;
pub use bound_range::IntoOwnedRange;
pub use key::Key;
pub use key::KeyRange;
pub use key::KvPairTTL;
pub use key::{cmp_key, next_key, prefix_next_key};
pub use key_flags::{apply_flags_ops, FlagsOp, KeyFlags, FLAG_BYTES};
pub use kvpair::KvPair;
pub use store_vars::{
    AccessLocationType, ReplicaReadAdjuster, ReplicaReadAdjustment, ReplicaReadConfig,
    ReplicaReadSelectorOption, ReplicaReadType, DEF_TXN_COMMIT_BATCH_SIZE, STORE_LIMIT,
    TXN_COMMIT_BATCH_SIZE,
};
pub use types::{
    batch_get_to_get_options, BatchGetOptions, BatchGetter, DeadlockError, DeadlockHandler,
    GetOption, GetOptions, Getter, LockContext, LockStatistics, ResourceGroupTagger, ReturnedValue,
    ValueEntry, LOCK_ALWAYS_WAIT, LOCK_NO_WAIT,
};
pub use value::Value;
pub use variables::{
    KillSignalHandler, Variables, DEFAULT_VARIABLES, DEF_BACKOFF_LOCK_FAST, DEF_BACKOFF_WEIGHT,
};

pub struct HexRepr<'a>(pub &'a [u8]);

impl fmt::Display for HexRepr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02X}")?;
        }
        Ok(())
    }
}
