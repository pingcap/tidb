// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use lazy_static::lazy_static;

use crate::Result;

pub const DEF_BACKOFF_LOCK_FAST: i32 = 10;
pub const DEF_BACKOFF_WEIGHT: i32 = 2;

/// Handles kill signals at interruptible request checkpoints.
pub trait KillSignalHandler: Send + Sync {
    fn handle_signal(&self) -> Result<()>;
}

/// Variables shared by KV request and retry code.
#[derive(Clone)]
pub struct Variables {
    pub backoff_lock_fast: i32,
    pub backoff_weight: i32,
    pub killed: Arc<AtomicU32>,
    pub disable_txn_file: bool,
    pub txn_file_min_mutation_size: u64,
    pub kill_signal_handler: Option<Arc<dyn KillSignalHandler>>,
}

impl Variables {
    pub fn new(killed: Arc<AtomicU32>) -> Self {
        Self {
            backoff_lock_fast: DEF_BACKOFF_LOCK_FAST,
            backoff_weight: DEF_BACKOFF_WEIGHT,
            killed,
            disable_txn_file: false,
            txn_file_min_mutation_size: 0,
            kill_signal_handler: None,
        }
    }
}

impl Default for Variables {
    fn default() -> Self {
        Self::new(Arc::new(AtomicU32::new(0)))
    }
}

lazy_static! {
    pub static ref DEFAULT_VARIABLES: Arc<Variables> = Arc::new(Variables::default());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn variables_have_source_defaults_and_share_the_kill_signal() {
        let killed = Arc::new(AtomicU32::new(0));
        let variables = Variables::new(killed.clone());
        assert_eq!(variables.backoff_lock_fast, 10);
        assert_eq!(variables.backoff_weight, 2);
        assert!(!variables.disable_txn_file);
        assert_eq!(variables.txn_file_min_mutation_size, 0);
        killed.store(7, Ordering::SeqCst);
        assert_eq!(variables.killed.load(Ordering::SeqCst), 7);
    }
}
