//! Go `session.go` and `context.go`: the transaction and contexts the table
//! layer writes a row under. The transaction stores nothing; its memory
//! buffer keeps every KV pair `AddRecord` sets, in order, for
//! `TakeKvPairs`.

use std::sync::{Arc, Mutex, PoisonError};

use tidb_datatype::{Datum, SessionTimeZone};
use tidb_kv::storage::{MemTableStorage, StorageError, StorageIterator, TableStorage};
use tidb_lightning_backend_encode::SessionOptions;
use tidb_lightning_verification::KvPair;
use tidb_tblctx::{ImportSessionVars, RowEncodingConfig, StaticMutateContext};
use tidb_txnkv::{AssertionOp, Key};

/// Go `MemBuf`: the KV pairs one `AddRecord` set.
#[derive(Clone, Debug, Default)]
pub struct MemBuf {
    pairs: Arc<Mutex<Vec<KvPair>>>,
}

impl MemBuf {
    fn push(&self, key: Key, value: Vec<u8>) {
        self.pairs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(KvPair {
                key: key.as_bytes().to_vec(),
                val: value,
                row_id: Vec::new(),
            });
    }

    fn take(&self) -> Vec<KvPair> {
        std::mem::take(&mut *self.pairs.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

/// Go `transaction` with its `MemBuf`: reads find nothing (Lightning
/// writes with `DupKeyCheckSkip`), deletes are refused, and every set is
/// kept.
impl TableStorage for MemBuf {
    fn get(&mut self, _key: &Key) -> Result<Vec<u8>, StorageError> {
        Err(StorageError::NotFound)
    }

    fn get_local(&mut self, _key: &Key) -> Result<Vec<u8>, StorageError> {
        Err(StorageError::NotFound)
    }

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), StorageError> {
        self.push(key, value);
        Ok(())
    }

    fn set_with_assertion(
        &mut self,
        key: Key,
        value: Vec<u8>,
        _assertion: AssertionOp,
    ) -> Result<(), StorageError> {
        self.push(key, value);
        Ok(())
    }

    fn set_with_constraint_check(
        &mut self,
        key: Key,
        value: Vec<u8>,
        _assertion: AssertionOp,
        _check_in_prewrite: bool,
    ) -> Result<(), StorageError> {
        self.push(key, value);
        Ok(())
    }

    /// Go `MemBuf.Delete`: "unsupported operation".
    fn delete(&mut self, _key: Key) -> Result<(), StorageError> {
        Err(StorageError::Backend("unsupported operation".into()))
    }

    fn delete_with_assertion(&mut self, _key: Key, _assertion: AssertionOp) -> Result<(), StorageError> {
        Err(StorageError::Backend("unsupported operation".into()))
    }

    /// Go `transaction.Iter`: an iterator that is never valid.
    fn iter(
        &mut self,
        start: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Box<dyn StorageIterator>, StorageError> {
        MemTableStorage::new().iter(start, upper_bound)
    }

    fn key_count(&self) -> usize {
        self.pairs.lock().unwrap_or_else(PoisonError::into_inner).len()
    }

    fn clear(&mut self) {
        self.take();
    }

    fn clone_box(&self) -> Box<dyn TableStorage> {
        Box::new(self.clone())
    }
}

/// Go `Session`: Lightning's expression and table contexts and its
/// transaction.
pub struct Session {
    pub(crate) ctx: StaticMutateContext,
    pub(crate) mem_buf: MemBuf,
}

/// Go `NewSession`'s system-variable handling: a variable Go cannot set
/// at session scope (unknown, read-only, global-only, malformed) is skipped,
/// as Go logs and skips it. Of what the contexts then load, the encoder
/// reads the location, the row format, `div_precision_increment` and
/// `default_week_format`; the row checksum follows the global
/// `tidb_enable_row_level_checksum` (`NewSessionVars`) and needs row format
/// v2 (`IsRowLevelChecksumEnabled`).
fn session_vars(options: &SessionOptions) -> ImportSessionVars {
    let mut zone = session_zone(tidb_timeutil::system_location());
    // `NewSessionVars` leaves `RowEncoder.Enable` off.
    let mut row_format_v2 = false;
    let mut div_precision_increment = 4;
    let mut default_week_format = 0;
    for (name, value) in &options.sys_vars {
        match name.as_str() {
            "time_zone" => {
                if let Ok(parsed) = tidb_timeutil::parse_time_zone(value) {
                    zone = session_zone(parsed);
                }
            }
            // TypeUnsigned 1..=2, clamped.
            "tidb_row_format_version" => {
                if let Ok(version) = value.trim().parse::<u64>() {
                    row_format_v2 = version >= 2;
                }
            }
            // TypeUnsigned 0..=30, clamped.
            "div_precision_increment" => {
                if let Ok(v) = value.trim().parse::<u64>() {
                    div_precision_increment = u32::try_from(v.min(30)).unwrap_or(30);
                }
            }
            // TypeUnsigned 0..=7, clamped.
            "default_week_format" => {
                if let Ok(v) = value.trim().parse::<u64>() {
                    default_week_format = i64::try_from(v.min(7)).unwrap_or(7);
                }
            }
            _ => {}
        }
    }
    ImportSessionVars {
        zone,
        row_encoding: RowEncodingConfig {
            is_row_level_checksum_enabled: row_format_v2
                && tidb_vardef::ENABLE_ROW_LEVEL_CHECKSUM.load(std::sync::atomic::Ordering::SeqCst),
            row_format_v2,
        },
        div_precision_increment,
        default_week_format,
    }
}

/// Go's `*time.Location` as the zone the table layer reads.
fn session_zone(zone: tidb_timeutil::TimeZone) -> SessionTimeZone {
    match zone {
        tidb_timeutil::TimeZone::Local => SessionTimeZone::Local,
        tidb_timeutil::TimeZone::Named(zone) => SessionTimeZone::Named(zone),
        tidb_timeutil::TimeZone::Fixed { name, offset_secs } => {
            SessionTimeZone::Fixed { name, offset_secs }
        }
    }
}

impl Session {
    /// Go `NewSession`.
    #[must_use]
    pub fn new(options: &SessionOptions) -> Self {
        let vars = session_vars(options);
        Self {
            ctx: StaticMutateContext::for_import_into(options.sql_mode, &vars, options.timestamp),
            mem_buf: MemBuf::default(),
        }
    }

    /// Go `Session.GetExprCtx` / `GetTableCtx`.
    #[must_use]
    pub fn context(&self) -> &StaticMutateContext {
        &self.ctx
    }

    /// Go `Session.TakeKvPairs`.
    pub fn take_kv_pairs(&self) -> Vec<KvPair> {
        self.mem_buf.take()
    }

    /// Go `Session.SetUserVarVal`.
    pub fn set_user_var_val(&self, name: &str, value: Datum) {
        self.ctx.set_user_var_val(name, value);
    }

    /// Go `Session.UnsetUserVar`.
    pub fn unset_user_var(&self, name: &str) {
        self.ctx.unset_user_var(name);
    }
}
