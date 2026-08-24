// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Utilities for removing keys from logs and key errors.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::kv::HexRepr;
use crate::ProtoKeyError;

const REDACT_MARKER: &[u8] = b"?";
static REDACT_LOG_ENABLED: AtomicBool = AtomicBool::new(false);

/// Returns whether log redaction is enabled.
pub fn need_redact() -> bool {
    REDACT_LOG_ENABLED.load(Ordering::Relaxed)
}

/// Enables or disables global log redaction.
pub fn set_redact_log_enabled(enabled: bool) {
    REDACT_LOG_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Formats a key as uppercase hexadecimal, or as `?` when redaction is enabled.
pub fn key(key: &[u8]) -> String {
    if need_redact() {
        "?".to_owned()
    } else {
        format!("{}", HexRepr(key))
    }
}

/// Returns [`key`] as bytes.
pub fn key_bytes(data: &[u8]) -> Vec<u8> {
    key(data).into_bytes()
}

/// Redacts every key field covered by client-go's `RedactKeyErrIfNecessary`.
pub fn redact_key_error_if_necessary(error: &mut ProtoKeyError) {
    if !need_redact() {
        return;
    }

    if let Some(locked) = &mut error.locked {
        redact_lock_info(locked);
    }
    if let Some(conflict) = &mut error.conflict {
        redact_nonempty(&mut conflict.key);
        redact_nonempty(&mut conflict.primary);
    }
    if let Some(already_exist) = &mut error.already_exist {
        redact_nonempty(&mut already_exist.key);
    }
    if let Some(deadlock) = &mut error.deadlock {
        redact_nonempty(&mut deadlock.lock_key);
        redact_nonempty(&mut deadlock.deadlock_key);
        for entry in &mut deadlock.wait_chain {
            redact_nonempty(&mut entry.key);
        }
    }
    if let Some(expired) = &mut error.commit_ts_expired {
        redact_nonempty(&mut expired.key);
    }
    if let Some(not_found) = &mut error.txn_not_found {
        redact_nonempty(&mut not_found.primary_key);
    }
    if let Some(assertion_failed) = &mut error.assertion_failed {
        redact_nonempty(&mut assertion_failed.key);
    }
    if let Some(lock_info) = error
        .primary_mismatch
        .as_mut()
        .and_then(|mismatch| mismatch.lock_info.as_mut())
    {
        redact_lock_info(lock_info);
    }
}

fn redact_lock_info(lock_info: &mut crate::proto::kvrpcpb::LockInfo) {
    redact_nonempty(&mut lock_info.primary_lock);
    redact_nonempty(&mut lock_info.key);
    for secondary in &mut lock_info.secondaries {
        *secondary = REDACT_MARKER.to_vec();
    }
}

fn redact_nonempty(key: &mut Vec<u8>) {
    if !key.is_empty() {
        *key = REDACT_MARKER.to_vec();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::deadlock::WaitForEntry;
    use crate::proto::kvrpcpb;
    use serial_test::serial;

    struct DisableRedaction;

    impl Drop for DisableRedaction {
        fn drop(&mut self) {
            set_redact_log_enabled(false);
        }
    }

    #[test]
    #[serial]
    fn key_rendering_follows_the_global_mode() {
        set_redact_log_enabled(false);
        assert_eq!(key(&[0xab, 0xcd, 1]), "ABCD01");
        assert_eq!(key_bytes(&[0xab, 0xcd, 1]), b"ABCD01");

        set_redact_log_enabled(true);
        let _reset = DisableRedaction;
        assert_eq!(key(b"secret"), "?");
        assert_eq!(key_bytes(b"secret"), b"?");
    }

    #[test]
    #[serial]
    fn key_error_redaction_covers_every_source_field() {
        set_redact_log_enabled(true);
        let _reset = DisableRedaction;

        let lock = || kvrpcpb::LockInfo {
            primary_lock: b"primary".to_vec(),
            key: b"key".to_vec(),
            secondaries: vec![b"secondary".to_vec(), Vec::new()],
            ..Default::default()
        };
        let mut error = ProtoKeyError {
            locked: Some(lock()),
            conflict: Some(kvrpcpb::WriteConflict {
                key: b"conflict".to_vec(),
                primary: b"conflict-primary".to_vec(),
                ..Default::default()
            }),
            already_exist: Some(kvrpcpb::AlreadyExist {
                key: b"existing".to_vec(),
            }),
            deadlock: Some(kvrpcpb::Deadlock {
                lock_key: b"lock".to_vec(),
                deadlock_key: b"deadlock".to_vec(),
                wait_chain: vec![WaitForEntry {
                    key: b"wait".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            commit_ts_expired: Some(kvrpcpb::CommitTsExpired {
                key: b"expired".to_vec(),
                ..Default::default()
            }),
            txn_not_found: Some(kvrpcpb::TxnNotFound {
                primary_key: b"missing-primary".to_vec(),
                ..Default::default()
            }),
            assertion_failed: Some(kvrpcpb::AssertionFailed {
                key: b"assertion".to_vec(),
                ..Default::default()
            }),
            primary_mismatch: Some(kvrpcpb::PrimaryMismatch {
                lock_info: Some(lock()),
            }),
            ..Default::default()
        };

        redact_key_error_if_necessary(&mut error);
        let assert_lock = |lock: &kvrpcpb::LockInfo| {
            assert_eq!(lock.primary_lock, b"?");
            assert_eq!(lock.key, b"?");
            assert_eq!(lock.secondaries, [b"?".to_vec(), b"?".to_vec()]);
        };
        assert_lock(error.locked.as_ref().unwrap());
        assert_eq!(error.conflict.as_ref().unwrap().key, b"?");
        assert_eq!(error.conflict.as_ref().unwrap().primary, b"?");
        assert_eq!(error.already_exist.as_ref().unwrap().key, b"?");
        assert_eq!(error.deadlock.as_ref().unwrap().lock_key, b"?");
        assert_eq!(error.deadlock.as_ref().unwrap().deadlock_key, b"?");
        assert_eq!(error.deadlock.as_ref().unwrap().wait_chain[0].key, b"?");
        assert_eq!(error.commit_ts_expired.as_ref().unwrap().key, b"?");
        assert_eq!(error.txn_not_found.as_ref().unwrap().primary_key, b"?");
        assert_eq!(error.assertion_failed.as_ref().unwrap().key, b"?");
        assert_lock(
            error
                .primary_mismatch
                .as_ref()
                .unwrap()
                .lock_info
                .as_ref()
                .unwrap(),
        );
    }

    #[test]
    #[serial]
    fn disabled_redaction_leaves_key_errors_unchanged() {
        set_redact_log_enabled(false);
        let mut error = ProtoKeyError {
            already_exist: Some(kvrpcpb::AlreadyExist {
                key: b"visible".to_vec(),
            }),
            ..Default::default()
        };
        let original = error.clone();
        redact_key_error_if_necessary(&mut error);
        assert_eq!(error, original);
    }
}
