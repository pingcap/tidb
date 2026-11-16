//! Go `pkg/lightning/common/errors.go`, the definitions the encoder raises.

use std::sync::LazyLock;
use tidb_terror::{TerrorCode, TerrorError};

fn normalize(message: &str, rfc_code: &str) -> TerrorError {
    TerrorError::normalize(message, rfc_code, TerrorCode::new(0))
}

/// Go `ErrEncodeKV`.
pub static ERR_ENCODE_KV: LazyLock<TerrorError> = LazyLock::new(|| {
    normalize("encode kv error in file %s at offset %d", "Lightning:Restore:ErrEncodeKV")
});

/// Go `ErrCastValue`. Go marks the received value (argument 2) for log
/// redaction only; the error text keeps it.
pub static ERR_CAST_VALUE: LazyLock<TerrorError> = LazyLock::new(|| {
    normalize(
        "Value conversion failed for column '%s'. Expected type: %s, received value: %s. Reason: %s.",
        "Import:ErrCastValue",
    )
});

/// Go `ErrChecksumMismatch`. Go's RFC code is spelled `Lighting`.
pub static ERR_CHECKSUM_MISMATCH: LazyLock<TerrorError> = LazyLock::new(|| {
    normalize(
        "checksum mismatched remote vs local => (checksum: %d vs %d) (total_kvs: %d vs %d) (total_bytes:%d vs %d)",
        "Lighting:Restore:ErrChecksumMismatch",
    )
});
