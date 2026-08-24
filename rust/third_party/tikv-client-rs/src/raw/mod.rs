// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! Raw related functionality.
//!
//! Using the [`raw::Client`](client::Client) you can utilize TiKV's raw interface.
//!
//! This interface offers optimal performance as it does not require coordination with a timestamp
//! oracle, while the transactional interface does.
//!
//! **Warning:** It is not advisable to use both raw and transactional functionality in the same keyspace.

use std::convert::TryFrom;
use std::fmt;

pub use self::client::Client;
use crate::Error;

mod client;
pub mod lowering;
mod requests;

/// Aggregate checksum for the key/value pairs in a raw-key range.
///
/// `crc64_xor` is the XOR of the per-pair CRC64 values. `total_bytes` includes
/// any API V2 prefix bytes, matching TiKV's server-side accounting.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RawChecksum {
    pub crc64_xor: u64,
    pub total_kvs: u64,
    pub total_bytes: u64,
}

/// A [`ColumnFamily`](ColumnFamily) is an optional parameter for [`raw::Client`](Client) requests.
///
/// TiKV uses RocksDB's `ColumnFamily` support. You can learn more about RocksDB's `ColumnFamily`s [on their wiki](https://github.com/facebook/rocksdb/wiki/Column-Families).
///
/// By default in TiKV data is stored in three different `ColumnFamily` values, configurable in the TiKV server's configuration:
///
/// * Default: Where real user data is stored. Set by `[rocksdb.defaultcf]`.
/// * Write: Where MVCC and index related data are stored. Set by `[rocksdb.writecf]`.
/// * Lock: Where lock information is stored. Set by `[rocksdb.lockcf]`.
///
/// Not providing a call a `ColumnFamily` means it will use the default value of `default`.
///
/// Built-in families preserve their typed variants; other names are retained and
/// passed through unchanged, matching client-go's raw option behavior.
///
/// # Examples
/// ```rust
/// # use tikv_client::ColumnFamily;
/// # use std::convert::TryFrom;
///
/// let cf = ColumnFamily::try_from("write").unwrap();
/// let cf = ColumnFamily::try_from(String::from("write")).unwrap();
/// ```
///
/// **But, you should not need to worry about all this:** Many functions which accept a
/// `ColumnFamily` accept an `Into<ColumnFamily>`, which means all of the above types can be passed
/// directly to those functions.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum ColumnFamily {
    Default,
    Lock,
    Write,
    Custom(String),
}

impl TryFrom<&str> for ColumnFamily {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "default" => Ok(ColumnFamily::Default),
            "lock" => Ok(ColumnFamily::Lock),
            "write" => Ok(ColumnFamily::Write),
            custom => Ok(ColumnFamily::Custom(custom.to_owned())),
        }
    }
}

impl TryFrom<String> for ColumnFamily {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        TryFrom::try_from(&*value)
    }
}

impl fmt::Display for ColumnFamily {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ColumnFamily::Default => f.write_str("default"),
            ColumnFamily::Lock => f.write_str("lock"),
            ColumnFamily::Write => f.write_str("write"),
            ColumnFamily::Custom(name) => f.write_str(name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ColumnFamily;
    use std::convert::TryFrom;

    #[test]
    fn custom_column_families_are_preserved_for_raw_requests() {
        assert_eq!(
            ColumnFamily::try_from("write").unwrap().to_string(),
            "write"
        );
        assert_eq!(
            ColumnFamily::try_from("tenant_cf").unwrap().to_string(),
            "tenant_cf"
        );
    }
}

trait RawRpcRequest: Default {
    fn set_cf(&mut self, cf: String);

    fn maybe_set_cf(&mut self, cf: Option<ColumnFamily>) {
        if let Some(cf) = cf {
            self.set_cf(cf.to_string());
        }
    }
}
