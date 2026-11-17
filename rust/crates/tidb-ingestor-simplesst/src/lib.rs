// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/simplesst`: the sorted KV files global sort writes to
//! object storage, their range-property (stat) files, and the readers and
//! merge iterators over them.
//!
//! A data file is a run of `<key-len><value-len><key><value>` blocks, both
//! lengths big-endian `u64`. Its stat file is a run of length-prefixed
//! [`RangeProperty`] records, one every `propSizeDist` bytes or
//! `propKeysDist` keys.

mod byte_reader;
mod codec;
mod file;
mod iter;
mod kv_reader;
mod onefile_writer;
mod util;
mod writer;

#[cfg(test)]
mod tests;

pub use byte_reader::{
    set_concurrent_reader_buffer_size_per_conc, CONCURRENT_READER_BUFFER_SIZE_PER_CONC,
};
pub use codec::RangeProperty;
pub use file::{KeyValueStore, LENGTH_BYTES};
pub use iter::{KVPair, MergeKVIter, MergePropIter};
pub use kv_reader::{KVReader, StatsReader, DEFAULT_READ_BUFFER_SIZE};
pub use onefile_writer::{
    OneFileWriter, DEFAULT_ONE_WRITER_BLOCK_SIZE, DEFAULT_ONE_WRITER_MEM_SIZE_LIMIT,
    MAX_UPLOAD_PART_COUNT,
};
pub use util::{
    get_all_file_names, get_max_overlapping, get_read_range_from_props, remove_duplicates,
    Endpoint, EndpointTp,
};
pub use writer::*;

/// The failures of this package, keeping the Go error a caller tests for.
#[derive(Clone, Debug)]
pub enum Error {
    /// Go `io.EOF`: the reader is exhausted.
    Eof,
    /// Go `io.ErrUnexpectedEOF`, possibly annotated.
    UnexpectedEof(String),
    /// An object-storage failure.
    Storage(tidb_objstore::Error),
    /// A Go `*errors.Error`, such as `ErrFoundDuplicateKeys`.
    Terror(tidb_terror::TerrorError),
    /// Go `ErrFoundDuplicateKeys.FastGenByArgs(key, value)`: the error with
    /// its two `[]byte` arguments, which Go reads back with `tErr.Args()`.
    FoundDuplicateKeys {
        /// The duplicated key.
        key: Vec<u8>,
        /// One of its values.
        value: Vec<u8>,
        /// The generated `ErrFoundDuplicateKeys`.
        error: tidb_terror::TerrorError,
    },
    /// Any other Go error text.
    Other(String),
}

impl Error {
    /// Go `errors.Is(err, io.EOF)`.
    #[must_use]
    pub fn is_eof(&self) -> bool {
        matches!(self, Self::Eof)
    }

    /// Go `errors.Is(err, io.ErrUnexpectedEOF)`.
    #[must_use]
    pub fn is_unexpected_eof(&self) -> bool {
        matches!(self, Self::UnexpectedEof(_))
    }

    /// Go `errors.Annotatef(err, msg)`.
    #[must_use]
    pub fn annotate(self, msg: impl std::fmt::Display) -> Self {
        match self {
            Self::UnexpectedEof(text) => Self::UnexpectedEof(format!("{msg}: {text}")),
            Self::Storage(e) => Self::Storage(e.annotate(msg)),
            other => Self::Other(format!("{msg}: {other}")),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eof => f.write_str("EOF"),
            Self::UnexpectedEof(text) | Self::Other(text) => f.write_str(text),
            Self::Storage(e) => e.fmt(f),
            Self::Terror(e) | Self::FoundDuplicateKeys { error: e, .. } => e.fmt(f),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Terror(e) | Self::FoundDuplicateKeys { error: e, .. } => Some(e),
            _ => None,
        }
    }
}

impl From<tidb_objstore::Error> for Error {
    fn from(e: tidb_objstore::Error) -> Self {
        Self::Storage(e)
    }
}

/// The package's result.
pub type Result<T> = std::result::Result<T, Error>;

/// Go `tErr.Args()` of an `ErrFoundDuplicateKeys` anywhere in `error`'s
/// chain: the duplicated key and its value.
#[must_use]
pub fn duplicate_key_and_value<'a>(error: &'a (dyn std::error::Error + 'static)) -> Option<(&'a [u8], &'a [u8])> {
    let mut cause = Some(error);
    while let Some(error) = cause {
        if let Some(Error::FoundDuplicateKeys { key, value, .. }) = error.downcast_ref::<Error>() {
            return Some((key, value));
        }
        cause = error.source();
    }
    None
}

/// Go `io.ErrUnexpectedEOF`'s text.
pub(crate) const UNEXPECTED_EOF: &str = "unexpected EOF";

/// Go `noEOF`: an EOF in the middle of a record is unexpected.
pub(crate) fn no_eof(e: Error) -> Error {
    if e.is_eof() {
        tidb_log::warn("unexpected EOF", &[]);
        return Error::UnexpectedEof(UNEXPECTED_EOF.to_owned());
    }
    e
}
