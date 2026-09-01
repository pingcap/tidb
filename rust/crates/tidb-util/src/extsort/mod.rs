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

//! External sorting from Go `pkg/util/extsort`.

mod disk_sorter;

use std::fmt;
use std::sync::atomic::AtomicBool;

pub use disk_sorter::{open_disk_sorter, DiskSorter, DiskSorterOptions};

/// An error returned by the external sorter.
#[derive(Debug)]
pub struct Error {
    message: String,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl Error {
    pub(crate) fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    pub(crate) fn canceled() -> Self {
        Self::message("context canceled")
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self {
            message: error.to_string(),
            source: Some(Box::new(error)),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self {
            message: error.to_string(),
            source: Some(Box::new(error)),
        }
    }
}

/// Result returned by this package.
pub type Result<T> = std::result::Result<T, Error>;

/// A sorter for key-value pairs held in external storage.
///
/// Keys are bytewise ordered and duplicate keys are removed.
pub trait ExternalSorter: Send + Sync {
    /// Creates a writer while the sorter is in its writing state.
    fn new_writer(&self, canceled: &AtomicBool) -> Result<Box<dyn Writer>>;

    /// Atomically and idempotently sorts all closed writer runs.
    fn sort(&self, canceled: &AtomicBool) -> Result<()>;

    /// Reports whether iterators may be created.
    fn is_sorted(&self) -> bool;

    /// Creates an iterator after sorting has completed.
    fn new_iterator(&self, canceled: &AtomicBool) -> Result<Box<dyn Iterator>>;

    /// Releases resources without deleting the sorter's directory.
    fn close(&self) -> Result<()>;

    /// Releases resources and removes the sorter's directory.
    fn close_and_cleanup(&self) -> Result<()>;
}

/// A buffered writer used before sorting starts.
pub trait Writer: Send {
    /// Adds one key-value pair without retaining the input slices.
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Flushes buffered pairs while keeping the writer reusable.
    fn flush(&mut self) -> Result<()>;

    /// Flushes and closes the writer.
    fn close(&mut self) -> Result<()>;
}

/// A forward iterator over sorted, unique key-value pairs.
pub trait Iterator: Send {
    /// Moves to the first key greater than or equal to `key`.
    fn seek(&mut self, key: &[u8]) -> bool;

    /// Moves to the first key.
    fn first(&mut self) -> bool;

    /// Moves to the next strictly greater key.
    fn next(&mut self) -> bool;

    /// Moves to the last key.
    fn last(&mut self) -> bool;

    /// Reports whether the iterator is positioned on a pair.
    fn valid(&self) -> bool;

    /// Returns the first iteration error, if any.
    fn error(&self) -> Option<&Error>;

    /// Returns the current key without copying.
    fn unsafe_key(&self) -> &[u8];

    /// Returns the current value without copying.
    fn unsafe_value(&self) -> &[u8];

    /// Releases resources held by the iterator.
    fn close(&mut self) -> Result<()>;
}
