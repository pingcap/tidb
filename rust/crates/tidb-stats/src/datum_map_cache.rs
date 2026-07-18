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

//! Encoded-TopN to Datum cache metadata from `pkg/statistics/cmsketch_util.go`.
//!
//! The Go owner combines this cache with schema-aware codec conversion.  This
//! leaf owns the byte-keyed cache and accepts an already-decoded Datum so that
//! conversion remains an explicit `tidb-codec`/field-type boundary.

use std::collections::HashMap;

use tidb_datatype::Datum;

/// Caches decoded Datum values by their immutable encoded TopN bytes.
#[derive(Clone, Debug, Default)]
pub struct DatumMapCache {
    datum_map: HashMap<Vec<u8>, Datum>,
}

impl DatumMapCache {
    /// Creates an empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a cloned Datum for an encoded key, if it has been cached.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Datum> {
        self.datum_map.get(key).cloned()
    }

    /// Stores an already-decoded Datum and returns the stored value.
    ///
    /// Repeated keys replace the previous value, matching Go map assignment.
    pub fn put(&mut self, key: &[u8], datum: Datum) -> Datum {
        self.datum_map.insert(key.to_vec(), datum.clone());
        datum
    }

    /// Returns the number of cached encoded keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.datum_map.len()
    }

    /// Returns whether no encoded keys are cached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.datum_map.is_empty()
    }
}
