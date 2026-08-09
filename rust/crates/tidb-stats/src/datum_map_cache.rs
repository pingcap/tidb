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
//! The cache owns the same schema-aware conversion boundary as Go: index TopN
//! values remain bytes, time and float fields use typed decoding, and every
//! other column uses the ordinary one-datum decoder.

use std::collections::HashMap;

use chrono::TimeZone;
use tidb_codec::{decode_as_datetime, decode_as_float32, decode_one, CodecError};
use tidb_datatype::{Datum, FieldTypeCode, TimeType};

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

    /// Go `DatumMapCache.Put` and `topNMetaToDatum`.
    pub fn put_encoded<TZ: TimeZone>(
        &mut self,
        cache_key: &[u8],
        encoded_value: &[u8],
        mysql_type: u8,
        is_index: bool,
        timezone: Option<&TZ>,
    ) -> Result<Datum, CodecError> {
        let datum = top_n_meta_to_datum(encoded_value, mysql_type, is_index, timezone)?;
        Ok(self.put(cache_key, datum))
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

fn top_n_meta_to_datum<TZ: TimeZone>(
    encoded_value: &[u8],
    mysql_type: u8,
    is_index: bool,
    timezone: Option<&TZ>,
) -> Result<Datum, CodecError> {
    if is_index {
        return Ok(Datum::Bytes(encoded_value.to_vec()));
    }

    let datum = match FieldTypeCode::from_mysql_type(mysql_type) {
        FieldTypeCode::Date => decode_as_datetime(encoded_value, TimeType::Date, timezone)?.1,
        FieldTypeCode::Datetime => {
            decode_as_datetime(encoded_value, TimeType::DateTime, timezone)?.1
        }
        FieldTypeCode::Timestamp => {
            decode_as_datetime(encoded_value, TimeType::Timestamp, timezone)?.1
        }
        FieldTypeCode::Float => decode_as_float32(encoded_value)?.1,
        _ => decode_one(encoded_value)?.1,
    };
    Ok(datum)
}
