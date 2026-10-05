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

//! Source-backed tests for encoded TopN Datum cache metadata.

use chrono::FixedOffset;
use chrono_tz::UTC;
use tidb_codec::{encode_key, encode_value_in_timezone, NIL_FLAG};
use tidb_datatype::{parse_datetime, Datum, TimeType};
use tidb_stats::DatumMapCache;

#[test]
fn source_cache_misses_before_insert() {
    let cache = DatumMapCache::new();
    assert!(cache.is_empty());
    assert_eq!(cache.get(b"missing"), None);
}

#[test]
fn source_cache_put_returns_and_retrieves_decoded_value() {
    let mut cache = DatumMapCache::new();
    let datum = Datum::new_int(42);
    assert_eq!(cache.put(b"encoded", datum.clone()), datum);
    assert_eq!(cache.get(b"encoded"), Some(datum));
    assert_eq!(cache.len(), 1);
}

#[test]
fn source_cache_repeated_keys_replace_values() {
    let mut cache = DatumMapCache::new();
    cache.put(b"encoded", Datum::new_bytes(b"first".to_vec()));
    cache.put(b"encoded", Datum::new_uint(7));
    assert_eq!(cache.get(b"encoded"), Some(Datum::new_uint(7)));
    assert_eq!(cache.len(), 1);
}

#[test]
fn source_encoded_put_preserves_index_and_general_column_boundaries() {
    let mut cache = DatumMapCache::new();
    let encoded = encode_key(&[Datum::Int(42)]).unwrap();
    let decoded = cache
        .put_encoded(b"column", &encoded, 3, false, Some(&UTC))
        .unwrap();
    assert_eq!(decoded, Datum::Int(42));
    assert_eq!(cache.get(b"column"), Some(Datum::Int(42)));

    let index = cache
        .put_encoded(b"index", &encoded, 3, true, Some(&UTC))
        .unwrap();
    assert_eq!(index, Datum::Bytes(encoded.clone()));
    assert_eq!(cache.get(b"index"), Some(Datum::Bytes(encoded)));
}

#[test]
fn source_encoded_put_uses_float32_decode_and_is_atomic_on_errors() {
    let mut cache = DatumMapCache::new();
    let encoded = encode_key(&[Datum::Real(1.25)]).unwrap();
    assert_eq!(
        cache
            .put_encoded(b"float", &encoded, 4, false, Some(&UTC))
            .unwrap(),
        Datum::Float32(1.25)
    );
    cache.put(b"stable", Datum::Int(9));
    assert!(cache
        .put_encoded(b"stable", &[0xff], 3, false, Some(&UTC))
        .is_err());
    assert_eq!(cache.get(b"stable"), Some(Datum::Int(9)));

    assert!(cache
        .put_encoded(b"null-float", &[NIL_FLAG], 4, false, Some(&UTC))
        .is_err());
    assert!(cache.get(b"null-float").is_none());
}

#[test]
fn source_encoded_put_decodes_all_time_kinds_and_timestamp_timezone() {
    let mut cache = DatumMapCache::new();
    for (key, mysql_type, kind) in [
        (&b"date"[..], 10, TimeType::Date),
        (&b"datetime"[..], 12, TimeType::DateTime),
    ] {
        let mut value = parse_datetime("2011-11-11 11:11:11", &UTC, true, false)
            .unwrap()
            .time;
        value.set_kind(kind);
        let encoded = encode_value_in_timezone(&UTC, &[Datum::new_time(value)]).unwrap();
        assert_eq!(
            cache
                .put_encoded(key, &encoded, mysql_type, false, Some(&UTC))
                .unwrap(),
            Datum::new_time(value)
        );
    }

    let east_eight = FixedOffset::east_opt(8 * 60 * 60).unwrap();
    let mut timestamp = parse_datetime("2011-11-11 11:11:11", &UTC, true, false)
        .unwrap()
        .time;
    timestamp.set_kind(TimeType::Timestamp);
    let encoded = encode_value_in_timezone(&east_eight, &[Datum::new_time(timestamp)]).unwrap();
    assert_eq!(
        cache
            .put_encoded(b"timestamp", &encoded, 7, false, Some(&east_eight))
            .unwrap(),
        Datum::new_time(timestamp)
    );

    assert_eq!(
        cache
            .put_encoded(b"null-time", &[NIL_FLAG], 12, false, Some(&UTC))
            .unwrap(),
        Datum::Null
    );
}
