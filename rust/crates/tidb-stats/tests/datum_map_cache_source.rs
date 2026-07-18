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

use tidb_datatype::Datum;
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
fn source_cache_keys_are_owned_bytes_and_values_are_overwritable() {
    let mut cache = DatumMapCache::new();
    let mut encoded = b"mutable".to_vec();
    cache.put(&encoded, Datum::new_bytes(b"first".to_vec()));
    encoded[0] = b'M';

    assert_eq!(
        cache.get(b"mutable"),
        Some(Datum::new_bytes(b"first".to_vec()))
    );
    cache.put(b"mutable", Datum::new_uint(7));
    assert_eq!(cache.get(b"mutable"), Some(Datum::new_uint(7)));
    assert_eq!(cache.len(), 1);
}
