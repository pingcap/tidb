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

//! Direct coprocessor cache key and admission obligations from TiDB's Go tests.

use std::mem;

use prost::Message;
use tidb_distsql::{
    build_copr_cache_key, CoprCache, CoprCacheAdmission, CoprCacheConfig, CoprCacheError,
    CoprCacheRequestContext, CoprCacheResponseContext, CoprCacheResponseOutcome, CoprCacheValue,
    CoprocessorRequestEnvelope, RequestKeyRange,
};
use tidb_proto::{CoprocessorKeyRange, CoprocessorResponse};

const MILLISECOND: i64 = 1_000_000;

fn admission(config: CoprCacheConfig) -> CoprCacheAdmission {
    CoprCacheAdmission::from_config(&config)
        .expect("valid source config")
        .expect("enabled source config")
}

fn cache() -> CoprCache {
    CoprCache::from_config(&CoprCacheConfig {
        admission_min_process_ms: 5,
        admission_max_result_mb: 1.0,
        capacity_mb: 1.0,
        ..CoprCacheConfig::default()
    })
    .expect("valid source config")
    .expect("enabled source config")
}

fn request_context(region_id: u64, start_ts: u64) -> CoprCacheRequestContext {
    CoprCacheRequestContext {
        is_unary_cop: true,
        cacheable: true,
        region_id,
        start_ts,
    }
}

fn response_context(
    region_id: u64,
    start_ts: u64,
    paging_enabled: bool,
) -> CoprCacheResponseContext {
    CoprCacheResponseContext {
        start_ts,
        region_id,
        process_time_nanos: Some(5 * MILLISECOND),
        paging_task_index: 0,
        paging_enabled,
    }
}

#[test]
fn test_build_cache_key() {
    const BYTE_PAGING_SIZE: u64 = 0x0102_0304_0506_0708;
    const ROW_PAGING_SIZE: u64 = 0x1112_1314_1516_1718;

    let mut request = CoprocessorRequestEnvelope {
        tp: 0xab,
        start_ts: 0xaa_bb_cc,
        data: vec![0x18, 0, 0x20, 0, 0x40, 0, 0x5a, 0],
        ranges: vec![
            RequestKeyRange {
                start_key: vec![0x01].into(),
                end_key: vec![0x01, 0x02].into(),
            },
            RequestKeyRange {
                start_key: vec![0x01, 0x01, 0x02].into(),
                end_key: vec![0x01, 0x01, 0x03].into(),
            },
        ],
        ..CoprocessorRequestEnvelope::default()
    };

    let expected = vec![
        0xab, 0x08, 0, 0, 0, 0x18, 0, 0x20, 0, 0x40, 0, 0x5a, 0, 0x01, 0, 0x01, 0x02, 0, 0x01,
        0x02, 0x03, 0, 0x01, 0x01, 0x02, 0x03, 0, 0x01, 0x01, 0x03,
    ];
    assert_eq!(build_copr_cache_key(&request).unwrap(), expected);

    request.paging_size_bytes = BYTE_PAGING_SIZE;
    let mut paging_expected = expected.clone();
    paging_expected.push(1);
    assert_eq!(build_copr_cache_key(&request).unwrap(), paging_expected);

    request.paging_size = ROW_PAGING_SIZE;
    request.paging_size_bytes = 0;
    assert_eq!(build_copr_cache_key(&request).unwrap(), paging_expected);

    request.paging_size_bytes = BYTE_PAGING_SIZE;
    assert_eq!(build_copr_cache_key(&request).unwrap(), paging_expected);

    request = CoprocessorRequestEnvelope {
        tp: 0xabcc,
        start_ts: 0xaa_bb_cc,
        data: vec![0x18],
        ..CoprocessorRequestEnvelope::default()
    };
    assert_eq!(
        build_copr_cache_key(&request),
        Err(CoprCacheError::RequestTypeTooBig)
    );
}

#[test]
fn test_admission() {
    let cache = admission(CoprCacheConfig {
        admission_min_process_ms: 5,
        admission_max_result_mb: 1.0,
        capacity_mb: 1.0,
        ..CoprCacheConfig::default()
    });

    assert!(cache.check_request(0));
    assert!(cache.check_request(1000));
    assert!(!cache.check_response(0, 0, 0));
    assert!(!cache.check_response(0, 4 * MILLISECOND, 0));
    assert!(!cache.check_response(0, 5 * MILLISECOND, 0));
    assert!(!cache.check_response(1, 0, 0));
    assert!(!cache.check_response(1, 4 * MILLISECOND, 0));
    assert!(cache.check_response(1, 5 * MILLISECOND, 0));
    assert!(cache.check_response(1024, 5 * MILLISECOND, 0));
    assert!(cache.check_response(1024 * 1024, 5 * MILLISECOND, 0));
    assert!(!cache.check_response(1024 * 1024 + 1, 5 * MILLISECOND, 0));
    assert!(!cache.check_response(1024 * 1024 + 1, 4 * MILLISECOND, 0));
    assert!(cache.check_response(1024, 4 * MILLISECOND, 1));
    assert!(!cache.check_response(1024, 4 * MILLISECOND, 51));

    let cache = admission(CoprCacheConfig {
        admission_max_ranges: 5,
        admission_min_process_ms: 5,
        admission_max_result_mb: 1.0,
        capacity_mb: 1.0,
    });
    assert!(cache.check_request(0));
    assert!(cache.check_request(5));
    assert!(!cache.check_request(6));
}

#[test]
fn test_disable() {
    assert!(CoprCache::from_optional_config(None).unwrap().is_none());

    let mut disabled = CoprCache::from_config(&CoprCacheConfig {
        capacity_mb: 0.0,
        ..CoprCacheConfig::default()
    })
    .unwrap();
    assert!(disabled.is_none());

    // Go permits calls through a nil *coprCache and returns false/nil. The
    // Rust owner makes disabled state explicit, so its caller performs the
    // same three operations through Option without fabricating an enabled
    // cache value.
    assert!(!disabled
        .as_mut()
        .is_some_and(|cache| cache.set(b"foo".to_vec(), CoprCacheValue::default(),)));
    assert!(disabled
        .as_ref()
        .and_then(|cache| cache.get(b"foo"))
        .is_none());
    assert!(!disabled
        .as_ref()
        .is_some_and(|cache| { cache.check_response_admission(1024, 5_000 * MILLISECOND, 0) }));

    assert_eq!(
        CoprCache::from_config(&CoprCacheConfig {
            capacity_mb: 0.001,
            ..CoprCacheConfig::default()
        })
        .unwrap_err(),
        CoprCacheError::AdmissionMaxResultMustBePositive
    );

    assert!(CoprCache::from_config(&CoprCacheConfig {
        capacity_mb: 0.001,
        admission_max_result_mb: 1.0,
        ..CoprCacheConfig::default()
    })
    .unwrap()
    .is_some());

    let less_than_one_byte_mb = 0.5 / (1024.0 * 1024.0);
    assert_eq!(
        CoprCache::from_config(&CoprCacheConfig {
            capacity_mb: less_than_one_byte_mb,
            admission_max_result_mb: 1.0,
            ..CoprCacheConfig::default()
        })
        .unwrap_err(),
        CoprCacheError::CapacityMustBePositive
    );
    assert_eq!(
        CoprCache::from_config(&CoprCacheConfig {
            capacity_mb: 1.0,
            admission_max_result_mb: less_than_one_byte_mb,
            ..CoprCacheConfig::default()
        })
        .unwrap_err(),
        CoprCacheError::AdmissionMaxResultMustBePositive
    );
}

#[test]
fn test_cache_value_len() {
    // On the source's 64-bit target this is four 24-byte slice headers and
    // three 8-byte integers, exactly the asserted Go unsafe.Sizeof value.
    assert_eq!(mem::size_of::<CoprCacheValue>(), 120);

    let value = CoprCacheValue {
        timestamp: 0x123,
        region_id: 0x1,
        region_data_version: 0x3,
        ..CoprCacheValue::default()
    };
    assert_eq!(value.len(), 120);

    let value = CoprCacheValue {
        key: b"foobar".to_vec(),
        data: b"12345678".to_vec(),
        timestamp: 0x123,
        region_id: 0x1,
        region_data_version: 0x3,
        ..CoprCacheValue::default()
    };
    assert_eq!(value.len(), 120 + value.key.len() + value.data.len());

    let value = CoprCacheValue {
        key: b"foobar".to_vec(),
        data: b"12345678".to_vec(),
        timestamp: 0x123,
        region_id: 0x1,
        region_data_version: 0x3,
        page_end: Some(b"3235".to_vec()),
        ..CoprCacheValue::default()
    };
    assert_eq!(
        value.len(),
        120 + value.key.len() + value.data.len() + value.page_end.as_ref().unwrap().len()
    );
}

#[test]
fn test_get_set_and_live_request_response_lifecycle() {
    let mut cache = cache();
    assert!(cache.get(b"foo").is_none());
    assert!(cache.set(
        b"foo".to_vec(),
        CoprCacheValue {
            key: b"caller value is replaced".to_vec(),
            data: b"bar".to_vec(),
            timestamp: 0x123,
            region_id: 0x1,
            region_data_version: 0x3,
            ..CoprCacheValue::default()
        }
    ));
    assert_eq!(cache.get(b"foo").unwrap().data, b"bar");
    assert_eq!(cache.get(b"foo").unwrap().key, b"foo");
    assert!(cache.get(b"foO").is_none());

    // Rust owns both map and retained-value keys. Mutating a caller copy can
    // neither invalidate the insertion nor make another exact key match.
    let mut caller_key = b"stable-key".to_vec();
    assert!(cache.set(
        caller_key.clone(),
        CoprCacheValue {
            data: b"stable-value".to_vec(),
            ..CoprCacheValue::default()
        },
    ));
    caller_key.fill(b'x');
    assert_eq!(cache.get(b"stable-key").unwrap().data, b"stable-value");
    assert!(cache.get(&caller_key).is_none());

    let mut request = CoprocessorRequestEnvelope {
        tp: 0xab,
        data: b"dag".to_vec(),
        ranges: vec![RequestKeyRange {
            start_key: b"a".to_vec().into(),
            end_key: b"z".to_vec().into(),
        }],
        paging_size_bytes: 1024,
        ..CoprocessorRequestEnvelope::default()
    };
    let lookup = cache
        .prepare_request(&mut request, request_context(7, 100))
        .unwrap();
    assert!(request.is_cache_enabled);
    assert_eq!(request.cache_if_match_version, 0);
    assert!(lookup.value().is_none());

    let mut miss = CoprocessorResponse {
        data: b"cached".to_vec(),
        range: Some(CoprocessorKeyRange {
            start: b"m".to_vec(),
            end: b"z".to_vec(),
        }),
        cache_last_version: 9,
        can_be_cached: true,
        ..CoprocessorResponse::default()
    };
    assert_eq!(
        cache
            .handle_response(&mut miss, Some(&lookup), response_context(7, 100, true),)
            .unwrap(),
        CoprCacheResponseOutcome::Stored
    );

    let lookup = cache
        .prepare_request(&mut request, request_context(7, 101))
        .unwrap();
    assert_eq!(request.cache_if_match_version, 9);
    assert_eq!(
        lookup.value().unwrap().page_start.as_deref(),
        Some(&b"m"[..])
    );

    let mut hit = CoprocessorResponse {
        is_cache_hit: true,
        ..CoprocessorResponse::default()
    };
    assert_eq!(
        cache
            .handle_response(&mut hit, Some(&lookup), response_context(7, 101, true),)
            .unwrap(),
        CoprCacheResponseOutcome::Hit
    );
    assert_eq!(hit.data, b"cached");
    assert_eq!(hit.range.unwrap().start, b"m");

    let stale_region = cache
        .prepare_request(&mut request, request_context(8, 101))
        .unwrap();
    assert_eq!(request.cache_if_match_version, 0);
    assert!(stale_region.value().is_none());

    let stale_timestamp = cache
        .prepare_request(&mut request, request_context(7, 99))
        .unwrap();
    assert_eq!(request.cache_if_match_version, 0);
    assert!(stale_timestamp.value().is_none());
}

#[test]
fn request_eligibility_and_bounded_storage_stay_inside_the_owner() {
    let mut cache = CoprCache::from_config(&CoprCacheConfig {
        capacity_mb: 0.0003,
        admission_max_ranges: 1,
        admission_max_result_mb: 1.0,
        ..CoprCacheConfig::default()
    })
    .unwrap()
    .unwrap();
    let mut request = CoprocessorRequestEnvelope {
        tp: 1,
        ranges: vec![RequestKeyRange::default(), RequestKeyRange::default()],
        ..CoprocessorRequestEnvelope::default()
    };
    assert!(cache
        .prepare_request(&mut request, request_context(1, 1))
        .is_none());
    assert!(!request.is_cache_enabled);

    request.ranges.truncate(1);
    assert!(cache
        .prepare_request(
            &mut request,
            CoprCacheRequestContext {
                cacheable: false,
                ..request_context(1, 1)
            },
        )
        .is_none());
    assert!(!request.is_cache_enabled);

    let small_value = CoprCacheValue {
        data: vec![1; 100],
        ..CoprCacheValue::default()
    };
    assert!(cache.set(b"first".to_vec(), small_value.clone()));
    assert!(cache.set(b"second".to_vec(), small_value));
    assert_eq!(cache.len(), 1);
    assert!(cache.get(b"first").is_none());
    assert!(cache.get(b"second").is_some());
    assert!(!cache.set(
        b"oversize".to_vec(),
        CoprCacheValue {
            data: vec![2; 400],
            ..CoprCacheValue::default()
        },
    ));
    assert!(cache.get(b"second").is_some());
}

#[test]
fn response_cache_fields_keep_exact_wire_numbers() {
    let response = CoprocessorResponse {
        is_cache_hit: true,
        cache_last_version: 42,
        can_be_cached: true,
        ..CoprocessorResponse::default()
    };
    assert_eq!(
        response.encode_to_vec(),
        vec![0x38, 0x01, 0x40, 0x2a, 0x48, 0x01]
    );
}

#[test]
fn hit_without_a_valid_local_value_is_rejected() {
    let mut cache = cache();
    let mut response = CoprocessorResponse {
        data: b"tikv must remain untouched".to_vec(),
        range: Some(CoprocessorKeyRange {
            start: b"a".to_vec(),
            end: b"z".to_vec(),
        }),
        is_cache_hit: true,
        ..CoprocessorResponse::default()
    };
    assert_eq!(
        cache.handle_response(&mut response, None, response_context(7, 100, false)),
        Err(CoprCacheError::IllegalCacheHit)
    );
    assert_eq!(response.data, b"tikv must remain untouched");
    assert_eq!(response.range.unwrap().start, b"a");
}

#[test]
fn paging_hit_preserves_absent_present_empty_and_nonpaging_range_states() {
    let mut cache = cache();
    let mut request = CoprocessorRequestEnvelope {
        tp: 1,
        data: b"range-presence".to_vec(),
        ranges: vec![RequestKeyRange {
            start_key: b"a".to_vec().into(),
            end_key: b"z".to_vec().into(),
        }],
        paging_size_bytes: 1,
        ..CoprocessorRequestEnvelope::default()
    };
    let lookup = cache
        .prepare_request(&mut request, request_context(7, 100))
        .unwrap();
    let mut empty_range_miss = CoprocessorResponse {
        data: b"empty-range".to_vec(),
        range: Some(CoprocessorKeyRange::default()),
        cache_last_version: 1,
        can_be_cached: true,
        ..CoprocessorResponse::default()
    };
    assert_eq!(
        cache
            .handle_response(
                &mut empty_range_miss,
                Some(&lookup),
                response_context(7, 100, true),
            )
            .unwrap(),
        CoprCacheResponseOutcome::Stored
    );
    let empty_range_lookup = cache
        .prepare_request(&mut request, request_context(7, 101))
        .unwrap();
    assert_eq!(
        empty_range_lookup.value().unwrap().page_start.as_deref(),
        Some(&b""[..])
    );
    let mut empty_range_hit = CoprocessorResponse {
        is_cache_hit: true,
        ..CoprocessorResponse::default()
    };
    cache
        .handle_response(
            &mut empty_range_hit,
            Some(&empty_range_lookup),
            response_context(7, 101, true),
        )
        .unwrap();
    assert_eq!(empty_range_hit.range, Some(CoprocessorKeyRange::default()));

    let absent_key = build_copr_cache_key(&CoprocessorRequestEnvelope {
        data: b"absent-range".to_vec(),
        ..request.clone()
    })
    .unwrap();
    assert!(cache.set(
        absent_key,
        CoprCacheValue {
            data: b"absent-range".to_vec(),
            timestamp: 100,
            region_id: 7,
            region_data_version: 2,
            page_start: None,
            page_end: None,
            ..CoprCacheValue::default()
        },
    ));
    let mut absent_request = CoprocessorRequestEnvelope {
        data: b"absent-range".to_vec(),
        ..request
    };
    let absent_lookup = cache
        .prepare_request(&mut absent_request, request_context(7, 101))
        .unwrap();
    let mut absent_hit = CoprocessorResponse {
        is_cache_hit: true,
        range: Some(CoprocessorKeyRange {
            start: b"stale".to_vec(),
            end: b"stale".to_vec(),
        }),
        ..CoprocessorResponse::default()
    };
    cache
        .handle_response(
            &mut absent_hit,
            Some(&absent_lookup),
            response_context(7, 101, true),
        )
        .unwrap();
    assert!(absent_hit.range.is_none());

    let mut nonpaging_hit = CoprocessorResponse {
        is_cache_hit: true,
        range: Some(CoprocessorKeyRange {
            start: b"transport".to_vec(),
            end: b"range".to_vec(),
        }),
        ..CoprocessorResponse::default()
    };
    cache
        .handle_response(
            &mut nonpaging_hit,
            Some(&absent_lookup),
            response_context(7, 101, false),
        )
        .unwrap();
    assert_eq!(nonpaging_hit.range.unwrap().start, b"transport");
}

#[test]
fn test_issue_24118() {
    let error = CoprCacheAdmission::from_config(&CoprCacheConfig {
        admission_min_process_ms: 5,
        admission_max_result_mb: 1.0,
        capacity_mb: -1.0,
        ..CoprCacheConfig::default()
    })
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Capacity must be > 0 to enable the cache"
    );
}
