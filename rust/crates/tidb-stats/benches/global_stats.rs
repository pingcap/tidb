// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/statistics/handle/globalstats/topn_bench_test.go`.

use std::hint::black_box;
use std::time::Instant;

use chrono::Utc;
use tidb_codec::encode_key;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_stats::histogram::{Bucket, Histogram};
use tidb_stats::{
    merge_partition_topn, merge_partition_topn_concurrently, TopN, MAX_PARTITION_MERGE_BATCH_SIZE,
};
use tidb_util::sqlkiller::SqlKiller;

const BENCHMARK_SIZES: [usize; 5] = [100, 1_000, 2_000, 5_000, 10_000];
const TOPN_VALUES: usize = 500;
const GLOBAL_TOPN: u32 = 100;
const CONCURRENCY: usize = 4;

fn encoded(value: usize) -> Vec<u8> {
    encode_key(&[Datum::Int(value as i64)]).expect("integer key encodes")
}

fn prepare(partitions: usize) -> (Vec<TopN>, Vec<Histogram>) {
    let topns = (0..partitions)
        .map(|partition| {
            let mut topn = TopN::new(TOPN_VALUES);
            for value in 1..=TOPN_VALUES {
                if partition % 2 == 0 && value % 2 == 0 {
                    continue;
                }
                topn.append(&encoded(value), ((partition + value) % 1_000) as u64);
            }
            topn
        })
        .collect();
    let histograms = (0..partitions)
        .map(|_| Histogram {
            id: 1,
            ndv: TOPN_VALUES as i64,
            buckets: (1..=TOPN_VALUES)
                .map(|value| Bucket {
                    count: (10 + value * 10) as i64,
                    repeat: 10,
                    ndv: 0,
                    lower_bound: Datum::Int(value as i64),
                    upper_bound: Datum::Int(value as i64),
                })
                .collect(),
            ..Histogram::default()
        })
        .collect();
    (topns, histograms)
}

fn main() {
    let killer = SqlKiller::default();
    let field_type = FieldType::new(FieldTypeCode::Tiny);
    for partitions in BENCHMARK_SIZES {
        let (topns, histograms) = prepare(partitions);
        let refs = topns.iter().map(Some).collect::<Vec<_>>();
        let started = Instant::now();
        black_box(
            merge_partition_topn(
                Some(&Utc),
                1,
                &refs,
                GLOBAL_TOPN,
                histograms,
                &field_type,
                false,
                &killer,
            )
            .expect("sequential global TopN merge"),
        );
        println!(
            "BenchmarkMergePartTopN2GlobalTopNWithHists/Size{partitions}: {:?}",
            started.elapsed()
        );

        let (topns, histograms) = prepare(partitions);
        let refs = topns.iter().map(Some).collect::<Vec<_>>();
        let batch_size = (partitions / CONCURRENCY).clamp(1, MAX_PARTITION_MERGE_BATCH_SIZE);
        let started = Instant::now();
        black_box(
            merge_partition_topn_concurrently(
                Some(&Utc),
                1,
                &refs,
                GLOBAL_TOPN,
                histograms,
                &field_type,
                false,
                CONCURRENCY,
                batch_size,
                &killer,
            )
            .expect("concurrent global TopN merge"),
        );
        println!(
            "BenchmarkMergeGlobalStatsTopNByConcurrencyWithHists/Size{partitions}: {:?}",
            started.elapsed()
        );
    }
}
