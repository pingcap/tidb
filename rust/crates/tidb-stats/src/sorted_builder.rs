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

//! Incremental sorted histogram construction from `builder.go`.

use tidb_datatype::{Collation, Datum, DatumValueError};

use crate::{Bucket, Histogram};

#[derive(Clone, Debug)]
pub struct SortedHistogramBuilder {
    histogram: Histogram,
    num_buckets: i64,
    values_per_bucket: i64,
    last_number: i64,
    bucket_index: i64,
    count: i64,
    need_bucket_ndv: bool,
}

impl SortedHistogramBuilder {
    #[must_use]
    pub fn new(num_buckets: i64, id: i64, stats_version: isize) -> Self {
        assert!(
            num_buckets >= 0,
            "histogram bucket count cannot be negative"
        );
        Self {
            histogram: Histogram {
                id,
                ..Histogram::default()
            },
            num_buckets,
            values_per_bucket: 1,
            last_number: 0,
            bucket_index: 0,
            count: 0,
            need_bucket_ndv: stats_version >= 2,
        }
    }

    #[must_use]
    pub const fn histogram(&self) -> &Histogram {
        &self.histogram
    }

    #[must_use]
    pub const fn count(&self) -> i64 {
        self.count
    }

    pub fn iterate(&mut self, value: Datum) -> Result<(), DatumValueError> {
        self.count = self.count.wrapping_add(1);
        if self.count == 1 {
            self.histogram.buckets.push(Bucket {
                count: 1,
                repeat: 1,
                ndv: i64::from(self.need_bucket_ndv),
                lower_bound: value.clone(),
                upper_bound: value,
            });
            self.histogram.ndv = 1;
            return Ok(());
        }
        let index = self.bucket_index as usize;
        let comparison = self.histogram.buckets[index]
            .upper_bound
            .compare(&value, Collation::Binary)?;
        if comparison == std::cmp::Ordering::Equal {
            let bucket = &mut self.histogram.buckets[index];
            bucket.count = bucket.count.wrapping_add(1);
            bucket.repeat = bucket.repeat.wrapping_add(1);
        } else if self.histogram.buckets[index]
            .count
            .wrapping_add(1)
            .wrapping_sub(self.last_number)
            <= self.values_per_bucket
        {
            self.update_last(value);
            self.histogram.ndv = self.histogram.ndv.wrapping_add(1);
        } else {
            if self.bucket_index.wrapping_add(1) == self.num_buckets {
                self.merge_buckets(index);
                self.values_per_bucket = self.values_per_bucket.wrapping_mul(2);
                self.bucket_index /= 2;
                self.last_number = if self.bucket_index == 0 {
                    0
                } else {
                    self.histogram.buckets[self.bucket_index as usize - 1].count
                };
            }
            let index = self.bucket_index as usize;
            if self.histogram.buckets[index]
                .count
                .wrapping_add(1)
                .wrapping_sub(self.last_number)
                <= self.values_per_bucket
            {
                self.update_last(value);
            } else {
                self.last_number = self.histogram.buckets[index].count;
                self.bucket_index = self.bucket_index.wrapping_add(1);
                self.histogram.buckets.push(Bucket {
                    count: self.last_number.wrapping_add(1),
                    repeat: 1,
                    ndv: i64::from(self.need_bucket_ndv),
                    lower_bound: value.clone(),
                    upper_bound: value,
                });
            }
            self.histogram.ndv = self.histogram.ndv.wrapping_add(1);
        }
        Ok(())
    }

    fn update_last(&mut self, value: Datum) {
        let bucket = &mut self.histogram.buckets[self.bucket_index as usize];
        bucket.upper_bound = value;
        bucket.count = bucket.count.wrapping_add(1);
        bucket.repeat = 1;
        if self.need_bucket_ndv && bucket.ndv > 0 {
            bucket.ndv = bucket.ndv.wrapping_add(1);
        }
    }

    fn merge_buckets(&mut self, bucket_index: usize) {
        let old = self.histogram.buckets.clone();
        let mut merged = Vec::with_capacity((bucket_index + 2) / 2);
        let mut position = 0;
        while position < bucket_index {
            merged.push(Bucket {
                count: old[position + 1].count,
                repeat: old[position + 1].repeat,
                ndv: old[position].ndv.wrapping_add(old[position + 1].ndv),
                lower_bound: old[position].lower_bound.clone(),
                upper_bound: old[position + 1].upper_bound.clone(),
            });
            position += 2;
        }
        if bucket_index.is_multiple_of(2) {
            merged.push(old[bucket_index].clone());
        }
        self.histogram.buckets = merged;
    }
}
