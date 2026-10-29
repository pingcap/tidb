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

use tidb_datatype::{Datum, DatumValueError, FieldType};
use tidb_charset::Collation;

use crate::Histogram;

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
    /// Go `NewSortedBuilder(sc, numBuckets, id, tp, statsVer)`.
    pub fn new(num_buckets: i64, id: i64, tp: &FieldType, stats_version: isize) -> Self {
        assert!(
            num_buckets >= 0,
            "histogram bucket count cannot be negative"
        );
        Self {
            histogram: Histogram::new(id, 0, 0, 0, tp, num_buckets as usize, 0),
            num_buckets,
            values_per_bucket: 1,
            last_number: 0,
            bucket_index: 0,
            count: 0,
            need_bucket_ndv: stats_version >= 2,
        }
    }

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
            self.append_bucket(value, 1);
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
                self.histogram.merge_neighbor_buckets(index);
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
                self.append_bucket(value, self.last_number.wrapping_add(1));
            }
            self.histogram.ndv = self.histogram.ndv.wrapping_add(1);
        }
        Ok(())
    }

    /// Go `Iterate`'s `appendBucket`: `AppendBucketWithNDV(.., 1)` when
    /// bucket NDV is kept, else `AppendBucket`.
    fn append_bucket(&mut self, value: Datum, count: i64) {
        self.histogram.append_bucket_with_ndv(
            value.clone(),
            value,
            count,
            1,
            i64::from(self.need_bucket_ndv),
        );
    }

    /// Go `updateLastBucket(&data, Buckets[bucketIdx].Count+1, 1,
    /// needBucketNDV)`.
    fn update_last(&mut self, value: Datum) {
        let count = self.histogram.buckets[self.bucket_index as usize]
            .count
            .wrapping_add(1);
        self.histogram
            .update_last_bucket(value, count, 1, self.need_bucket_ndv);
    }
}
