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

//! Weighted reservoir selection from `pkg/statistics/row_sampler.go`.
//!
//! The Go collector keeps the smallest weight at the root of a min heap. It
//! fills the reservoir first, heapifies exactly at capacity, and then replaces
//! the root only for a strictly larger incoming weight. This leaf owns only
//! that selection policy; random-weight generation, Datum row construction,
//! FM sketches, memory accounting, and distributed collector merging remain
//! external boundaries.

/// A caller-owned payload paired with its A-Res sampling weight.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeightedSample<T> {
    payload: T,
    weight: i64,
}

impl<T> WeightedSample<T> {
    /// Creates a weighted sample.
    #[must_use]
    pub const fn new(weight: i64, payload: T) -> Self {
        Self { payload, weight }
    }

    /// Returns the source-generated sampling weight.
    #[must_use]
    pub const fn weight(&self) -> i64 {
        self.weight
    }

    /// Returns the caller-owned payload.
    #[must_use]
    pub const fn payload(&self) -> &T {
        &self.payload
    }
}

/// A fixed-size min-heap reservoir matching `WeightedRowSampleHeap`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeightedReservoir<T> {
    max_sample_size: usize,
    samples: Vec<WeightedSample<T>>,
}

impl<T> WeightedReservoir<T> {
    /// Creates an empty reservoir. A zero-sized reservoir accepts no samples.
    #[must_use]
    pub fn new(max_sample_size: usize) -> Self {
        Self {
            max_sample_size,
            samples: Vec::with_capacity(max_sample_size),
        }
    }

    /// Inserts a weighted payload using the source's fill/replace policy.
    ///
    /// In particular, equal weights do not replace the current minimum. The
    /// vector is a min heap once it reaches capacity; its order is otherwise
    /// unspecified, just like Go's `container/heap` representation.
    pub fn consider(&mut self, weight: i64, payload: T) {
        if self.max_sample_size == 0 {
            return;
        }

        if self.samples.len() < self.max_sample_size {
            self.samples.push(WeightedSample::new(weight, payload));
            if self.samples.len() == self.max_sample_size {
                self.heapify();
            }
            return;
        }

        if self.samples[0].weight < weight {
            self.samples[0] = WeightedSample::new(weight, payload);
            self.sift_down(0);
        }
    }

    /// Returns the selected samples in heap order.
    #[must_use]
    pub fn samples(&self) -> &[WeightedSample<T>] {
        &self.samples
    }

    /// Returns the number of selected samples.
    #[must_use]
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Returns whether the reservoir has no selected samples.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    fn heapify(&mut self) {
        if self.samples.len() < 2 {
            return;
        }
        for index in (0..self.samples.len() / 2).rev() {
            self.sift_down(index);
        }
    }

    fn sift_down(&mut self, mut index: usize) {
        loop {
            let left = index * 2 + 1;
            if left >= self.samples.len() {
                return;
            }
            let right = left + 1;
            let mut child = left;
            // `container/heap` chooses the right child when `Less(left,
            // right)` is false, including equal weights. Keep that detail so
            // heap-order ties remain source-shaped even though selection is
            // governed only by the strict root comparison above.
            if right < self.samples.len() && self.samples[left].weight >= self.samples[right].weight
            {
                child = right;
            }
            if self.samples[index].weight <= self.samples[child].weight {
                return;
            }
            self.samples.swap(index, child);
            index = child;
        }
    }
}
