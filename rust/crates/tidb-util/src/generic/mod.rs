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

//! Complete transcreation of Go `pkg/util/generic`.
//!
//! The package provides a concurrent map and a fixed-capacity best-N heap.

mod bounded_min_heap;
mod sync_map;

pub use bounded_min_heap::BoundedMinHeap;
pub use sync_map::SyncMap;

#[cfg(test)]
mod tests {
    use super::{BoundedMinHeap, SyncMap};

    #[test]
    #[deny(unused_must_use)]
    fn return_values_may_be_ignored_like_go() {
        fn cmp(a: &i32, b: &i32) -> isize {
            (*a > *b) as isize - (*a < *b) as isize
        }

        let heap = BoundedMinHeap::new(1, Some(cmp));
        BoundedMinHeap::new(1, Some(cmp));
        heap.len();
        heap.to_sorted_slice();

        let map = SyncMap::<i32, i32>::new(1);
        SyncMap::<i32, i32>::new(1);
        map.load(&0);
        map.keys();
    }
}
