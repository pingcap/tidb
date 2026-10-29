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
// See the License for the specific language governing permissions and
// limitations under the License.


//! Go stdlib `container/heap`, which both `topn_chunk_heap.go` and
//! `multi_way_merge.go` import.
//!
//! The sift order of Go's implementation is observable whenever the comparison
//! has ties, so it is reproduced rather than approximated. `less` takes the two
//! ELEMENTS instead of two indices, which lets a caller keep its comparison
//! state (evaluated keys, a captured comparison error) outside `data`.

/// Go `container/heap.down`. Returns whether the element moved.
pub fn down<T>(
    data: &mut [T],
    i0: usize,
    n: usize,
    less: &mut dyn FnMut(&T, &T) -> bool,
) -> bool {
    let mut i = i0;
    loop {
        let j1 = 2 * i + 1;
        if j1 >= n {
            break;
        }
        let mut j = j1;
        let j2 = j1 + 1;
        if j2 < n && less(&data[j2], &data[j1]) {
            j = j2;
        }
        if !less(&data[j], &data[i]) {
            break;
        }
        data.swap(i, j);
        i = j;
    }
    i > i0
}

/// Go `container/heap.up`.
pub fn up<T>(data: &mut [T], j0: usize, less: &mut dyn FnMut(&T, &T) -> bool) {
    let mut j = j0;
    loop {
        // Go's `i := (j - 1) / 2` on a signed int gives 0 for j == 0
        // (division truncates toward zero), which is what ends the walk at
        // the root; an unsigned `j - 1` would wrap instead.
        let i = if j == 0 { 0 } else { (j - 1) / 2 };
        if i == j || !less(&data[j], &data[i]) {
            break;
        }
        data.swap(i, j);
        j = i;
    }
}

/// Go `container/heap.Init`.
pub fn init<T>(data: &mut [T], less: &mut dyn FnMut(&T, &T) -> bool) {
    let n = data.len();
    for i in (0..n / 2).rev() {
        down(data, i, n, less);
    }
}

/// Go `container/heap.Fix`.
pub fn fix<T>(data: &mut [T], i: usize, less: &mut dyn FnMut(&T, &T) -> bool) {
    let n = data.len();
    if !down(data, i, n, less) {
        up(data, i, less);
    }
}

/// Go `container/heap.Pop`: the root is swapped to the end, the rest is
/// sifted, and the tail element is removed and returned.
pub fn pop<T>(data: &mut Vec<T>, less: &mut dyn FnMut(&T, &T) -> bool) -> Option<T> {
    let n = data.len().checked_sub(1)?;
    data.swap(0, n);
    down(data, 0, n, less);
    data.pop()
}

/// Go `container/heap.Remove`.
pub fn remove<T>(
    data: &mut Vec<T>,
    i: usize,
    less: &mut dyn FnMut(&T, &T) -> bool,
) -> Option<T> {
    let n = data.len().checked_sub(1)?;
    if n != i {
        data.swap(i, n);
        if !down(data, i, n, less) {
            up(data, i, less);
        }
    }
    data.pop()
}
