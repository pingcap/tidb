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

//! Native Rust mapping of the Go `pkg/util/slice` package.

/// Returns true when every item matches `predicate`.
///
/// Like the source's negated `slices.ContainsFunc`, this is vacuously true for
/// an empty slice and stops at the first mismatch.
pub fn all_of<T>(slice: &[T], predicate: impl FnMut(&T) -> bool) -> bool {
    slice.iter().all(predicate)
}

/// Converts signed 64-bit integers to their base-ten Go string form.
#[must_use]
pub fn int64s_to_strings(ints: &[i64]) -> Vec<String> {
    ints.iter().map(i64::to_string).collect()
}

/// Uses [`Clone::clone`] to clone every element while preserving source nil.
///
/// `None` represents a nil Go slice. `Some(&[])` represents a present empty
/// slice and therefore returns `Some(Vec::new())`, keeping those source states
/// distinct.
#[must_use]
pub fn deep_clone<T: Clone>(slice: Option<&[T]>) -> Option<Vec<T>> {
    slice.map(|items| {
        let mut cloned = Vec::with_capacity(items.len());
        cloned.extend(items.iter().cloned());
        cloned
    })
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::all_of;

    #[test]
    fn TestSlice() {
        let tests = [
            (&[][..], true),
            (&[1, 2, 3][..], false),
            (&[1, 3][..], false),
            (&[2, 2, 4][..], true),
        ];

        for (values, expected) in tests {
            assert_eq!(all_of(values, |value| value % 2 == 0), expected);
        }
    }
}
