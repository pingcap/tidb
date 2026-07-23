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

//! Complete transcreation of `pkg/util/mathutil`.
//!
//! `math.go`, `rand.go`, and `exponential_average.go` map one-for-one to the
//! sibling Rust modules. Their three source test files are kept beside the
//! owning implementations with every original test name and table.
//! `main_test.go` only installs TiDB's shared Go test setup and leak verifier;
//! Rust's scoped tests create no background worker and need no process-global
//! setup or leak-ignore list. `BUILD.bazel` maps to `tidb-util`'s Cargo
//! manifest. The package has no benchmarks, fuzz targets, examples, fixtures,
//! generated files, or build-tag variants.

mod exponential_average;
mod math;
mod rand;

pub use exponential_average::ExponentialMovingAverage;
pub use math::{
    abs, clamp, divide_2_batches, is_finite, next_power_of_two, str_len_of_int64_fast,
    str_len_of_uint64_fast, INT_BITS, MAX_INT, MAX_UINT, MIN_INT,
};
pub use rand::MysqlRng;
