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

//! All ordinary `tidb-stats` integration tests in one process.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod analysis_policy_source;
mod analyze_jobs_source;
mod analyze_version_policy_source;
mod builder_source;
mod cmsketch_source;
mod column_source;
mod constants_source;
mod correlation_source;
mod datum_map_cache_source;
mod estimate_source;
mod existence_map_source;
mod fmsketch_source;
mod global_stats_source;
mod histogram_source;
mod index_query_source;
mod index_source;
mod memory_usage_source;
mod overlap_geometry_source;
mod pkg_statistics_go_tests_source;
mod row_estimate_source;
mod row_sample_memory_quota_source;
mod sample_bytes_source;
mod scalar_enum_source;
mod scalar_geometry_source;
mod sorted_builder_source;
mod stats_version_source;
mod status_source;
mod table_source;
