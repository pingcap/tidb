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

//! All `tidb-codec` integration tests in one process.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod bytes_float_go_vectors;
mod bytes_source;
mod codec_package_source;
mod collation_keys;
mod column_source;
mod decimal_column_shape_go_vectors;
mod decimal_fixed_source;
mod decimal_go_vectors;
mod default_datum_source;
mod duration_source;
mod json_source;
mod number_boundaries;
mod row_decoder_source;
mod row_encoder_source;
mod row_index_source;
mod row_layout_source;
mod rowcodec_package_source;
mod rowv2_go_vectors;
mod runtime_collation_mode_source;
mod temporal_source;
mod typed_column_source;
mod unsigned_decimal_key_order;
mod value_codec_source;
