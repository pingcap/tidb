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

//! Go `pkg/table` and `pkg/table/tables`: a table over the store -- its row
//! and index encoding, column casts and defaults, generated columns,
//! partition routing and auto ids -- below the executor, which reads and
//! writes through it, and the DDL package, which builds it from a
//! `TableInfo` (`TableFromMeta`).
pub mod handle_range;
pub mod hypo_index;
pub mod zero_date;
pub mod column_default;
pub mod context;
pub mod bad_null;
pub mod column;
pub mod generated_column;
pub mod partition_fold;
pub mod partition_metadata;
pub mod table_from_meta;
pub mod partition_routing;
pub mod kv_table;
pub mod sequence;
pub mod origin_default;
