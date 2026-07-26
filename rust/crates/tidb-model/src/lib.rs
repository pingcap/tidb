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

//! `pkg/meta/model`: TiDB schema/table metadata types.
//!
//! PACKAGE IN PROGRESS: the Go `meta/model` package is large (~9k lines
//! across many interdependent files: `job.go`, `index.go`, `column.go`,
//! `table.go`, ...). This crate is being grown bottom-up from its
//! self-contained leaf types; only the modules declared below are ported so
//! far. It is seed evidence, not yet the complete package.

pub mod flags;
pub mod masking_policy;
pub mod schema_state;
pub mod table_mode;

pub use masking_policy::{MaskingPolicyInfo, MaskingPolicyStatus};
pub use schema_state::SchemaState;
pub use table_mode::{AlterTableModeTarget, TableMode};
