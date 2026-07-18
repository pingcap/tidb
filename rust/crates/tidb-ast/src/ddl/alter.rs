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

//! Typed `ALTER TABLE` payload leaves.
//!
//! The outer [`crate::ddl::AlterTableAction`] enum is the stable statement
//! envelope.  Each payload with an independent Go parser owner lives here so
//! an agent can port its AST/restore contract without reopening the DDL root.

pub mod cache;
pub mod check;
pub mod column_default;
pub mod drop_check;
pub mod drop_foreign_key;
pub mod drop_primary_key;
pub mod index_visibility;
pub mod lock;
pub mod rename_column;
pub mod rename_index;
pub mod ttl;
