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

//! Source-owned `ALTER TABLE` grammar leaves.

#[path = "alter/auto_id_options.rs"]
pub(super) mod auto_id_options;
#[path = "alter/auto_increment.rs"]
pub(super) mod auto_increment;
#[path = "alter/cache.rs"]
pub(super) mod cache;
#[path = "alter/check.rs"]
pub(super) mod check;
#[path = "alter/column_default.rs"]
pub(super) mod column_default;
#[path = "alter/comment.rs"]
pub(super) mod comment;
#[path = "alter/drop_check.rs"]
pub(super) mod drop_check;
#[path = "alter/drop_foreign_key.rs"]
pub(super) mod drop_foreign_key;
#[path = "alter/drop_primary_key.rs"]
pub(super) mod drop_primary_key;
#[path = "alter/index_visibility.rs"]
pub(super) mod index_visibility;
#[path = "alter/lock.rs"]
pub(super) mod lock;
#[path = "alter/placement_policy.rs"]
pub(super) mod placement_policy;
#[path = "alter/rename_column.rs"]
pub(super) mod rename_column;
#[path = "alter/rename_index.rs"]
pub(super) mod rename_index;
#[path = "alter/repartition.rs"]
pub(super) mod repartition;
#[path = "alter/shard_row_id_bits.rs"]
pub(super) mod shard_row_id_bits;
#[path = "alter/ttl.rs"]
pub(super) mod ttl;
