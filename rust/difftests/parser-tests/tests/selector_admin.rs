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

//! Stable Cargo shard for independently owned ADMIN parser selectors.
//!
//! Each module remains one source-owned selector and test. This entrypoint is
//! deliberately the only Cargo integration binary for this family.

#[path = "selectors/admin/admin_alter_ddl_jobs_selector.rs"]
mod admin_alter_ddl_jobs;
#[path = "selectors/admin/admin_bdr.rs"]
mod admin_bdr;
#[path = "selectors/admin/admin_checksum.rs"]
mod admin_checksum;
#[path = "selectors/admin/admin_cleanup_table_lock_selector.rs"]
mod admin_cleanup_table_lock;
#[path = "selectors/admin/admin_ddl_job_control_selector.rs"]
mod admin_ddl_job_control;
#[path = "selectors/admin/admin_flush_plan_cache_selector.rs"]
mod admin_flush_plan_cache;
#[path = "selectors/admin/admin_recover_index.rs"]
mod admin_recover_index;
#[path = "selectors/admin/admin_reload.rs"]
mod admin_reload;
#[path = "selectors/admin/admin_show_bdr_selector.rs"]
mod admin_show_bdr;
#[path = "selectors/admin/admin_show_ddl_selector.rs"]
mod admin_show_ddl;
#[path = "selectors/admin/admin_show_ddl_job_queries_selector.rs"]
mod admin_show_ddl_job_queries;
#[path = "selectors/admin/admin_show_ddl_jobs_selector.rs"]
mod admin_show_ddl_jobs;
#[path = "selectors/admin/admin_show_next_row_id_selector.rs"]
mod admin_show_next_row_id;
#[path = "selectors/admin/admin_show_slow_selector.rs"]
mod admin_show_slow;
