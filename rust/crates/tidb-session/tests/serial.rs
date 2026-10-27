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

//! The `tidb-session` tests that mutate process-wide state -- the global
//! config (Go `config.UpdateGlobal`) or a sysvar Go keeps in a process atomic
//! -- in a process of their own, so the topology-independent `all` suite
//! never observes them. Each holds `support::testkit`'s serial lock (taken by
//! `update_global_config` or `serial()`) while it runs.

#[allow(dead_code)]
mod support;
mod check_constraint_write_source;
mod serial_column_flags_and_limits_source;
mod serial_db_integration_config_source;
mod serial_multi_schema_change_config_source;
