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

//! Gap test for Go `pkg/executor/show_stats_test.go:32::TestShowStatsMeta`
//! (item 600, the single item this batch takes from that file).

/// Go `pkg/executor/show_stats_test.go:32::TestShowStatsMeta`: after
/// `analyze table t, t1 all columns`, `show stats_meta` lists one row per
/// table sorted by name with a non-NULL `update_time`, and the WHERE
/// matrix (`table_name = 't'`, `in ('t','t1')`, `db_name = 'test' and
/// table_name = 't1'`, `db_name = 'mysql'`, `or`, `and 1=1`) filters exactly
/// like a virtual table. Needs the analyze pipeline writing
/// mysql.stats_meta and the SHOW executor retriever
/// (`fetchShowStatsMeta`, pkg/executor/show_stats.go:36).
#[test]
#[ignore = "go-parity-gap: SHOW STATS_META (pkg/executor/show_stats.go:36) needs the analyze pipeline's mysql.stats_meta rows and the SHOW executor, both unported"]
fn show_stats_meta_lists_analyzed_tables_and_filters_by_where() {}
