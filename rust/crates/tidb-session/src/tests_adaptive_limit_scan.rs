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

//! Go master `pkg/executor/distsql_test.go`'s adaptive LIMIT scan tests
//! (0b505ecc58b, 33bdc22d43d, cd9601b3f2f). Go's `CheckSelectRequestHook`
//! half of `TestAdaptiveLimitExecution` (the copr request concurrency stays
//! the baseline's) has no Rust request hook and is not ported.

use crate::testkit::TestKit;

/// Go `fmt.Sprint(tk.MustQuery(sql).Rows())`.
fn rows_text(tk: &mut TestKit, sql: &str) -> String {
    tk.must_query(sql)
        .rows()
        .iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
        .join("\n")
}

fn rows_of(tk: &mut TestKit, sql: &str) -> Vec<String> {
    tk.must_query(sql).rows().iter().map(|row| row.join(" ")).collect()
}

fn sorted_rows_of(tk: &mut TestKit, sql: &str) -> Vec<String> {
    let mut rows = rows_of(tk, sql);
    rows.sort();
    rows
}

/// Go's `fmt.Sscanf(text[start:], "adaptive:{outer:%d/%d, lookup:%d/%d,
/// outstanding:%d/%d, blocked:")`.
fn join_adaptive_counters(analyze: &str) -> [u64; 6] {
    let start = analyze.find("adaptive:{outer:").unwrap_or_else(|| panic!("no adaptive stats: {analyze}"));
    let text = &analyze[start + "adaptive:{outer:".len()..];
    let mut numbers = [0u64; 6];
    let mut rest = text;
    for (i, separator) in ["/", ", lookup:", "/", ", outstanding:", "/", ", blocked:"].iter().enumerate() {
        let end = rest.find(separator).unwrap_or_else(|| panic!("malformed adaptive stats: {text}"));
        numbers[i] = rest[..end].parse().unwrap_or_else(|_| panic!("malformed adaptive stats: {text}"));
        rest = &rest[end + separator.len()..];
    }
    numbers
}

/// Go `TestAdaptiveLimitExecution`.
#[test]
fn test_adaptive_limit_execution() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "create table adaptive_outer (
		id int primary key, order_key int not null, join_key int not null,
		filter_col int not null, payload varchar(32), key idx_order_key(order_key))",
    );
    tk.must_exec(
        "create table adaptive_inner (
		id int primary key, join_key int not null, v int not null,
		key idx_join_key(join_key))",
    );
    let outer_values: Vec<String> = (1..=128)
        .map(|i| {
            let filter_col = i32::from(matches!(i, 1 | 3 | 6 | 9 | 12) || i % 16 == 0);
            format!("({i},{i},{i},{filter_col},'payload_{i}')")
        })
        .collect();
    tk.must_exec(&format!("insert into adaptive_outer values {}", outer_values.join(",")));
    let mut inner_values: Vec<String> = (1..=64).map(|i| format!("({i},1,{i})")).collect();
    inner_values.extend(["(65,3,3)", "(66,6,6)", "(67,9,9)", "(68,12,12)"].map(str::to_owned));
    inner_values.extend((16..=128).step_by(16).map(|i| format!("({},{i},{i})", 1000 + i)));
    tk.must_exec(&format!("insert into adaptive_inner values {}", inner_values.join(",")));
    tk.must_query("select @@tidb_enable_adaptive_limit_scan").check(["1"]);
    for sql in [
        "set tidb_enable_adaptive_limit_scan = on",
        "set tidb_index_join_batch_size = 32",
        "set tidb_index_lookup_size = 32",
        "set tidb_index_lookup_join_concurrency = 2",
        "set tidb_index_lookup_concurrency = 2",
        "set tidb_init_chunk_size = 32",
        "set tidb_max_chunk_size = 32",
    ] {
        tk.must_exec(sql);
    }

    let sql = "select /*+ inl_join(i) */ o.payload, i.v
		from adaptive_outer o use index(idx_order_key)
		join adaptive_inner i use index(idx_join_key) on o.join_key = i.join_key
		where o.order_key between 1 and 4 and o.filter_col > 0
		order by o.order_key limit 40";
    let plan = rows_text(&mut tk, &format!("explain {sql}"));
    assert!(plan.contains("IndexJoin"), "{plan}");
    assert!(plan.contains("keep order:true"), "{plan}");
    let on_rows = sorted_rows_of(&mut tk, sql);
    assert_eq!(on_rows.len(), 40);

    let analyze = rows_text(&mut tk, &format!("explain analyze {sql}"));
    for part in ["adaptive:{", "outer:", "lookup:", "outstanding:", "blocked:"] {
        assert!(analyze.contains(part), "{part}: {analyze}");
    }
    let budget_sql = "select /*+ inl_join(i) */ o.payload, i.v
		from adaptive_outer o use index(idx_order_key)
		join adaptive_inner i use index(idx_join_key) on o.join_key = i.join_key
		where o.order_key between 1 and 128 and o.filter_col >= 0
		order by o.order_key limit 4";
    let budget_analyze = rows_text(&mut tk, &format!("explain analyze {budget_sql}"));
    tk.must_exec("set tidb_enable_adaptive_limit_scan = off");
    let baseline_analyze = rows_text(&mut tk, &format!("explain analyze {budget_sql}"));
    assert!(!baseline_analyze.contains("adaptive:{"), "{baseline_analyze}");
    tk.must_exec("set tidb_enable_adaptive_limit_scan = on");
    let stats = &budget_analyze[budget_analyze.find("adaptive:{outer:").expect("adaptive stats")..];
    assert!(stats.contains(", blocked:outer=") && stats.contains(",lookup="), "{stats}");
    let [outer_fetched, outer_consumed, lookup_handles, lookup_rows, outer_outstanding, lookup_outstanding] =
        join_adaptive_counters(&budget_analyze);
    assert!(outer_fetched <= 4, "{stats}");
    assert!(lookup_handles <= 4, "{stats}");
    assert!(outer_consumed <= outer_fetched, "{stats}");
    assert!(lookup_rows <= lookup_handles, "{stats}");
    assert!(outer_outstanding <= 4, "{stats}");
    assert!(lookup_outstanding <= 4, "{stats}");

    let low_selectivity_sql = "select /*+ inl_join(i) */ o.order_key, i.v
		from adaptive_outer o use index(idx_order_key)
		join adaptive_inner i use index(idx_join_key) on o.join_key = i.join_key
		where o.order_key between 13 and 128 and o.filter_col = 1
		order by o.order_key limit 4";
    let low_selectivity_plan = rows_text(&mut tk, &format!("explain {low_selectivity_sql}"));
    for part in ["IndexLookUp", "Selection", "TableRowIDScan"] {
        assert!(low_selectivity_plan.contains(part), "{part}: {low_selectivity_plan}");
    }
    let low_selectivity_on_rows = rows_of(&mut tk, low_selectivity_sql);
    assert_eq!(low_selectivity_on_rows, ["16 16", "32 32", "48 48", "64 64"]);
    let low_selectivity_analyze = rows_text(&mut tk, &format!("explain analyze {low_selectivity_sql}"));
    let small_limit_sql = low_selectivity_sql.replacen("limit 4", "limit 1", 1);
    let small_limit_on_rows = rows_of(&mut tk, &small_limit_sql);
    assert_eq!(small_limit_on_rows, ["16 16"]);
    let small_limit_analyze = rows_text(&mut tk, &format!("explain analyze {small_limit_sql}"));
    let table_task_nums: Vec<usize> = small_limit_analyze
        .match_indices("table_task: {")
        .filter_map(|(at, _)| {
            let text = &small_limit_analyze[at..];
            let num = &text[text.find("num: ")? + "num: ".len()..];
            num[..num.find(|c: char| !c.is_ascii_digit())?].parse().ok()
        })
        .collect();
    assert!(!table_task_nums.is_empty(), "{small_limit_analyze}");
    assert_eq!(table_task_nums.iter().copied().max(), Some(1), "{small_limit_analyze}");
    let [outer_fetched, outer_consumed, lookup_handles, lookup_rows, _, _] =
        join_adaptive_counters(&low_selectivity_analyze);
    assert!(lookup_handles > lookup_rows, "{low_selectivity_analyze}");
    assert!(lookup_rows >= 4, "{low_selectivity_analyze}");
    assert!(outer_consumed <= outer_fetched, "{low_selectivity_analyze}");
    let ordered_sql = "select /*+ inl_join(i) */ o.order_key, i.v
		from adaptive_outer o use index(idx_order_key)
		join adaptive_inner i use index(idx_join_key) on o.join_key = i.join_key
		where o.order_key between 2 and 12 and o.filter_col > 0
		order by o.order_key limit 4";
    let ordered_on_rows = rows_of(&mut tk, ordered_sql);
    assert_eq!(ordered_on_rows, ["3 3", "6 6", "9 9", "12 12"]);

    tk.must_exec("set tidb_enable_adaptive_limit_scan = off");
    assert_eq!(sorted_rows_of(&mut tk, sql), on_rows);
    assert_eq!(rows_of(&mut tk, low_selectivity_sql), low_selectivity_on_rows);
    assert_eq!(rows_of(&mut tk, &small_limit_sql), small_limit_on_rows);
    assert_eq!(rows_of(&mut tk, ordered_sql), ordered_on_rows);
    assert!(!rows_text(&mut tk, &format!("explain analyze {sql}")).contains("adaptive:{"));
}

/// Go `TestAdaptiveLimitSingleReadIndexJoin`.
#[test]
fn test_adaptive_limit_single_read_index_join() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "create table adaptive_single_outer (
		id bigint primary key clustered, ord int not null, join_key bigint not null,
		key idx_ord(ord, id, join_key))",
    );
    tk.must_exec("create table adaptive_single_inner (id bigint primary key clustered, v int not null)");
    tk.must_exec(
        "insert into adaptive_single_outer values (1,30,101),(2,10,102),(3,50,103),(4,20,104),(5,40,105),(6,60,106)",
    );
    tk.must_exec("insert into adaptive_single_inner values (101,1),(102,2),(104,4),(105,5),(106,6)");

    for (name, access, filter, order, limit, reader, scan, rows) in [
        ("table", "primary", "o.id between 1 and 100", "o.id", "3", "TableReader", "TableRangeScan", vec!["1", "2", "4"]),
        ("index", "idx_ord", "o.ord between 10 and 60", "o.ord, o.id", "3", "IndexReader", "IndexRangeScan", vec!["2", "4", "1"]),
        (
            "index-desc-offset",
            "idx_ord",
            "o.ord between 10 and 60",
            "o.ord desc, o.id desc",
            "2 offset 1",
            "IndexReader",
            "IndexRangeScan",
            vec!["5", "1"],
        ),
    ] {
        let mut tk = tk.new_tk();
        tk.must_exec("use test");
        tk.must_exec("set tidb_index_lookup_concurrency = 1");
        let sql = format!(
            "select /*+ inl_join(i) */ o.id
				from adaptive_single_outer o use index({access}) join adaptive_single_inner i on i.id = o.join_key
				where {filter} order by {order} limit {limit}"
        );
        let plan = rows_text(&mut tk, &format!("explain format = 'brief' {sql}"));
        assert!(plan.contains("IndexJoin"), "{name}: {plan}");
        assert!(plan.contains(&format!("{reader}(Build)")), "{name}: {plan}");
        assert!(
            plan.lines().any(|line| line.contains(scan) && line.contains("keep order:true")),
            "{name}: {plan}"
        );

        tk.must_exec("set tidb_enable_adaptive_limit_scan = off");
        tk.must_query(&sql).check(&rows);
        assert!(!rows_text(&mut tk, &format!("explain analyze {sql}")).contains("adaptive:{"), "{name}");
        tk.must_exec("set tidb_enable_adaptive_limit_scan = on");
        tk.must_query(&sql).check(&rows);
        let analyze = rows_text(&mut tk, &format!("explain analyze {sql}"));
        let stats = &analyze[analyze.find("adaptive:{outer:").unwrap_or_else(|| panic!("{name}: {analyze}"))..];
        assert!(stats.contains(", outstanding:") && stats.contains(", blocked:") && !stats[..stats.find('}').unwrap()].contains("lookup:"), "{name}: {stats}");
    }

    // one empty outer batch
    tk.must_exec("set tidb_enable_adaptive_limit_scan = on");
    let sql = "select /*+ inl_join(i) */ o.id from adaptive_single_outer o use index(primary)
			join adaptive_single_inner i on i.id = o.join_key where o.id between 3 and 5 order by o.id limit 1";
    tk.must_query(sql).check(["4"]);
    let analyze = rows_text(&mut tk, &format!("explain analyze {sql}"));
    assert!(analyze.contains("adaptive:{outer:2/2, outstanding:0,"), "{analyze}");

    let outer_values: Vec<String> = (7..=128).map(|id| format!("({id},{},{})", id + 100, id + 100)).collect();
    tk.must_exec(&format!("insert into adaptive_single_outer values {}", outer_values.join(",")));
    tk.must_exec("insert into adaptive_single_inner values (228,128)");
    let sparse = "select /*+ inl_join(i) */ o.id from adaptive_single_outer o use index(primary)
		join adaptive_single_inner i on i.id = o.join_key where o.id between 7 and 128 order by o.id limit 1";
    tk.must_exec(&format!("prepare single_read from '{sparse}'"));
    for state in ["on", "off", "on"] {
        tk.must_exec(&format!("set tidb_enable_adaptive_limit_scan = {state}"));
        tk.must_query("execute single_read").check(["128"]);
    }
    tk.must_query(&sparse.replacen("7 and 128", "7 and 127", 1)).check_empty();
    assert!(rows_text(&mut tk, &format!("explain analyze {sparse}")).contains("adaptive:{outer:"));
    let unordered = sparse.replacen("order by o.id ", "", 1);
    assert!(!rows_text(&mut tk, &format!("explain analyze {unordered}")).contains("adaptive:{"));

    tk.must_exec("create table adaptive_single_multi (id int primary key, join_key bigint, key idx_join_key(join_key))");
    tk.must_exec("insert into adaptive_single_multi values (1,101),(2,101),(3,102)");
    let one_to_many = "select /*+ inl_join(i) */ o.id from adaptive_single_outer o use index(primary)
		join adaptive_single_multi i use index(idx_join_key) on i.join_key = o.join_key
		where o.id between 1 and 3 order by o.id limit 2 offset 1";
    tk.must_query(one_to_many).check(["1", "2"]);
    assert!(rows_text(&mut tk, &format!("explain analyze {one_to_many}")).contains("adaptive:{outer:"));

    let left_join = "select /*+ inl_join(i) */ o.id from adaptive_single_outer o use index(primary)
		left join adaptive_single_multi i use index(idx_join_key) on i.join_key = o.join_key
		where o.id between 1 and 3 order by o.id limit 2 offset 2";
    tk.must_query(left_join).check(["2", "3"]);
    let left_plan = rows_text(&mut tk, &format!("explain format = 'brief' {left_join}"));
    assert!(left_plan.contains("Limit(Build)"), "{left_plan}");
    assert!(!rows_text(&mut tk, &format!("explain analyze {left_join}")).contains("adaptive:{"));
}

/// Go `TestAdaptiveLimitDirectIndexLookUpExecution`.
#[test]
fn test_adaptive_limit_direct_index_look_up_execution() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "create table adaptive_direct_lookup (
		id int primary key, order_key int not null, filter_col int not null,
		payload varchar(32), key idx_order_key(order_key))",
    );
    let values: Vec<String> = (1..=128)
        .map(|i| {
            let filter_col = i32::from(matches!(i, 1 | 3 | 6 | 9 | 12) || i % 16 == 0);
            format!("({i},{i},{filter_col},'payload_{i}')")
        })
        .collect();
    tk.must_exec(&format!("insert into adaptive_direct_lookup values {}", values.join(",")));
    for sql in [
        "set tidb_index_lookup_size = 32",
        "set tidb_index_lookup_concurrency = 2",
        "set tidb_init_chunk_size = 32",
        "set tidb_max_chunk_size = 32",
    ] {
        tk.must_exec(sql);
    }

    let sql = "select order_key, payload
		from adaptive_direct_lookup use index(idx_order_key)
		where order_key between 1 and 128 and filter_col = 1
		order by order_key limit 4";
    let plan = rows_text(&mut tk, &format!("explain {sql}"));
    for part in ["IndexLookUp", "Selection", "keep order:true"] {
        assert!(plan.contains(part), "{part}: {plan}");
    }

    tk.must_exec("set tidb_enable_adaptive_limit_scan = on");
    let on_rows = rows_of(&mut tk, sql);
    assert_eq!(on_rows, ["1 payload_1", "3 payload_3", "6 payload_6", "9 payload_9"]);
    let on_analyze = rows_text(&mut tk, &format!("explain analyze {sql}"));
    assert!(on_analyze.contains("adaptive:{lookup:"), "{on_analyze}");
    assert!(!on_analyze.contains("outer:"), "{on_analyze}");
    let stats = &on_analyze[on_analyze.find("adaptive:{lookup:").unwrap()..];
    assert!(stats.contains(", outstanding:") && stats.contains(", blocked:"), "{stats}");

    let paging_sql = "select order_key, payload
		from adaptive_direct_lookup use index(idx_order_key)
		where order_key between 1 and 128 and filter_col >= 0
		order by order_key limit 1";
    let paging_plan = rows_text(&mut tk, &format!("explain {paging_sql}"));
    for part in ["IndexLookUp", "Selection", "keep order:true"] {
        assert!(paging_plan.contains(part), "{part}: {paging_plan}");
    }
    assert_eq!(rows_of(&mut tk, paging_sql), ["1 payload_1"]);
    let paging_analyze = rows_text(&mut tk, &format!("explain analyze {paging_sql}"));
    assert!(paging_analyze.contains("adaptive:{lookup:1/1, outstanding:0,"), "{paging_analyze}");

    let offset_sql = sql.replacen("limit 4", "limit 2, 2", 1);
    assert_eq!(rows_of(&mut tk, &offset_sql), ["6 payload_6", "9 payload_9"]);
    let empty_sql = sql.replacen("filter_col = 1", "filter_col = 2", 1);
    tk.must_query(&empty_sql).check_empty();
    assert!(rows_text(&mut tk, &format!("explain analyze {empty_sql}")).contains("adaptive:{lookup:"));
    let large_limit_sql = sql.replacen("limit 4", "limit 100", 1);
    let large_limit_rows = rows_of(&mut tk, &large_limit_sql);
    assert_eq!(large_limit_rows.len(), 13);

    tk.must_exec("set tidb_enable_adaptive_limit_scan = off");
    assert_eq!(rows_of(&mut tk, sql), on_rows);
    assert_eq!(rows_of(&mut tk, paging_sql), ["1 payload_1"]);
    assert_eq!(rows_of(&mut tk, &offset_sql), ["6 payload_6", "9 payload_9"]);
    tk.must_query(&empty_sql).check_empty();
    assert_eq!(rows_of(&mut tk, &large_limit_sql), large_limit_rows);
    assert!(!rows_text(&mut tk, &format!("explain analyze {sql}")).contains("adaptive:{"));
}

/// Go `TestAdaptiveLimitScanClusterDefaults`' new-cluster half: bootstrap
/// stores ON (Go `GlobalSystemVariableInitialValue`). Its upgrade half
/// (`upgradeToVer317` keeps an upgraded cluster OFF) has no Rust upgrade
/// step to land on: bootstrap upgrade steps stop at version 263.
#[test]
fn test_adaptive_limit_scan_cluster_defaults() {
    let mut tk = TestKit::new();
    tk.must_query(
        "select variable_value, @@global.tidb_enable_adaptive_limit_scan, @@session.tidb_enable_adaptive_limit_scan \
         from mysql.GLOBAL_VARIABLES where variable_name='tidb_enable_adaptive_limit_scan'",
    )
    .check(["ON 1 1"]);
}
