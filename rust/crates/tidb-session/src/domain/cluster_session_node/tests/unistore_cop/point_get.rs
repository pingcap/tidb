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


//! Point and batch-point reads and the prepared plan cache, as the executor's
//! in-memory tier used to check them, over the cop-backed TestKit stack
//! (X11): the shapes Go plans as Point_Get / Batch_Point_Get, constants keyed
//! in the column's domain, out-of-range handles, fast writes, and prepared
//! executions that hit the cache and rebind their parameters. The fix-control
//! 52592 case is Go's `TestIssue52592` in `pointget.rs`.

use super::sql_kit::*;
use super::*;

/// Executes prepared `name` with `params` (user-variable assignments) and
/// answers its displayed rows and whether the plan came from the cache.
fn execute(tk: &mut Session, name: &str, params: &[&str]) -> (Vec<Vec<String>>, bool) {
    let mut using = Vec::new();
    for (position, value) in params.iter().enumerate() {
        rows(tk, &format!("SET @p{position} = {value}"));
        using.push(format!("@p{position}"));
    }
    let sql = if using.is_empty() { format!("EXECUTE {name}") } else { format!("EXECUTE {name} USING {}", using.join(", ")) };
    let answered = q(tk, &sql);
    let hit = q(tk, "SELECT @@last_plan_from_cache")[0][0] == "1";
    (answered, hit)
}

#[test]
fn cached_physical_plan_rebuilds_and_executes_the_retained_tree() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 780);
    exec_all(&mut tk, &[
        "CREATE TABLE cache_no_replan (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)",
        "INSERT INTO cache_no_replan VALUES (1,10),(2,20),(3,30)",
        "PREPARE st FROM 'SELECT value FROM cache_no_replan WHERE id BETWEEN ? AND ? ORDER BY id'",
    ]);
    assert_eq!(execute(&mut tk, "st", &["1", "2"]), (vec![vec!["10".to_owned()], vec!["20".to_owned()]], false));
    assert_eq!(execute(&mut tk, "st", &["2", "3"]), (vec![vec!["20".to_owned()], vec!["30".to_owned()]], true));
}

#[test]
fn prepared_in_predicate_caches_and_rebinds_analyzed_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 781);
    exec_all(&mut tk, &[
        "CREATE TABLE prepared_in_stats (id BIGINT PRIMARY KEY, k BIGINT, v BIGINT, KEY k_idx(k))",
        "INSERT INTO prepared_in_stats VALUES (1,1,11),(2,2,22),(3,3,33),(4,4,44),(5,5,55),(6,6,66),(7,7,77),(8,8,88),(9,9,99),(10,10,110)",
        "ANALYZE TABLE prepared_in_stats",
        "PREPARE st FROM 'SELECT id, v FROM prepared_in_stats WHERE k IN (?,?,?,?,?,?,?,?,?,?)'",
    ]);
    let first = (1..=10).map(|v| v.to_string()).collect::<Vec<_>>();
    let (mut got, hit) = execute(&mut tk, "st", &first.iter().map(String::as_str).collect::<Vec<_>>());
    got.sort_by_key(|row| row[0].parse::<i64>().unwrap());
    assert!(!hit);
    assert_eq!(got, (1..=10).map(|id| vec![id.to_string(), (id * 11).to_string()]).collect::<Vec<_>>());
    let second = (5..=14).map(|v| v.to_string()).collect::<Vec<_>>();
    let (mut got, hit) = execute(&mut tk, "st", &second.iter().map(String::as_str).collect::<Vec<_>>());
    got.sort_by_key(|row| row[0].parse::<i64>().unwrap());
    assert!(hit);
    assert_eq!(got, (5..=10).map(|id| vec![id.to_string(), (id * 11).to_string()]).collect::<Vec<_>>());
}

#[test]
fn cached_composite_handle_range_rebuilds_every_tuple_bound() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 782);
    exec_all(&mut tk, &[
        "CREATE TABLE cache_tuple_range (a BIGINT NOT NULL, b BIGINT NOT NULL, payload BIGINT NOT NULL, PRIMARY KEY (a, b) NONCLUSTERED)",
        "INSERT INTO cache_tuple_range VALUES (1,-1,10),(2,-1,20),(3,-1,30)",
        "PREPARE st FROM 'SELECT a, b FROM cache_tuple_range WHERE (a, b) > (?, ?) ORDER BY a, b LIMIT 2'",
    ]);
    assert_eq!(q(&mut tk, "SELECT a, b FROM cache_tuple_range WHERE (a, b) > (2, -1) ORDER BY a, b LIMIT 2"), [["3", "-1"]]);
    assert_eq!(execute(&mut tk, "st", &["0", "0"]), (vec![vec!["1".to_owned(), "-1".to_owned()], vec!["2".to_owned(), "-1".to_owned()]], false));
    assert_eq!(execute(&mut tk, "st", &["2", "-1"]), (vec![vec!["3".to_owned(), "-1".to_owned()]], true));
}

/// A prepared execution answers what the same statement with literals
/// answers, from the cache on its second execution.
fn assert_prepared_matches(tk: &mut Session, prepared: &str, executions: &[(&[&str], &str)]) {
    rows(tk, &format!("PREPARE st FROM '{}'", prepared.replace('\'', "''")));
    for (index, (params, ordinary)) in executions.iter().enumerate() {
        let expected = q(tk, ordinary);
        let (got, hit) = execute(tk, "st", params);
        assert_eq!(got, expected, "{prepared} with {params:?}");
        assert_eq!(hit, index != 0, "{prepared} with {params:?}");
    }
}

#[test]
fn cached_physical_index_readers_build_without_legacy_planner() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 783);
    exec_all(&mut tk, &[
        "CREATE TABLE cache_index_reader (id BIGINT PRIMARY KEY, c BIGINT NOT NULL, payload BIGINT NOT NULL, INDEX c_idx(c))",
        "INSERT INTO cache_index_reader VALUES (1,10,100),(2,20,200),(3,30,300),(4,40,400)",
    ]);
    for projection in ["c, id", "payload"] {
        assert_prepared_matches(
            &mut tk,
            &format!("SELECT {projection} FROM cache_index_reader FORCE INDEX (c_idx) WHERE c BETWEEN ? AND ? ORDER BY c, id"),
            &[
                (&["20", "40"], &format!("SELECT {projection} FROM cache_index_reader FORCE INDEX (c_idx) WHERE c BETWEEN 20 AND 40 ORDER BY c, id")),
                (&["10", "20"], &format!("SELECT {projection} FROM cache_index_reader FORCE INDEX (c_idx) WHERE c BETWEEN 10 AND 20 ORDER BY c, id")),
            ],
        );
    }
}

#[test]
fn prepared_select_plan_reuses_shape_and_rebinds_parameters() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 784);
    exec_all(&mut tk, &[
        "CREATE TABLE range_cache (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, c CHAR(8) NOT NULL)",
        "INSERT INTO range_cache VALUES (1,10,'b'),(2,20,'a'),(3,30,'b'),(4,40,'c')",
        "CREATE TABLE range_cache_dim (id BIGINT PRIMARY KEY, label CHAR(8) NOT NULL)",
        "INSERT INTO range_cache_dim VALUES (1,'one'),(2,'two'),(3,'three'),(4,'four')",
    ]);
    for shape in [
        "SELECT c FROM range_cache WHERE id BETWEEN {a} AND {b}",
        "SELECT c FROM range_cache WHERE id BETWEEN {a} AND {b} ORDER BY c",
        "SELECT DISTINCT c FROM range_cache WHERE id BETWEEN {a} AND {b} ORDER BY c",
        "SELECT SUM(k) FROM range_cache WHERE id BETWEEN {a} AND {b}",
        "SELECT c, SUM(k) FROM range_cache WHERE id BETWEEN {a} AND {b} GROUP BY c ORDER BY c",
        "SELECT r.id, d.label FROM range_cache r JOIN range_cache_dim d ON r.id = d.id WHERE r.id BETWEEN {a} AND {b} ORDER BY r.id",
    ] {
        let bind = |a: &str, b: &str| shape.replace("{a}", a).replace("{b}", b);
        assert_prepared_matches(&mut tk, &bind("?", "?"), &[(&["2", "3"], &bind("2", "3")), (&["1", "4"], &bind("1", "4"))]);
    }
    assert_prepared_matches(
        &mut tk,
        "SELECT id FROM range_cache WHERE id BETWEEN ? AND ? AND c = ? ORDER BY id",
        &[
            (&["1", "4", "'b'"], "SELECT id FROM range_cache WHERE id BETWEEN 1 AND 4 AND c = 'b' ORDER BY id"),
            (&["1", "4", "'c'"], "SELECT id FROM range_cache WHERE id BETWEEN 1 AND 4 AND c = 'c' ORDER BY id"),
        ],
    );
    rows(&mut tk, "PREPARE lim FROM 'SELECT id FROM range_cache ORDER BY id LIMIT ?'");
    for (value, hit) in [("2", false), ("3", false), ("2", true)] {
        assert_eq!(execute(&mut tk, "lim", &[value]).1, hit, "LIMIT {value}");
    }
    for _ in 0..2 {
        assert!(!execute(&mut tk, "lim", &["10001"]).1, "a LIMIT above 10000 is never cached");
    }
    rows(&mut tk, "SET sql_mode = 'ANSI_QUOTES'");
    assert!(!execute(&mut tk, "lim", &["2"]).1);
    assert!(execute(&mut tk, "lim", &["2"]).1);
}

/// Go plans Point_Get only for a full key equality with no residual on the
/// handle path, no ORDER BY, and no LIMIT that could drop the row.
#[test]
fn point_get_is_chosen_only_for_the_shapes_go_accepts() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 785);
    exec_all(&mut tk, &["CREATE TABLE d (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)", "INSERT INTO d VALUES (1, 'a', 10)"]);
    for (sql, point) in [
        ("SELECT v FROM d WHERE id = 1", Some("handle:1")),
        ("SELECT v FROM d WHERE 1 = id", Some("handle:1")),
        ("SELECT v FROM d WHERE code = 'a'", Some("index:code(code)")),
        ("SELECT v FROM d WHERE id = 7", Some("handle:7")),
        ("SELECT v FROM d WHERE code = 'z'", Some("index:code(code)")),
        ("SELECT v FROM d WHERE code = 'a' AND v = 10", Some("index:code(code)")),
        ("SELECT v FROM d WHERE v = 10", None),
        ("SELECT v FROM d WHERE id > 1", None),
        ("SELECT v FROM d", None),
    ] {
        let plan = plan_lines(&mut tk, sql);
        let has_point = plan.iter().any(|line| line.contains("Point_Get") && !line.contains("Batch_Point_Get"));
        assert_eq!(has_point, point.is_some(), "{sql}: {plan:?}");
        if let Some(target) = point {
            assert!(plan.iter().any(|line| line.contains("Point_Get") && line.contains(target)), "{sql}: {plan:?}");
        }
    }
}

#[test]
fn prepared_point_plan_binds_common_handle_without_cloning_template() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 786);
    exec_all(&mut tk, &[
        "CREATE TABLE prepared_y (id VARCHAR(64) PRIMARY KEY, v VARCHAR(32))",
        "INSERT INTO prepared_y VALUES ('user-0001', 'value-1')",
        "PREPARE st FROM 'SELECT * FROM prepared_y WHERE id = ?'",
    ]);
    assert_eq!(execute(&mut tk, "st", &["'user-0001'"]).0, [["user-0001", "value-1"]]);
    assert_eq!(execute(&mut tk, "st", &["'user-0001'"]), (vec![vec!["user-0001".to_owned(), "value-1".to_owned()]], true));
}

#[test]
fn prepared_point_cache_answers_a_unique_secondary_index() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 787);
    exec_all(&mut tk, &[
        "CREATE TABLE idx_pin (id VARCHAR(8) PRIMARY KEY, a VARCHAR(8) NOT NULL COLLATE utf8mb4_bin, b VARCHAR(8) NOT NULL COLLATE utf8mb4_bin, v BIGINT NOT NULL, UNIQUE INDEX ia (a, b))",
        "INSERT INTO idx_pin VALUES ('k1','a1','b1',10),('k2','a1','b2',20),('k3','a2','b1',30)",
        "PREPARE st FROM 'SELECT id, v FROM idx_pin WHERE a = ? AND b = ? AND v = ?'",
    ]);
    let plan = plan_lines(&mut tk, "SELECT id, v FROM idx_pin WHERE a = 'a1' AND b = 'b2' AND v = 20");
    assert!(plan.iter().any(|line| line.contains("Point_Get") && line.contains("index:ia(a, b)")), "{plan:?}");
    assert_eq!(execute(&mut tk, "st", &["'a1'", "'b2'", "20"]).0, [["k2", "20"]]);
    assert!(execute(&mut tk, "st", &["'a1'", "'b1'", "999"]).0.is_empty());
    assert!(execute(&mut tk, "st", &["NULL", "'b1'", "10"]).0.is_empty());
}

/// A partial clustered key or a non-unique index is never a point read.
#[test]
fn prepared_point_cache_declines_partial_and_non_unique_keys() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 788);
    rows(&mut tk, "CREATE TABLE partial_pin (custacc VARCHAR(8) NOT NULL COLLATE utf8mb4_bin, seq BIGINT NOT NULL, stsrcd VARCHAR(4) NOT NULL COLLATE utf8mb4_bin, v BIGINT NOT NULL, PRIMARY KEY (custacc, seq) CLUSTERED, INDEX by_status (stsrcd, v))");
    for sql in ["SELECT seq, v FROM partial_pin WHERE custacc = 'x'", "SELECT seq, v FROM partial_pin WHERE stsrcd = 'x' AND v = 1"] {
        let plan = plan_lines(&mut tk, sql);
        assert!(plan.iter().all(|line| !line.contains("Point_Get")), "{sql}: {plan:?}");
    }
}

#[test]
fn prepared_point_cache_answers_an_is_null_residual() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 789);
    exec_all(&mut tk, &[
        "CREATE TABLE null_pin (alwcobj VARCHAR(8) NOT NULL COLLATE utf8mb4_bin, alwcnum VARCHAR(8) NOT NULL COLLATE utf8mb4_bin, lmtdms VARCHAR(8), flgval VARCHAR(2) NOT NULL COLLATE utf8mb4_bin, PRIMARY KEY (alwcobj, alwcnum) CLUSTERED)",
        "INSERT INTO null_pin VALUES ('o1','n1',NULL,'1'),('o1','n2','dms','1')",
        "ANALYZE TABLE null_pin",
        "PREPARE st FROM 'SELECT alwcobj, alwcnum, lmtdms, flgval FROM null_pin WHERE alwcobj = ? AND alwcnum = ? AND lmtdms IS NULL AND flgval = ?'",
    ]);
    assert_eq!(execute(&mut tk, "st", &["'o1'", "'n1'", "'1'"]).0, [["o1", "n1", "NULL", "1"]]);
    assert!(execute(&mut tk, "st", &["'o1'", "'n2'", "'1'"]).0.is_empty());
}

/// A bounded clustered handle range reads through Limit over a TableReader;
/// a full equality is a bare Point_Get, also under UPDATE.
#[test]
fn shared_planner_reads_the_bounded_clustered_handle_range() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 790);
    exec_all(&mut tk, &[
        "CREATE TABLE ycsb_scan (id VARCHAR(32) PRIMARY KEY CLUSTERED, v VARCHAR(32))",
        "INSERT INTO ycsb_scan VALUES ('user-0001','value-1'),('user-0002','value-2'),('user-0003','value-3')",
    ]);
    assert_eq!(q(&mut tk, "SELECT * FROM ycsb_scan WHERE id >= 'user-0002' LIMIT 1"), [["user-0002", "value-2"]]);
    assert_eq!(q(&mut tk, "SELECT * FROM ycsb_scan WHERE id >= 'user-0002' LIMIT 2").len(), 2);
    let point = explain_brief(&mut tk, "SELECT * FROM ycsb_scan WHERE id = 'user-0002'");
    assert_eq!(point.len(), 1, "{point:?}");
    assert!(point[0][0] == "Point_Get" && point[0][3].contains("clustered index:PRIMARY(id)"), "{point:?}");
    let scan = explain_brief(&mut tk, "SELECT * FROM ycsb_scan WHERE id >= 'user-0002' LIMIT 1").iter().map(|row| row[0].clone()).collect::<Vec<_>>();
    assert!(scan[0].starts_with("Limit") && scan[1].contains("TableReader") && scan[2].contains("Limit") && scan[3].contains("TableRangeScan"), "{scan:?}");
    let update = explain_brief(&mut tk, "UPDATE ycsb_scan SET v = 'updated' WHERE id = 'user-0002'").iter().map(|row| row[0].clone()).collect::<Vec<_>>();
    assert_eq!(update.len(), 2, "{update:?}");
    assert!(update[0].starts_with("Update") && update[1].contains("Point_Get"), "{update:?}");
}

/// Go plans Batch_Point_Get only for a bare IN over a full key; the reads
/// answer present keys and skip absent ones, across partitions too.
#[test]
fn batch_point_get_is_chosen_only_for_the_shapes_go_accepts() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 791);
    exec_all(&mut tk, &["CREATE TABLE bd (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)", "INSERT INTO bd VALUES (1, 'a', 10)"]);
    for (sql, batch) in [
        ("SELECT v FROM bd WHERE id IN (1, 2)", true),
        ("SELECT v FROM bd WHERE code IN ('a', 'zz')", true),
        ("SELECT v FROM bd WHERE id NOT IN (1)", false),
        ("SELECT v FROM bd WHERE v IN (1)", false),
        ("SELECT v FROM bd WHERE id = 1", false),
    ] {
        let plan = plan_lines(&mut tk, sql);
        assert_eq!(plan.iter().any(|line| line.contains("Batch_Point_Get")), batch, "{sql}: {plan:?}");
    }
    assert_eq!(q(&mut tk, "SELECT v FROM bd WHERE code IN ('a', 'zz')"), [["10"]]);
    for primary in ["PRIMARY KEY(id)", "PRIMARY KEY(id, v) CLUSTERED"] {
        exec_all(&mut tk, &[
            "DROP TABLE IF EXISTS bp",
            &format!("CREATE TABLE bp (id BIGINT, v BIGINT, {primary}) PARTITION BY HASH(id) PARTITIONS 4"),
            "INSERT INTO bp VALUES (1,10),(2,20),(3,30),(4,40)",
        ]);
        let predicate = if primary == "PRIMARY KEY(id)" { "id IN (4,1,3,2,1,99)" } else { "(id,v) IN ((4,40),(1,10),(3,30),(2,20),(1,10),(99,990))" };
        let mut got = q(&mut tk, &format!("SELECT id FROM bp WHERE {predicate}")).into_iter().map(|row| row[0].parse::<i64>().unwrap()).collect::<Vec<_>>();
        got.sort_unstable();
        assert_eq!(got, [1, 2, 3, 4], "{primary}");
    }
}

/// A point constant is keyed in the column's domain: exact-integer decimals,
/// reals and strings match, fractional ones never do.
#[test]
fn a_point_plan_keys_by_the_constant_in_the_columns_domain() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 792);
    exec_all(&mut tk, &[
        "CREATE TABLE pk1 (pk BIGINT PRIMARY KEY, v BIGINT)",
        "INSERT INTO pk1 VALUES (1, 10)",
        "CREATE TABLE uq (id BIGINT PRIMARY KEY, u BIGINT UNIQUE)",
        "INSERT INTO uq VALUES (7, 1)",
    ]);
    for (predicate, found) in [
        ("pk = 1.0", true), ("pk = 1.00", true), ("pk = 1e0", true), ("pk = '1'", true), ("pk = '1.0'", true),
        ("pk IN (1.0, 2.0)", true), ("pk = 1.5", false), ("pk = 0.5", false), ("pk = '1.5'", false),
        ("pk IN (1.5, 2.5)", false), ("pk <> 1.0", false), ("pk <> 1.5", true), ("pk < 1.5", true),
        ("pk <= 1.0", true), ("pk > 0.5", true), ("pk >= 1.0", true), ("pk BETWEEN 0.5 AND 1.5", true),
    ] {
        assert_eq!(!q(&mut tk, &format!("SELECT pk FROM pk1 WHERE {predicate}")).is_empty(), found, "{predicate}");
    }
    assert_eq!(q(&mut tk, "SELECT id FROM uq WHERE u = 1.0"), [["7"]]);
    assert_eq!(q(&mut tk, "SELECT id FROM uq WHERE u IN (1.0, 2.0)"), [["7"]]);
    assert!(q(&mut tk, "SELECT id FROM uq WHERE u = 1.5").is_empty());
}

#[test]
fn fast_point_get_replaces_selection_and_projection_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 793);
    exec_all(&mut tk, &["CREATE TABLE fast_point (id INT PRIMARY KEY, c CHAR(8) NOT NULL)", "INSERT INTO fast_point VALUES (1, 'one')"]);
    assert_eq!(plan_lines(&mut tk, "SELECT c FROM fast_point WHERE id = 1"), ["Point_Get\t1.00\troot\ttable:fast_point\thandle:1"]);
    assert_eq!(q(&mut tk, "SELECT c FROM fast_point WHERE id = 1"), [["one"]]);
}

#[test]
fn fast_point_writes_remove_only_the_consumed_selection_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 794);
    exec_all(&mut tk, &["CREATE TABLE fast_write (id INT PRIMARY KEY, c CHAR(8) NOT NULL)", "INSERT INTO fast_write VALUES (1, 'one')"]);
    assert_eq!(plan_lines(&mut tk, "UPDATE fast_write SET c = 'two' WHERE id = 1"), ["Update\tN/A\troot\t\tN/A", "└─Point_Get\t1.00\troot\ttable:fast_write\thandle:1"]);
    assert!(plan_lines(&mut tk, "UPDATE fast_write SET c = 'bad' WHERE id = 1 AND c = 'missing'").iter().any(|line| line.contains("Selection")));
    assert_eq!(affected(&mut tk, "UPDATE fast_write SET c = 'bad' WHERE id = 1 AND c = 'missing'"), 0);
    assert_eq!(affected(&mut tk, "UPDATE fast_write SET c = 'two' WHERE id = 1"), 1);
    assert_eq!(q(&mut tk, "SELECT c FROM fast_write WHERE id = 1"), [["two"]]);
    assert_eq!(plan_lines(&mut tk, "DELETE FROM fast_write WHERE id = 1"), ["Delete\tN/A\troot\t\tN/A", "└─Point_Get\t1.00\troot\ttable:fast_write\thandle:1"]);
    assert_eq!(affected(&mut tk, "DELETE FROM fast_write WHERE id = 1"), 1);
    assert!(q(&mut tk, "SELECT c FROM fast_write WHERE id = 1").is_empty());
}

/// The residual Selection over a common-handle range shows the logical rows
/// it keeps, not the access rows below it.
#[test]
fn residual_selection_uses_logical_rows_over_access_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 795);
    exec_all(&mut tk, &[
        "CREATE TABLE customer (c_id INT NOT NULL, c_d_id INT NOT NULL, c_w_id INT NOT NULL, c_first VARCHAR(16), c_middle CHAR(2), c_last VARCHAR(16), c_balance DECIMAL(12,2), PRIMARY KEY(c_w_id,c_d_id,c_id) CLUSTERED, KEY idx_customer(c_w_id,c_d_id,c_last,c_first))",
        "INSERT INTO customer VALUES (1,1,1,'Alice','OE','Able',10.00), (2,1,1,'Bob','OE','Able',20.00), (3,1,1,'Carol','OE','Baker',30.00), (1,2,1,'Dan','OE','Clark',40.00), (1,1,2,'Eve','OE','Davis',50.00)",
        "ANALYZE TABLE customer",
    ]);
    let plan = explain_brief(&mut tk, "SELECT c_balance, c_first, c_middle, c_id FROM customer IGNORE INDEX(idx_customer) WHERE c_w_id=1 AND c_d_id=1 AND c_last='Able' ORDER BY c_first");
    let selection = plan.iter().find(|row| row[0].contains("Selection")).unwrap_or_else(|| panic!("{plan:#?}"));
    let scan = plan.iter().find(|row| row[0].contains("TableRangeScan")).unwrap_or_else(|| panic!("{plan:#?}"));
    assert_ne!(selection[1], scan[1], "{plan:#?}");
}

#[test]
fn prepared_point_plan_answers_an_out_of_range_handle_without_reading() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 796);
    exec_all(&mut tk, &[
        "CREATE TABLE prepared_overflow (id BIGINT PRIMARY KEY, v BIGINT)",
        "INSERT INTO prepared_overflow VALUES (1, 10)",
        "PREPARE st FROM 'SELECT v FROM prepared_overflow WHERE id = ?'",
    ]);
    assert!(execute(&mut tk, "st", &["'99999999999999999999999999'"]).0.is_empty());
}

#[test]
fn out_of_range_point_literal_plans_a_table_dual() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 797);
    exec_all(&mut tk, &["CREATE TABLE plain_overflow (id BIGINT PRIMARY KEY, v BIGINT)", "INSERT INTO plain_overflow VALUES (1, 10)"]);
    let sql = "SELECT v FROM plain_overflow WHERE id = '99999999999999999999999999'";
    assert!(q(&mut tk, sql).is_empty());
    let plan = plan_lines(&mut tk, sql);
    assert!(plan.iter().any(|line| line.contains("TableDual")), "{plan:?}");
}
