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

//! Go master `TestRangeIntersection` (`planner/core/casetest/index`): the
//! issue72780 (#72802) and issue73063 (#73064) cases.

use crate::support::testkit::TestKit;

fn rows_of(tk: &mut TestKit, sql: &str) -> Vec<Vec<String>> {
    tk.must_query(sql).rows().to_vec()
}

/// Go `TestRangeIntersection` "issue72780": with Fix54337 the appended index
/// ranges intersect with the full-column constraints left as filters.
#[test]
fn range_intersection_issue72780() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set @@tidb_opt_fix_control = '54337:ON'");
    tk.must_exec("create table issue72780 (id int primary key, a int, b bigint, c bigint, key abc(a,b,c))");
    tk.must_exec("insert into issue72780 values (1,0,null,1),(2,0,9,1),(3,0,10,null),(4,0,10,19),(5,0,10,20),(6,1,9,1),(7,2,10,19),(8,5,9,1),(9,0,11,1)");
    let cases = [
        (
            "a in (1,0,5,2) and ((a=0 and (b<10 or b is null)) or (a=0 and b=10 and (c<20 or c is null)))",
            "range:[0 NULL,0 10), [0 10 NULL,0 10 20)",
        ),
        (
            "a in (1,0,5,2) and (a=0 or (a=0 and b=1701659790724 and (c<209597 or c is null)))",
            "range:[0,0]",
        ),
        ("a in (-1,0,1) and ((a>0 and b=9) or (a=0 and b>9))", "range:(0 9,0 +inf], [1 9,1 +inf]"),
    ];
    for (predicate, ranges) in cases {
        let query = format!("select id from issue72780 {{}} where {predicate} order by a desc,b desc,c desc limit 3");
        let indexed = query.replace("{}", "force index(abc)");
        let plan = rows_of(&mut tk, &format!("explain format='brief' {indexed}"));
        let scans: Vec<String> = plan
            .iter()
            .filter(|row| row[0].contains("IndexRangeScan"))
            .map(|row| row.join(" "))
            .collect();
        assert!(!scans.is_empty(), "{indexed}: {plan:?}");
        for scan in &scans {
            assert!(scan.contains(&format!("{ranges}, keep order:")), "{indexed}: {scan}");
        }
        for quota in [0, 1, 1024] {
            tk.must_exec(&format!("set @@tidb_opt_range_max_size = {quota}"));
            for limit in [3, 50] {
                let q = query.replacen("limit 3", &format!("limit {limit}"), 1);
                let ignored = rows_of(&mut tk, &q.replace("{}", "ignore index(abc)"));
                assert_eq!(rows_of(&mut tk, &q.replace("{}", "force index(abc)")), ignored, "{q} quota={quota}");
            }
        }
        tk.must_exec("set @@tidb_opt_range_max_size = default");
    }
    tk.must_query("select id from issue72780 force index(abc) where a in (1,2) and ((a=0 and b<10) or (a=0 and b=10 and c<20))")
        .check(Vec::<&str>::new());
    tk.must_exec("create table prefix72780 (a varchar(8), b int, key ab(a(1),b))");
    tk.must_exec("insert into prefix72780 values ('aa',1),('ab',1),('ab',2),('ba',1),('ba',2),('bb',2)");
    let prefix_query = "select a,b from prefix72780 {} where a in ('aa','ab','ba') and ((a='ab' and b=1) or (a='ba' and b=2)) order by a,b limit 10";
    let ignored = rows_of(&mut tk, &prefix_query.replace("{}", "ignore index(ab)"));
    assert_eq!(rows_of(&mut tk, &prefix_query.replace("{}", "force index(ab)")), ignored);
    tk.must_exec("prepare stmt from 'select id from issue72780 force index(abc) where a in (?,?) and ((a=0 and b<10) or (a=0 and b=10 and c<20)) order by id'");
    tk.must_exec("set @a=0,@b=1");
    tk.must_query("execute stmt using @a,@b").check(["2", "4"]);
    tk.must_exec("set @a=1,@b=2");
    tk.must_query("execute stmt using @a,@b").check(Vec::<&str>::new());
    for typ in ["enum('a','z','b')", "set('a','z','b')"] {
        tk.must_exec("drop table if exists enum72780");
        tk.must_exec(&format!("create table enum72780 (a int, b {typ}, c int, key abc(a,b,c))"));
        tk.must_exec("insert into enum72780 values (0,'a',1),(0,'z',1),(0,'b',1),(1,'a',1),(1,'z',1),(1,'b',1),(0,null,1)");
        for predicate in [
            "a in (0,1) and b in ('a','z','b') and ((a=0 and b='z' and c>0) or (a=1 and b='a' and c>0))",
            "a in (0,1) and ((a=0 and b in ('a','z','b') and c>0) or (a=1 and b='z' and c>0))",
        ] {
            let q = format!("select a,b,c from enum72780 {{}} where {predicate} order by a,b,c limit 50");
            let ignored = rows_of(&mut tk, &q.replace("{}", "ignore index(abc)"));
            assert_eq!(rows_of(&mut tk, &q.replace("{}", "force index(abc)")), ignored, "{typ} {predicate}");
        }
    }
    tk.must_exec("create table bit72780 (a int, b bit(16), key ab(a,b))");
    tk.must_exec("insert into bit72780 values (1,255),(1,256),(1,511),(1,512),(2,255),(2,256),(2,511),(2,512),(1,null)");
    let bit_query = "select a,b+0 from bit72780 {} where a in (1,2) and ((a=1 and b=255) or (a=2 and b<512)) order by a,b";
    tk.must_query(&bit_query.replace("{}", "force index(ab)")).check(["1 255", "2 255", "2 256", "2 511"]);
    let ignored = rows_of(&mut tk, &bit_query.replace("{}", "ignore index(ab)"));
    assert_eq!(rows_of(&mut tk, &bit_query.replace("{}", "force index(ab)")), ignored);
    tk.must_exec("deallocate prepare stmt");
}

/// Go `TestRangeIntersection` "issue73063": a prefix-index range keeps its
/// residual OR filter, and the filter on one branch does not leak into the
/// other.
#[test]
fn range_intersection_issue73063() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create table r (a int, b varchar(20), c int, key idx(a,b(1)))");
    tk.must_exec("insert into r values (1,'xa',1),(1,'xz',1),(1,'ya',1),(2,'zz',1),(3,'aa',1),(1,'xz',0)");
    for fix_control in ["54337:OFF,44389:OFF", "", "54337:OFF,44389:ON"] {
        tk.must_exec(&format!("set tidb_opt_fix_control='{fix_control}'"));
        for index_hint in ["force index(idx)", "ignore index(idx)"] {
            tk.must_query(&format!("select * from r {index_hint} where ((a=1 and b>'xx') or a>2) and c=1 order by a,b,c"))
                .check(["1 xz 1", "1 ya 1", "3 aa 1"]);
        }
    }
}
