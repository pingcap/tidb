// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Complete source rows from `pkg/parser/digester_test.go`.

#![allow(deprecated)]

use sha2::{Digest as _, Sha256};
use tidb_parser::{
    digest_hash, digest_normalized, normalize, normalize_digest, normalize_digest_for_binding,
    normalize_for_binding, normalize_keep_hint, Digest, RedactMode,
};

#[test]
fn test_normalize() {
    let rows = [
        ("select _utf8mb4'123'", "select (_charset) ?"),
        ("select * from b where id in (_utf8mb4'123')", "select * from `b` where `id` in ( (_charset) ? )"),
        ("select * from b where id in (_utf8mb4'123', _binary'34')", "select * from `b` where `id` in ( ... )"),
        ("select * from b where id in (_utf8mb4'123', _binary'34', _binary'56')", "select * from `b` where `id` in ( ... )"),
        ("SELECT 1", "select ?"),
        ("select null", "select ?"),
        (r"select \N", "select ?"),
        ("SELECT `null`", "select `null`"),
        ("select * from b where id = 1", "select * from `b` where `id` = ?"),
        ("select 1 from b where id in (1, 3, '3', 1, 2, 3, 4)", "select ? from `b` where `id` in ( ... )"),
        ("select 1 from b where id in (1, a, 4)", "select ? from `b` where `id` in ( ? , `a` , ? )"),
        ("select 1 from b order by 2", "select ? from `b` order by 2"),
        ("select /*+ a hint */ 1", "select ?"),
        ("select /* a hint */ 1", "select ?"),
        ("select truncate(1, 2)", "select truncate ( ... )"),
        ("select -1 + - 2 + b - c + 0.2 + (-2) from c where d in (1, -2, +3)", "select ? + ? + `b` - `c` + ? + ( ? ) from `c` where `d` in ( ... )"),
        ("select * from t where a <= -1 and b < -2 and c = -3 and c > -4 and c >= -5 and e is 1", "select * from `t` where `a` <= ? and `b` < ? and `c` = ? and `c` > ? and `c` >= ? and `e` is ?"),
        ("select count(a), b from t group by 2", "select count ( `a` ) , `b` from `t` group by 2"),
        ("select count(a), b, c from t group by 2, 3", "select count ( `a` ) , `b` , `c` from `t` group by 2 , 3"),
        ("select count(a), b, c from t group by (2, 3)", "select count ( `a` ) , `b` , `c` from `t` group by ( 2 , 3 )"),
        ("select a, b from t order by 1, 2", "select `a` , `b` from `t` order by 1 , 2"),
        ("select count(*) from t", "select count ( ? ) from `t`"),
        ("select * from t Force Index(kk)", "select * from `t`"),
        ("select * from t USE Index(kk)", "select * from `t`"),
        ("select * from t Ignore Index(kk)", "select * from `t`"),
        ("select * from t1 straight_join t2 on t1.id=t2.id", "select * from `t1` join `t2` on `t1` . `id` = `t2` . `id`"),
        ("select * from `table`", "select * from `table`"),
        ("select * from `30`", "select * from `30`"),
        ("select * from `select`", "select * from `select`"),
        ("select * from 🥳", "select * from `🥳`"),
        ("select * from t ignore index(", "select * from `t` ignore index"),
        ("select /*+ ", "select "),
        ("select 1 / 2", "select ? / ?"),
        ("select * from t where a = 40 limit ?, ?", "select * from `t` where `a` = ? limit ..."),
        ("select * from t where a > ?", "select * from `t` where `a` > ?"),
        ("select @a=b from t", "select @a = `b` from `t`"),
        ("select * from `table", "select * from"),
        ("Select * from t where (i, j) in ((1,1), (2,2))", "select * from `t` where ( `i` , `j` ) in ( ( ... ) )"),
        ("insert into t values (1,1), (2,2)", "insert into `t` values ( ... )"),
        ("insert into t values (1), (2)", "insert into `t` values ( ... )"),
        ("insert into t values (1)", "insert into `t` values ( ? )"),
    ];
    for (sql, expected) in rows {
        let normalized = normalize(sql, RedactMode::Enabled);
        assert_eq!(normalized, expected, "{sql}");
        let (combined, digest) = normalize_digest(sql);
        assert_eq!(combined, normalized, "{sql}");
        assert_eq!(digest, digest_normalized(&normalized), "{sql}");
    }

    let binding_rows = [
        (
            "select * from t where a in (1)",
            "select * from `t` where `a` in ( ... )",
        ),
        (
            "select * from t where (a, b) in ((1, 1))",
            "select * from `t` where ( `a` , `b` ) in ( ( ... ) )",
        ),
        (
            "select * from t where (a, b) in ((1, 1), (2, 2))",
            "select * from `t` where ( `a` , `b` ) in ( ( ... ) )",
        ),
        (
            "select * from t where a in(1, 2)",
            "select * from `t` where `a` in ( ... )",
        ),
        (
            "select * from t where a in(1, 2, 3)",
            "select * from `t` where `a` in ( ... )",
        ),
    ];
    for (sql, expected) in binding_rows {
        let normalized = normalize_for_binding(sql, false);
        assert_eq!(normalized, expected, "{sql}");
        let (combined, digest) = normalize_digest_for_binding(sql);
        assert_eq!(combined, normalized, "{sql}");
        assert_eq!(digest, digest_normalized(&normalized), "{sql}");
    }
    assert_eq!(
        normalize_for_binding("select * from t where a in (...)", true),
        "select * from `t` where `a` in ( ? )"
    );
}

#[test]
fn test_normalize_redact() {
    let rows = [
        ("select * from t where a in (1)", "select * from `t` where `a` in ( ‹1› )"),
        ("select * from t where a in (1, 3)", "select * from `t` where `a` in ( ‹1› , ‹3› )"),
        ("select ? from b order by 2", "select ? from `b` order by ‹2›"),
        ("select ? from b order by 2 limit 10 offset 10", "select ? from `b` order by ‹2› limit ‹10› offset ‹10›"),
        ("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 c1 from cte1 limit 100 offset 100) select * from cte1;", "with recursive `cte1` ( `c1` ) as ( select `c1` from `t1` union select `c1` + ‹1› `c1` from `cte1` limit ‹100› offset ‹100› ) select * from `cte1`"),
        ("select *, first_value(v) over (partition by p order by o range between 3 preceding and 0 following) as a from test.first_range", "select * , `first_value` ( `v` ) `over` ( partition by `p` order by `o` range between ‹3› preceding and ‹0› following ) as `a` from `test` . `first_range`"),
    ];
    for (sql, expected) in rows {
        assert_eq!(normalize(sql, RedactMode::Marker), expected, "{sql}");
    }
    assert_eq!(normalize("select ‹1›", RedactMode::Marker), "select `‹1›`");
    assert_eq!(normalize("SELECT 1", RedactMode::Disabled), "SELECT 1");
}

#[test]
fn test_normalize_keep_hint() {
    let rows = [
        ("select _utf8mb4'123'", "select (_charset) ?"),
        ("SELECT 1", "select ?"),
        ("select null", "select ?"),
        (r"select \N", "select ?"),
        ("SELECT `null`", "select `null`"),
        ("select * from b where id = 1", "select * from `b` where `id` = ?"),
        ("select 1 from b where id in (1, 3, '3', 1, 2, 3, 4)", "select ? from `b` where `id` in ( ... )"),
        ("select 1 from b where id in (1, a, 4)", "select ? from `b` where `id` in ( ? , `a` , ? )"),
        ("select 1 from b order by 2", "select ? from `b` order by 2"),
        ("select /*+ a hint */ 1", "select /*+ a hint */ ?"),
        ("select /* a hint */ 1", "select ?"),
        ("select truncate(1, 2)", "select truncate ( ... )"),
        ("select -1 + - 2 + b - c + 0.2 + (-2) from c where d in (1, -2, +3)", "select ? + ? + `b` - `c` + ? + ( ? ) from `c` where `d` in ( ... )"),
        ("select * from t where a <= -1 and b < -2 and c = -3 and c > -4 and c >= -5 and e is 1", "select * from `t` where `a` <= ? and `b` < ? and `c` = ? and `c` > ? and `c` >= ? and `e` is ?"),
        ("select count(a), b from t group by 2", "select count ( `a` ) , `b` from `t` group by 2"),
        ("select count(a), b, c from t group by 2, 3", "select count ( `a` ) , `b` , `c` from `t` group by 2 , 3"),
        ("select count(a), b, c from t group by (2, 3)", "select count ( `a` ) , `b` , `c` from `t` group by ( 2 , 3 )"),
        ("select a, b from t order by 1, 2", "select `a` , `b` from `t` order by 1 , 2"),
        ("select count(*) from t", "select count ( ? ) from `t`"),
        ("select * from t Force Index(kk)", "select * from `t` force index ( `kk` )"),
        ("select * from t USE Index(kk)", "select * from `t` use index ( `kk` )"),
        ("select * from t Ignore Index(kk)", "select * from `t` ignore index ( `kk` )"),
        ("select * from t1 straight_join t2 on t1.id=t2.id", "select * from `t1` straight_join `t2` on `t1` . `id` = `t2` . `id`"),
        ("select * from `table`", "select * from `table`"),
        ("select * from `30`", "select * from `30`"),
        ("select * from `select`", "select * from `select`"),
        ("select * from 🥳", "select * from `🥳`"),
        ("select * from t ignore index(", "select * from `t` ignore index ("),
        ("select /*+ ", "select "),
        ("select 1 / 2", "select ? / ?"),
        ("select * from t where a = 40 limit ?, ?", "select * from `t` where `a` = ? limit ..."),
        ("select * from t where a > ?", "select * from `t` where `a` > ?"),
        ("select @a=b from t", "select @a = `b` from `t`"),
        ("select * from `table", "select * from"),
    ];
    for (sql, expected) in rows {
        assert_eq!(normalize_keep_hint(sql), expected, "{sql}");
    }
}

#[test]
fn test_normalize_digest() {
    let sql = "select 1 from b where id in (1, 3, '3', 1, 2, 3, 4)";
    let expected = "select ? from `b` where `id` in ( ... )";
    let expected_digest = "e1c8cc2738f596dc24f15ef8eb55e0d902910d7298983496362a7b46dbc0b310";
    let (normalized, digest) = normalize_digest(sql);
    assert_eq!(normalized, expected);
    assert_eq!(digest.as_str(), expected_digest);
    assert_eq!(digest_normalized(expected).as_str(), expected_digest);
}

#[test]
#[allow(deprecated)]
fn test_digest_hash_eq_for_simple_sql() {
    for group in [
        &[
            "select * from b where id = 1",
            "select * from b where id = '1'",
            "select * from b where id =2",
        ][..],
        &[
            "select 2 from b, c where c.id > 1",
            "select 4 from b, c where c.id > 23",
        ][..],
        &["Select 3", "select 1"][..],
        &[
            "Select * from t where (i, j) in ((1,1), (2,2))",
            "select * from t where (i, j) in ((1,1), (2,2), (3,3))",
        ][..],
        &[
            "insert into t values (1,1)",
            "insert into t values (1,1), (2,2)",
        ][..],
    ] {
        let first = digest_hash(group[0]);
        for sql in &group[1..] {
            assert_eq!(digest_hash(sql), first, "{sql}");
        }
    }
}

#[test]
#[allow(deprecated)]
fn test_digest_hash_not_eq_for_simple_sql() {
    let base = digest_hash("select * from b where id = 1");
    for sql in [
        "select a from b where id = 1",
        "select * from d where bid =1",
    ] {
        assert_ne!(digest_hash(sql), base, "{sql}");
    }
}

#[test]
fn test_gen_digest() {
    let bytes = Sha256::digest(b"abc").to_vec();
    let digest = Digest::new(bytes.clone());
    assert_eq!(
        digest.as_str(),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(digest.as_bytes(), bytes);
    let empty = Digest::new(Vec::<u8>::new());
    assert_eq!(empty.as_str(), "");
    assert!(empty.as_bytes().is_empty());
}
