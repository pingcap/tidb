//! ADD PARTITION error surfaces: adding a partition after a MAXVALUE
//! definition fails with Go's `ErrPartitionMaxvalue` text ("MAXVALUE can
//! only be used in last partition definition", 1493), for both an
//! increasing and a decreasing new bound.

use tidb_session::Session;

fn setup(session: &mut Session) {
    session
        .run(
            "create table t (a int) partition by range (a) \
             (partition p0 values less than (10), partition pmax values less than (maxvalue))",
        )
        .unwrap();
}

#[test]
fn add_partition_after_maxvalue_is_refused() {
    let mut session = Session::new();
    setup(&mut session);

    for bound in ["alter table t add partition (partition p2 values less than (30))",
                  "alter table t add partition (partition p3 values less than (5))"] {
        let error = session
            .run(bound)
            .expect_err("nothing may follow a MAXVALUE partition");
        assert!(
            error
                .to_string()
                .contains("MAXVALUE can only be used in last partition definition"),
            "{error}"
        );
    }
}
