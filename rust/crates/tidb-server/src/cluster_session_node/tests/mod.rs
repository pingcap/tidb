//! The convergence node's tests, grouped by the seam each one exercises.
//!
//! The three support modules build the world -- the committed cluster, the
//! stored-state seams, and one authenticated connection over a loaded catalog
//! -- and the rest assert one subject each.

mod mock_cluster;
mod mock_seams;
mod node_fixture;

mod accounts;
mod autocommit_transactions;
mod global_variables;
mod point_get_max_ts;
mod prepared_transactions;
mod schema_changes;
mod statistics;
mod transactions;
mod unistore_cop;
mod wide_sql;

#[test]
fn analyze_process_sql_detection_matches_go() {
    for sql in [
        "analyze table test.t",
        " analyze table test.t ",
        " ANALYZE TABLE test.t ",
        "/* axxxx */ analyze table test.t",
        "/*\n/*> this is a\n/*> multiple-line comment\n/*> */ analyze table test.t",
        "/*+ hint */ analyze table test.t",
        "/*+ hint */analyze table test.t",
    ] {
        assert!(super::is_analyze_table_sql(sql), "{sql:?}");
    }
}
