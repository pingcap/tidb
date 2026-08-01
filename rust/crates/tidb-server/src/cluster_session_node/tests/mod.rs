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
mod point_get_max_ts;
mod prepared_transactions;
mod schema_changes;
mod statistics;
mod transactions;
mod wide_sql;
