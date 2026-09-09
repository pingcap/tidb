# Read entry points

The Rust server has two SQL-to-storage paths:

- `--cluster-session` uses `tidb-session::Session`, the executor driver and
  shared planner, with `cluster_storage` and `remote_scan` for TiKV access.
- `--read-table` and `--load-table` use the configured single/multi-table
  nodes, `ReadOnlyScanPlan` and `tidb-exec::real_tikv_read`. This path has
  separate, restricted AST-to-DAG lowering and is used by the configured-node
  scripts.

Go implementation and original Go tests define correctness. General read
implementation belongs in the shared planner/executor path; configured-node
consumers should reuse those owners. Do not freeze the current unsupported
feature lists, source layout or file sizes with Rust-only protection tests.

The configured path is still referenced by server modes and live-test scripts.
Removing it requires migrating those callers; deleting its protection tests
does not migrate or remove the implementation.
