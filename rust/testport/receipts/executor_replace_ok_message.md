# Batch: INSERT/REPLACE OK-packet info message (executor walk, write path)

## Go reference (master `f2c346fe4f3`)

* `pkg/executor/replace.go:211` `ReplaceExec.setMessage` and
  `pkg/executor/insert.go:557` `InsertExec.setMessage`: whenever the
  statement ran as INSERT ... SELECT or attempted MORE THAN ONE row, the
  statement context carries
  `fmt.Sprintf(mysql.ErrInsertInfo.Raw, numRecords, numDuplicates, warnings)`
  = `Records: %d  Duplicates: %d  Warnings: %d` where
  `numDuplicates = AffectedRows() - RecordRows()`
  (`replace.go:163` `AddRecordRows(len(newRows))`).
* `writeOKWith` (`pkg/server/conn.go`) appends that text to the OK packet's
  info field.

## Divergence found

The Rust configured-write path reported `affected_rows` only; nothing ever
composed the info text, so a MySQL client running a multi-row INSERT or
REPLACE never saw the Records/Duplicates/Warnings summary Go answers with.

## Fix

* `tidb-planner::prepared_dml`:
  * `configured_write_record_rows(&ConfiguredPreparedWrite)` — the
    attempted-row count (insert-family writes return their row count; the
    point UPDATE/DELETE writes return 0).
  * `compose_insert_ok_message(record_rows, affected_rows, warnings)` —
    Go's gate (`record_rows > 1`) and Go's exact two-space text.
* `tidb-server`:
  * `QuerySession::statement_message()` default `None`; both real-node
    sessions compose it after a configured write via the helper;
  * `write_affected_rows_ok` forwards the message into `OkPacket.info`.
* `tidb-server/src/cluster_session_node/tests/mock_seams.rs`: restores the
  four `DdlStatement::*NoOp` arms the tip's test build was missing (the
  remote tip did not compile its tidb-server test target without them).

## Tests

* planner unit tests: the message text for a 2-row replace with 3 affected
  (`Records: 2  Duplicates: 1  Warnings: 1`), the single-row/empty gate,
  and the attempted-row counting per write family.
* wire regression `a_multi_row_write_ok_packet_carries_the_records_message_as_info`:
  a REPLACE over the socket answers with an OK whose length-encoded info is
  exactly the composed text.

## Known pre-existing failure on the tip (not this batch)

`mysql_client_lifecycle_source::cursor_reader_error_is_reported_by_fetch_
and_closes_cursor` fails identically on the bare tip + mock-arms-only
repair (verified in a clean worktree before any of this batch's other
changes): the cursor's spilled `DataInDiskByRows` chunk file is missing at
fetch time. The tip could not compile its test target before the mock-arms
repair, so this failure predates this batch and belongs to the cursor/
spill work.
