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

//! Source contract for the real Campaign 27 prepared TCP route.
//!
//! Live TCP/PD/TiKV acceptance belongs to the campaign runner. These guards
//! keep the production wiring from drifting to interpolation, text rows, or a
//! process-global statement registry while that real proof is assembled.

#[test]
fn authenticated_connection_owns_the_registry_and_all_three_commands() {
    let source = include_str!("../src/mysql_connection.rs");
    let session = source
        .find("factory.open_session(SessionContext")
        .expect("real worker-local session opens after authentication");
    let registry = source
        .find("let mut prepared = PreparedStatementRegistry::default()")
        .expect("one connection-local prepared registry");
    let command_loop = source
        .find("reader.set_sequence(0);\n        let payload = match reader.read_packet()")
        .expect("authenticated command loop");
    assert!(session < registry && registry < command_loop);
    assert!(source.contains("Command::StmtPrepare(bytes)"));
    assert!(source.contains("Command::StmtExecute(bytes)"));
    assert!(source.contains("Command::StmtClose(bytes)"));
    assert!(!source.contains("static PREPARED"));
}

#[test]
fn connection_receipt_distinguishes_prepared_commands_from_text_fallback() {
    let source = include_str!("../src/mysql_connection.rs");
    assert!(source.contains("commands.text_query_commands += 1"));
    assert!(source.contains("commands.stmt_prepare_commands += 1"));
    assert!(source.contains("commands.stmt_prepare_successes += 1"));
    assert!(source.contains("commands.stmt_execute_commands += 1"));
    assert!(source.contains("commands.stmt_execute_successes += 1"));
    assert!(source.contains("commands.stmt_close_commands += 1"));
    assert!(source.contains("\\\"stmt_execute_successes\\\":{}"));
}

#[test]
fn prepare_and_execute_use_typed_real_session_and_binary_rows() {
    let connection = include_str!("../src/mysql_connection.rs");
    let prepare_branch = connection
        .find("Command::StmtPrepare(bytes) => {")
        .expect("prepare dispatch branch");
    let prepare = connection[prepare_branch..]
        .find("engine.prepare_point_read(sql)")
        .map(|offset| prepare_branch + offset)
        .expect("prepare reaches the concrete session catalog");
    let prepare_packets = connection[prepare_branch..]
        .find("encode_prepared_statement_prepare_response")
        .map(|offset| prepare_branch + offset)
        .expect("prepare emits protocol-owned metadata");
    assert!(prepare < prepare_packets);

    let execute_branch = connection
        .find("Command::StmtExecute(bytes) => {")
        .expect("execute dispatch branch");
    // Matched without its argument list: the call spans several lines once the
    // marker count became per statement, but the ordering this asserts —
    // typed decode, then execution, then binary rows — is the actual contract.
    let decode = connection[execute_branch..]
        .find("decode_prepared_statement_execute(")
        .map(|offset| execute_branch + offset)
        .expect("execute packet is typed before execution");
    let execute = connection[execute_branch..]
        .find(".execute_prepared_point_read(&point_read, &parameters)")
        .map(|offset| execute_branch + offset)
        .expect("typed values reach the concrete session");
    let binary = connection[execute_branch..]
        .find("write_connection_binary_result_set_to_sink")
        .map(|offset| execute_branch + offset)
        .expect("prepared results use MySQL binary rows");
    assert!(decode < execute && execute < binary);
    assert!(!connection.contains("replace(\"?\""));
}

#[test]
fn single_and_multi_table_sessions_open_the_existing_real_tikv_plan_path() {
    let executor = include_str!("../../tidb-exec/src/real_tikv_read.rs");
    let single = include_str!("../src/real_tikv_node.rs");
    let multi = include_str!("../src/real_tikv_multi_node.rs");

    assert!(executor.contains("prepare_configured_point_read"));
    assert!(executor.contains("execute_lowered_plan_with_cancellation"));
    assert!(executor.contains("execute_point_read_plan_with_cancellation"));
    assert!(single.contains(".execute_lowered_plan_with_cancellation(plan, cancellation)"));
    assert!(multi.contains(".execute_point_read_plan_with_cancellation(plan, cancellation)"));
    assert!(single.contains("observe_real_tikv_query"));
    assert!(multi.contains("observe_real_tikv_query"));
}

#[test]
fn close_is_silent_and_binary_writer_rejects_non_signed_bigint_rows() {
    let connection = include_str!("../src/mysql_connection.rs");
    let close_start = connection
        .find("Command::StmtClose(bytes) => {")
        .expect("close dispatch branch");
    // The close branch ends at whatever dispatch arm follows it, which is not
    // always the same one -- bounding it by a specific later arm would sweep
    // that arm's own body into this assertion.
    let close_end = connection[close_start..]
        .find("\n            Command::")
        .map(|offset| close_start + offset)
        .expect("next dispatch branch");
    let close_branch = &connection[close_start..close_end];
    assert!(close_branch.contains("prepared.remove(statement_id)"));
    assert!(!close_branch.contains("write_"));

    let writer = include_str!("../src/connection_resultset.rs");
    // The binary writer maps each typed Datum to a binary cell dispatched by the
    // column type, exactly as Go's DumpBinaryRow switches on `columns[i].Type`,
    // and frames rows through the shared stream.
    assert!(writer.contains("fn datum_to_binary_cell"));
    assert!(writer.contains("BinaryResultCell::LongLong(value)"));
    assert!(writer.contains("BinaryResultCell::NewDecimal(value)"));
    assert!(writer.contains("BinaryResultCell::String("));
    assert!(writer.contains("BinaryResultSetStream"));
}
