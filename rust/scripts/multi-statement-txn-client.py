#!/usr/bin/env python3

"""Multi-statement transaction proof client for the Rust SQL node.

Drives two independent connections through one open transaction each, which is
the only way to observe what a transaction actually is: that a connection reads
its own uncommitted writes, that nobody else does until COMMIT, that a
pessimistic lock really blocks another transaction, and that two optimistic
transactions racing for one row produce exactly one winner. The `text` command
runs the same transaction legs over the TEXT protocol only: every INSERT,
UPDATE, DELETE, and locking read carries its own literals in a COM_QUERY
packet, with nothing prepared.

Every check is an assertion against the wire: a row value, an affected-row
count, or a MySQL error code. Any deviation exits nonzero.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import pathlib
import sys
from typing import Any

CLIENT_PATH = pathlib.Path(__file__).with_name("mysql-prepared-client.py")
_spec = importlib.util.spec_from_file_location("mysql_prepared_client", CLIENT_PATH)
if _spec is None or _spec.loader is None:
    raise SystemExit(f"cannot load the shared MySQL proof client from {CLIENT_PATH}")
_client = importlib.util.module_from_spec(_spec)
# Registered before execution because the shared client's `@dataclass`
# declarations resolve their own module through `sys.modules`.
sys.modules[_spec.name] = _client
_spec.loader.exec_module(_client)

MysqlConnection = _client.MysqlConnection
MysqlError = _client.MysqlError
ProtocolError = _client.ProtocolError
execute_write_payload = _client.execute_write_payload
parse_column = _client.parse_column
parse_error = _client.parse_error
read_lenenc = _client.read_lenenc
read_lenenc_bytes = _client.read_lenenc_bytes
assert_eof = _client.assert_eof

COM_QUERY = 0x03

# Go `errno` codes this proof asserts on the wire.
ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET = 3572
ERR_WRITE_CONFLICT = 9007


def emit(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, separators=(",", ":")), flush=True)


def query(connection: MysqlConnection, sql: str) -> tuple[list[list[str]], MysqlError | None]:
    """Runs one COM_QUERY, returning its text rows or the server's error.

    An OK packet (BEGIN/COMMIT/ROLLBACK) returns no rows and no error.
    """
    connection.write_packet(bytes([COM_QUERY]) + sql.encode(), 0)
    first = connection.read_packet(1)
    if first and first[0] == 0xFF:
        return [], parse_error(first)
    if first and first[0] == 0x00:
        return [], None
    column_count, offset = read_lenenc(first, 0)
    if column_count is None or offset != len(first):
        raise ProtocolError(f"query response omitted a concrete column count: {first.hex()}")
    sequence = 2
    for _ in range(column_count):
        parse_column(connection.read_packet(sequence))
        sequence += 1
    if column_count and not connection.deprecate_eof:
        assert_eof(connection.read_packet(sequence), False)
        sequence += 1
    rows: list[list[str]] = []
    while True:
        packet = connection.read_packet(sequence)
        sequence += 1
        if packet and packet[0] == 0xFE and len(packet) < 9:
            assert_eof(packet, connection.deprecate_eof)
            return rows, None
        cells: list[str] = []
        cursor = 0
        for _ in range(column_count):
            value, cursor = read_lenenc_bytes(packet, cursor)
            cells.append("NULL" if value is None else value.decode())
        rows.append(cells)


def run(connection: MysqlConnection, sql: str, *, name: str) -> list[list[str]]:
    """Runs one statement that must succeed."""
    rows, error = query(connection, sql)
    if error is not None:
        raise ProtocolError(f"{name}: {sql!r} failed with {error.code} {error.message}")
    return rows


def require_query_error(
    connection: MysqlConnection, sql: str, expected_code: int, *, name: str
) -> MysqlError:
    rows, error = query(connection, sql)
    if error is None or error.code != expected_code:
        raise ProtocolError(
            f"{name}: {sql!r} expected error {expected_code}, got rows={rows} error={error}"
        )
    return error


def read_row(connection: MysqlConnection, select_sql: str, *, name: str) -> list[str]:
    rows = run(connection, select_sql, name=name)
    if len(rows) != 1:
        raise ProtocolError(f"{name}: expected exactly one row, got {rows}")
    return rows[0]


def expect_row(connection: MysqlConnection, select_sql: str, expected: list[str], *, name: str) -> None:
    observed = read_row(connection, select_sql, name=name)
    if observed != expected:
        raise ProtocolError(f"{name}: expected row {expected}, observed {observed}")


def update_statement(connection: MysqlConnection, table: str) -> int:
    prepared = connection.prepare(f"UPDATE {table} SET balance = ? WHERE id = ?")
    if isinstance(prepared, MysqlError):
        raise ProtocolError(f"prepare UPDATE failed: {prepared.code} {prepared.message}")
    return prepared.statement_id


def apply_update(connection: MysqlConnection, statement_id: int, balance: int, row_id: int, *, name: str) -> None:
    affected, error = connection.execute_write(execute_write_payload(statement_id, [balance, row_id]))
    if error is not None:
        raise ProtocolError(f"{name}: UPDATE failed with {error.code} {error.message}")
    if affected != 1:
        raise ProtocolError(f"{name}: UPDATE reported {affected} affected rows, expected 1")


def connect(args: argparse.Namespace) -> MysqlConnection:
    return MysqlConnection(args.host, args.port, args.user, args.password)


def pessimistic(args: argparse.Namespace) -> None:
    """One pessimistic transaction's writes, isolation, and locks."""
    table = f"{args.database}.{args.table}"
    select_sql = f"SELECT id, balance FROM {table} WHERE id = {args.row_id}"
    lock_sql = f"{select_sql} FOR UPDATE NOWAIT"

    with connect(args) as first, connect(args) as second:
        before = read_row(second, select_sql, name="baseline")
        emit("baseline", row=before)
        if before[0] != str(args.row_id):
            raise ProtocolError(f"baseline row is not id {args.row_id}: {before}")
        if before[1] == str(args.new_balance):
            raise ProtocolError("the proof needs a new balance different from the stored one")

        run(first, "BEGIN PESSIMISTIC", name="A BEGIN")
        statement_id = update_statement(first, table)
        apply_update(first, statement_id, args.new_balance, args.row_id, name="A UPDATE")
        emit("updated_in_transaction", id=args.row_id, balance=args.new_balance)

        # Read-your-own-writes: A's own SELECT sees the row it just wrote,
        # although nothing has been published to TiKV yet.
        expect_row(
            first,
            select_sql,
            [str(args.row_id), str(args.new_balance)],
            name="A reads its own write",
        )
        emit("read_your_writes", observed=args.new_balance)

        # Isolation: B still sees the pre-transaction value.
        expect_row(second, select_sql, before, name="B is isolated")
        emit("isolated", observed=before[1])

        # A holds the row's pessimistic lock, so B cannot take it.
        run(second, "BEGIN PESSIMISTIC", name="B BEGIN")
        error = require_query_error(
            second, lock_sql, ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET, name="B NOWAIT"
        )
        emit("lock_refused", code=error.code, state=error.state, message=error.message)
        # The failure was statement-scoped: B's transaction is still usable.
        expect_row(second, select_sql, before, name="B survives its failed statement")
        run(second, "ROLLBACK", name="B ROLLBACK")

        run(first, "COMMIT", name="A COMMIT")
        emit("committed", id=args.row_id, balance=args.new_balance)

        # The write is public now, and the lock is gone.
        expect_row(
            second,
            select_sql,
            [str(args.row_id), str(args.new_balance)],
            name="B reads the committed value",
        )
        run(second, "BEGIN PESSIMISTIC", name="B BEGIN again")
        expect_row(
            second,
            lock_sql,
            [str(args.row_id), str(args.new_balance)],
            name="B locks the released row",
        )
        run(second, "ROLLBACK", name="B ROLLBACK again")
        emit("lock_released", id=args.row_id)


def optimistic(args: argparse.Namespace) -> None:
    """Two optimistic transactions racing for one row: exactly one wins."""
    table = f"{args.database}.{args.table}"
    select_sql = f"SELECT id, balance FROM {table} WHERE id = {args.row_id}"

    with connect(args) as first, connect(args) as second:
        before = read_row(first, select_sql, name="baseline")
        emit("baseline", row=before)

        run(first, "BEGIN OPTIMISTIC", name="A BEGIN")
        run(second, "BEGIN OPTIMISTIC", name="B BEGIN")
        # Both transactions write the same row. Neither takes a lock, so
        # neither learns of the other before its own COMMIT.
        apply_update(
            first, update_statement(first, table), args.first_balance, args.row_id, name="A UPDATE"
        )
        apply_update(
            second,
            update_statement(second, table),
            args.second_balance,
            args.row_id,
            name="B UPDATE",
        )
        emit("both_wrote", first=args.first_balance, second=args.second_balance)

        run(first, "COMMIT", name="A COMMIT")
        emit("first_commit_won", balance=args.first_balance)

        error = require_query_error(second, "COMMIT", ERR_WRITE_CONFLICT, name="B COMMIT")
        emit("second_commit_lost", code=error.code, message=error.message)

        # The winner's value is the durable one, and B is back in autocommit.
        expect_row(
            second,
            select_sql,
            [str(args.row_id), str(args.first_balance)],
            name="the winner's value is durable",
        )
        emit("winner_is_durable", balance=args.first_balance)


def write(connection: MysqlConnection, sql: str, *, name: str, expected_affected: int) -> None:
    """Runs one text-protocol DML statement, asserting its OK affected-row count.

    This is the mysql-client-style path: the statement carries its own literals
    in a COM_QUERY packet, with no prepare, no parameter markers, and no binary
    execute values anywhere.
    """
    connection.write_packet(bytes([COM_QUERY]) + sql.encode(), 0)
    packet = connection.read_packet(1)
    if packet and packet[0] == 0xFF:
        error = parse_error(packet)
        raise ProtocolError(f"{name}: {sql!r} failed with {error.code} {error.message}")
    if not packet or packet[0] != 0x00:
        raise ProtocolError(f"{name}: {sql!r} did not answer with an OK packet: {packet.hex()}")
    affected, cursor = read_lenenc(packet, 1)
    if affected is None or cursor > len(packet):
        raise ProtocolError(f"{name}: {sql!r} OK packet omitted affected rows: {packet.hex()}")
    if affected != expected_affected:
        raise ProtocolError(
            f"{name}: {sql!r} reported {affected} affected rows, expected {expected_affected}"
        )


def expect_no_row(connection: MysqlConnection, select_sql: str, *, name: str) -> None:
    rows = run(connection, select_sql, name=name)
    if rows:
        raise ProtocolError(f"{name}: expected no row, observed {rows}")


def text(args: argparse.Namespace) -> None:
    """Every DML leg of a transaction over the TEXT protocol only.

    No statement in this proof is prepared: each INSERT/UPDATE/DELETE carries
    its own literals in a COM_QUERY packet, inside and outside an explicit
    pessimistic transaction, and the locking read is text as well.
    """
    table = f"{args.database}.{args.table}"
    select_sql = f"SELECT id, balance FROM {table} WHERE id = {args.row_id}"
    lock_sql = f"{select_sql} FOR UPDATE NOWAIT"
    inserted_sql = f"SELECT id, balance FROM {table} WHERE id = {args.insert_id}"

    with connect(args) as first, connect(args) as second:
        before = read_row(second, select_sql, name="baseline")
        emit("baseline", row=before)
        if before[1] == str(args.new_balance):
            raise ProtocolError("the proof needs a new balance different from the stored one")
        expect_no_row(second, inserted_sql, name="the inserted row must not exist yet")

        run(first, "BEGIN PESSIMISTIC", name="A BEGIN")
        write(
            first,
            f"UPDATE {table} SET balance = {args.new_balance} WHERE id = {args.row_id}",
            name="A text UPDATE",
            expected_affected=1,
        )
        emit("text_update_in_transaction", id=args.row_id, balance=args.new_balance)

        # Read-your-own-writes over text, exactly as the prepared path proves it.
        expect_row(
            first,
            select_sql,
            [str(args.row_id), str(args.new_balance)],
            name="A reads its own text write",
        )
        # Isolation: B still sees the pre-transaction value.
        expect_row(second, select_sql, before, name="B is isolated")

        # A holds the row's pessimistic lock, so B's TEXT locking read is refused.
        run(second, "BEGIN PESSIMISTIC", name="B BEGIN")
        error = require_query_error(
            second, lock_sql, ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET, name="B text NOWAIT"
        )
        emit("text_lock_refused", code=error.code, state=error.state, message=error.message)
        expect_row(second, select_sql, before, name="B survives its failed statement")
        run(second, "ROLLBACK", name="B ROLLBACK")

        # A text INSERT in the same transaction, published by the same COMMIT.
        write(
            first,
            f"INSERT INTO {table} (id, balance) VALUES ({args.insert_id}, {args.insert_balance})",
            name="A text INSERT",
            expected_affected=1,
        )
        run(first, "COMMIT", name="A COMMIT")
        emit("text_transaction_committed", id=args.row_id, inserted=args.insert_id)

        expect_row(
            second,
            select_sql,
            [str(args.row_id), str(args.new_balance)],
            name="B reads the committed text UPDATE",
        )
        expect_row(
            second,
            inserted_sql,
            [str(args.insert_id), str(args.insert_balance)],
            name="B reads the committed text INSERT",
        )

        # Autocommit text DML: an arithmetic UPDATE and a point DELETE, each its
        # own single-statement transaction.
        write(
            second,
            f"UPDATE {table} SET balance = balance + 1 WHERE id = {args.insert_id}",
            name="text autocommit UPDATE",
            expected_affected=1,
        )
        expect_row(
            second,
            inserted_sql,
            [str(args.insert_id), str(args.insert_balance + 1)],
            name="the autocommit text UPDATE is durable",
        )
        emit("text_autocommit_update", id=args.insert_id, balance=args.insert_balance + 1)
        write(
            second,
            f"DELETE FROM {table} WHERE id = {args.insert_id}",
            name="text autocommit DELETE",
            expected_affected=1,
        )
        expect_no_row(second, inserted_sql, name="the autocommit text DELETE is durable")
        emit("text_autocommit_delete", id=args.insert_id)

        # Parity: a shape the prepared write path refuses is refused as text too,
        # rather than silently running as some other statement.
        rows, error = query(second, f"UPDATE {table} SET balance = 1 WHERE balance = 2")
        if error is None:
            raise ProtocolError(
                f"a non-point text UPDATE must be refused, got rows={rows}"
            )
        emit("text_refusal_matches_prepared", code=error.code, message=error.message)

def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    subcommands = result.add_subparsers(dest="command", required=True)
    for name in ("pessimistic", "optimistic", "text"):
        command = subcommands.add_parser(name)
        command.add_argument("--host", default="127.0.0.1")
        command.add_argument("--port", type=int, required=True)
        command.add_argument("--user", required=True)
        command.add_argument("--password", required=True)
        command.add_argument("--database", required=True)
        command.add_argument("--table", required=True)
        command.add_argument("--row-id", type=int, required=True)
        if name == "optimistic":
            command.add_argument("--first-balance", type=int, required=True)
            command.add_argument("--second-balance", type=int, required=True)
        else:
            command.add_argument("--new-balance", type=int, required=True)
        if name == "text":
            command.add_argument("--insert-id", type=int, required=True)
            command.add_argument("--insert-balance", type=int, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        commands = {"pessimistic": pessimistic, "optimistic": optimistic, "text": text}
        commands[args.command](args)
    except (ProtocolError, OSError) as error:
        emit("failed", command=args.command, error=str(error))
        return 1
    emit("passed", command=args.command)
    return 0


if __name__ == "__main__":
    sys.exit(main())
