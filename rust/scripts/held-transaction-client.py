#!/usr/bin/env python3

"""How long a client may hold an explicit transaction open before COMMIT.

Wall-clock time a connection spends inside a transaction is not work the
transaction did, and it must not consume the budget the commit will need. This
client holds one explicit transaction open for a requested number of seconds
between its statements and then commits, reporting the exact wire outcome. A
run at several hold times is what separates "the commit is broken" from "the
commit ran out of a budget that started at BEGIN".

The `ttl` command is the same shape held past the 20-second pessimistic lock
TTL, where the keep-alive heartbeat is the only thing that can keep the primary
lock alive; it also writes a second row after the wait, so the commit publishes
a lock the heartbeat has been refreshing.

Every check is an assertion against the wire. Any deviation exits nonzero.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import pathlib
import sys
import time
from typing import Any

CLIENT_PATH = pathlib.Path(__file__).with_name("mysql-prepared-client.py")
_spec = importlib.util.spec_from_file_location("mysql_prepared_client", CLIENT_PATH)
if _spec is None or _spec.loader is None:
    raise SystemExit(f"cannot load the shared MySQL proof client from {CLIENT_PATH}")
_client = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _client
_spec.loader.exec_module(_client)

MysqlConnection = _client.MysqlConnection
MysqlError = _client.MysqlError
ProtocolError = _client.ProtocolError
parse_column = _client.parse_column
parse_error = _client.parse_error
read_lenenc = _client.read_lenenc
read_lenenc_bytes = _client.read_lenenc_bytes
assert_eof = _client.assert_eof

COM_QUERY = 0x03


def emit(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, separators=(",", ":")), flush=True)


def query(connection: MysqlConnection, sql: str) -> tuple[list[list[str]], MysqlError | None]:
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
    rows, error = query(connection, sql)
    if error is not None:
        raise ProtocolError(f"{name}: {sql!r} failed with {error.code} {error.message}")
    return rows


def read_row(connection: MysqlConnection, select_sql: str, *, name: str) -> list[str]:
    rows = run(connection, select_sql, name=name)
    if len(rows) != 1:
        raise ProtocolError(f"{name}: expected exactly one row, got {rows}")
    return rows[0]


def connect(args: argparse.Namespace) -> MysqlConnection:
    connection = MysqlConnection(args.host, args.port, args.user, args.password)
    # The hold is longer than the shared client's own five-second socket
    # timeout, and the COMMIT's answer must be read after it.
    connection.stream.settimeout(max(120, args.hold_seconds * 3))
    return connection


def begin_statement(mode: str) -> str:
    return "BEGIN PESSIMISTIC" if mode == "pessimistic" else "BEGIN OPTIMISTIC"


def hold(args: argparse.Namespace) -> None:
    """Holds one explicit transaction open for `--hold-seconds`, then commits."""
    table = f"{args.database}.{args.table}"
    select_sql = f"SELECT id, balance FROM {table} WHERE id = {args.row_id}"

    with connect(args) as held, connect(args) as observer:
        before = read_row(observer, select_sql, name="baseline")
        emit("baseline", row=before, mode=args.mode, hold_seconds=args.hold_seconds)

        run(held, begin_statement(args.mode), name="BEGIN")
        run(
            held,
            f"UPDATE {table} SET balance = {args.new_balance} WHERE id = {args.row_id}",
            name="UPDATE",
        )
        # The transaction is now open with a buffered write, exactly as a client
        # that walked away from its terminal leaves it.
        time.sleep(args.hold_seconds)

        started = time.monotonic()
        rows, error = query(held, "COMMIT")
        elapsed_ms = round((time.monotonic() - started) * 1000, 1)
        if error is not None:
            emit(
                "commit_failed",
                mode=args.mode,
                hold_seconds=args.hold_seconds,
                code=error.code,
                message=error.message,
                commit_ms=elapsed_ms,
            )
            raise ProtocolError(
                f"COMMIT after holding {args.hold_seconds}s failed with "
                f"{error.code} {error.message}"
            )
        if rows:
            raise ProtocolError(f"COMMIT returned rows: {rows}")
        emit(
            "committed",
            mode=args.mode,
            hold_seconds=args.hold_seconds,
            balance=args.new_balance,
            commit_ms=elapsed_ms,
        )

        observed = read_row(observer, select_sql, name="published")
        if observed != [str(args.row_id), str(args.new_balance)]:
            raise ProtocolError(f"the held transaction published {observed}")
        emit("passed", command="hold", mode=args.mode, hold_seconds=args.hold_seconds)


def ttl(args: argparse.Namespace) -> None:
    """A pessimistic transaction held past the 20s lock TTL, heartbeat alive.

    The lock the keep-alive refreshes is the primary, and since the primary-pin
    change the pinned primary IS the key the commit prewrites first. A hold
    longer than the TTL therefore either survives on the heartbeat's refreshes
    or tears at Prewrite with the lock already collected.
    """
    table = f"{args.database}.{args.table}"
    first_select = f"SELECT id, balance FROM {table} WHERE id = {args.row_id}"
    second_select = f"SELECT id, balance FROM {table} WHERE id = {args.second_row_id}"

    with connect(args) as held, connect(args) as observer:
        before_first = read_row(observer, first_select, name="baseline first")
        before_second = read_row(observer, second_select, name="baseline second")
        emit("baseline", first=before_first, second=before_second)

        run(held, "BEGIN PESSIMISTIC", name="BEGIN")
        # Statement one takes the pessimistic lock the heartbeat then refreshes.
        run(
            held,
            f"UPDATE {table} SET balance = {args.new_balance} WHERE id = {args.row_id}",
            name="UPDATE one",
        )
        emit("locked_first_row", id=args.row_id, ttl_wait_seconds=args.hold_seconds)

        time.sleep(args.hold_seconds)

        # Statement two AFTER the TTL window: it must still be admitted, which
        # it can only be if the transaction's locks were kept alive.
        run(
            held,
            f"UPDATE {table} SET balance = {args.second_balance} "
            f"WHERE id = {args.second_row_id}",
            name="UPDATE two after the TTL window",
        )
        emit("second_statement_admitted", id=args.second_row_id)

        # Isolation still holds after the wait: nothing was published early.
        for select_sql, expected, name in (
            (first_select, before_first, "observer first"),
            (second_select, before_second, "observer second"),
        ):
            if read_row(observer, select_sql, name=name) != expected:
                raise ProtocolError(f"{name}: the held transaction leaked a write")

        started = time.monotonic()
        rows, error = query(held, "COMMIT")
        elapsed_ms = round((time.monotonic() - started) * 1000, 1)
        if error is not None:
            emit(
                "commit_failed",
                hold_seconds=args.hold_seconds,
                code=error.code,
                message=error.message,
                commit_ms=elapsed_ms,
            )
            raise ProtocolError(
                f"COMMIT after {args.hold_seconds}s past the TTL window failed with "
                f"{error.code} {error.message}"
            )
        if rows:
            raise ProtocolError(f"COMMIT returned rows: {rows}")
        emit("committed", hold_seconds=args.hold_seconds, commit_ms=elapsed_ms)

        for select_sql, expected, name in (
            (first_select, [str(args.row_id), str(args.new_balance)], "published first"),
            (
                second_select,
                [str(args.second_row_id), str(args.second_balance)],
                "published second",
            ),
        ):
            observed = read_row(observer, select_sql, name=name)
            if observed != expected:
                raise ProtocolError(f"{name}: expected {expected}, observed {observed}")
        emit("passed", command="ttl", hold_seconds=args.hold_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("hold", "ttl"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--row-id", type=int, default=1)
    parser.add_argument("--second-row-id", type=int, default=2)
    parser.add_argument("--new-balance", type=int, required=True)
    parser.add_argument("--second-balance", type=int, default=0)
    parser.add_argument("--hold-seconds", type=int, required=True)
    parser.add_argument("--mode", choices=("pessimistic", "optimistic"), default="pessimistic")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        {"hold": hold, "ttl": ttl}[args.command](args)
    except (ProtocolError, OSError) as failure:
        emit("failed", command=args.command, detail=str(failure))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
