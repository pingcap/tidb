#!/usr/bin/env python3

"""Repeatable-read and start_ts conflict proof for the cluster-session node.

Two independent connections over the MySQL wire prove the two things holding
ONE transaction from BEGIN to COMMIT buys, and that only holding one can:

  * repeatable read -- a statement inside A's transaction cannot see a row B
    committed after A's BEGIN, because A has no timestamp newer than its own
    start_ts to see it at;
  * start_ts conflict detection -- A's COMMIT prewrites at that same start_ts,
    so TiKV refuses it once B's newer commit landed on the key, and the client
    is told 9007.

A per-statement-timestamp session would fail the first check (it would read
B's value) and the second (its prewrite would carry a timestamp newer than B's
commit and silently overwrite it). Every check is an assertion against the
wire; any deviation exits nonzero.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import pathlib
import sys
from typing import Any

TXN_CLIENT_PATH = pathlib.Path(__file__).with_name("multi-statement-txn-client.py")
_spec = importlib.util.spec_from_file_location("multi_statement_txn_client", TXN_CLIENT_PATH)
if _spec is None or _spec.loader is None:
    raise SystemExit(f"cannot load the shared transaction proof client from {TXN_CLIENT_PATH}")
_txn = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _txn
_spec.loader.exec_module(_txn)

connect = _txn.connect
run = _txn.run
read_row = _txn.read_row
expect_row = _txn.expect_row
require_query_error = _txn.require_query_error
ERR_WRITE_CONFLICT = _txn.ERR_WRITE_CONFLICT


def emit(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, separators=(",", ":")), flush=True)


def repeatable_read(args: argparse.Namespace) -> None:
    table = f"{args.database}.{args.table}"
    select_sql = f"SELECT id, balance FROM {table} WHERE id = {args.row_id}"

    with connect(args) as reader, connect(args) as writer:
        before = read_row(reader, select_sql, name="baseline")
        emit("baseline", row=before)
        original = before[1]

        # OPTIMISTIC by name, because the mode decides the answer and Go's
        # default is the other one. Captured from two real Go TiDB sessions
        # running this exact dance (pkg/session, mock store): under BEGIN
        # OPTIMISTIC, A's COMMIT is refused with 9007 and the row keeps the
        # racing writer's value; under BEGIN PESSIMISTIC, A's UPDATE takes a
        # lock at a newer for_update_ts, its COMMIT succeeds, and A's value
        # wins. The node's transaction tier is the optimistic 2PC, so the
        # optimistic answer is the one it owes.
        run(reader, "BEGIN OPTIMISTIC", name="A BEGIN")
        expect_row(reader, select_sql, before, name="A's first read")
        emit("a_read_before_the_race", balance=original)

        # B is a whole other connection in autocommit: its UPDATE takes its own
        # timestamps and is durable the moment it returns.
        run(
            writer,
            f"UPDATE {table} SET balance = {args.racing_balance} WHERE id = {args.row_id}",
            name="B UPDATE",
        )
        expect_row(
            writer,
            select_sql,
            [str(args.row_id), str(args.racing_balance)],
            name="B's own committed row",
        )
        emit("b_committed", balance=args.racing_balance)

        # The repeatable read: A still sees what it saw at BEGIN.
        expect_row(reader, select_sql, before, name="A's repeated read")
        emit("a_repeated_read_is_unchanged", balance=original)

        # And the conflict: A's prewrite carries the BEGIN timestamp, which now
        # predates B's commit on this very key.
        run(
            reader,
            f"UPDATE {table} SET balance = {args.losing_balance} WHERE id = {args.row_id}",
            name="A UPDATE",
        )
        error = require_query_error(reader, "COMMIT", ERR_WRITE_CONFLICT, name="A COMMIT")
        emit("a_commit_lost_the_race", code=error.code, message=error.message)

        # B's row is the durable one, and A is back in autocommit at the newest
        # committed state.
        expect_row(
            reader,
            select_sql,
            [str(args.row_id), str(args.racing_balance)],
            name="the winner's value is durable",
        )
        emit("passed", command="repeatable-read", balance=args.racing_balance)


def parser() -> argparse.ArgumentParser:
    arguments = argparse.ArgumentParser(description=__doc__)
    arguments.add_argument("--host", default="127.0.0.1")
    arguments.add_argument("--port", type=int, required=True)
    arguments.add_argument("--user", required=True)
    arguments.add_argument("--password", required=True)
    arguments.add_argument("--database", required=True)
    arguments.add_argument("--table", required=True)
    arguments.add_argument("--row-id", type=int, required=True)
    arguments.add_argument("--racing-balance", type=int, required=True)
    arguments.add_argument("--losing-balance", type=int, required=True)
    return arguments


def main() -> int:
    args = parser().parse_args()
    repeatable_read(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
