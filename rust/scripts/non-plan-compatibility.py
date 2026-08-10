#!/usr/bin/env python3
"""Execute the manifest's non-plan SQL in an isolated TiDB schema.

CREATE/SPLIT/DROP and transaction-control statements do not have physical
plans.  This companion gate executes their exact SQL text and records schema
and transaction-state receipts instead of pretending EXPLAIN can cover them.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_MANIFEST = SCRIPT_DIR / "tpcc-sysbench-plan-manifest.json"
SAFE_NAME = re.compile(r"^[A-Za-z0-9_$]+$")
CREATE_TABLE_NAME = re.compile(
    r"^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?([A-Za-z0-9_$]+)`?",
    re.IGNORECASE,
)


class CompatibilityError(RuntimeError):
    """A non-plan statement cannot be given a trustworthy receipt."""


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def mysql(
    args: argparse.Namespace,
    sql: str,
    *,
    database: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    command = [
        args.mysql,
        "--batch",
        "--raw",
        "--skip-column-names",
        f"--connect-timeout={args.connect_timeout}",
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--user",
        args.user,
    ]
    if database is not None:
        if not SAFE_NAME.fullmatch(database):
            raise CompatibilityError(f"unsafe database name: {database!r}")
        command.extend(("--database", database))
    environment = os.environ.copy()
    if args.password:
        environment["MYSQL_PWD"] = args.password
    result = subprocess.run(
        command,
        input=sql,
        text=True,
        capture_output=True,
        timeout=args.statement_timeout,
        env=environment,
    )
    if check and result.returncode != 0:
        raise CompatibilityError(
            f"MySQL statement failed ({result.returncode}): {sql}\n{result.stderr.strip()}"
        )
    return result


def run_case(
    args: argparse.Namespace, case: dict[str, Any], database: str
) -> dict[str, Any]:
    result = mysql(args, case["sql"], database=database, check=False)
    receipt = {
        "id": case["id"],
        "source": case["source"],
        "sql_sha256": sha256_bytes(case["sql"].encode()),
        "returncode": result.returncode,
        "stdout": result.stdout.rstrip("\n"),
        "stderr": result.stderr.rstrip("\n"),
    }
    if result.returncode != 0:
        raise CompatibilityError(
            f"{case['id']} failed ({result.returncode}): {receipt['stderr']}"
        )
    return receipt


def table_names(cases: list[dict[str, Any]]) -> list[str]:
    names = []
    for case in cases:
        match = CREATE_TABLE_NAME.match(case["sql"].strip())
        if match is not None:
            names.append(match.group(1))
    return names


def normalized_show_create(args: argparse.Namespace, database: str, table: str) -> str:
    if not SAFE_NAME.fullmatch(table):
        raise CompatibilityError(f"unsafe table name: {table!r}")
    result = mysql(args, f"SHOW CREATE TABLE `{table}`", database=database)
    columns = result.stdout.rstrip("\n").split("\t", 1)
    if len(columns) != 2 or columns[0] != table:
        raise CompatibilityError(f"unexpected SHOW CREATE output for {table}: {result.stdout!r}")
    return columns[1].replace(f"`{database}`.", "")


def transaction_receipt(args: argparse.Namespace) -> dict[str, Any]:
    database = f"{args.database_prefix}_txn"
    mysql(args, f"DROP DATABASE IF EXISTS `{database}`")
    mysql(args, f"CREATE DATABASE `{database}`")
    try:
        mysql(args, "CREATE TABLE txn_probe (id INT PRIMARY KEY)", database=database)
        sql = (
            "BEGIN; INSERT INTO txn_probe VALUES (1); COMMIT; "
            "SELECT COUNT(*) FROM txn_probe; "
            "BEGIN; INSERT INTO txn_probe VALUES (2); ROLLBACK; "
            "SELECT COUNT(*) FROM txn_probe;"
        )
        result = mysql(args, sql, database=database)
        counts = [line for line in result.stdout.splitlines() if line]
        if counts != ["1", "1"]:
            raise CompatibilityError(
                f"unexpected committed/rolled-back row counts: {counts!r}"
            )
        return {
            "covered_case_ids": [
                "tpcc.run.transaction.begin",
                "tpcc.run.transaction.commit",
                "tpcc.run.transaction.rollback",
                "sysbench.run.transaction.begin",
                "sysbench.run.transaction.commit",
            ],
            "row_counts_after_commit_and_rollback": counts,
        }
    finally:
        mysql(args, f"DROP DATABASE IF EXISTS `{database}`", check=False)


def exact_create_database_receipt(args: argparse.Namespace) -> dict[str, Any]:
    databases = set(mysql(args, "SHOW DATABASES").stdout.splitlines())
    if "sbtest" in databases:
        raise CompatibilityError("refusing to touch pre-existing database 'sbtest'")
    try:
        created = mysql(args, "CREATE DATABASE sbtest")
        if "sbtest" not in set(mysql(args, "SHOW DATABASES").stdout.splitlines()):
            raise CompatibilityError("CREATE DATABASE sbtest did not make sbtest visible")
        return {
            "id": "sysbench.prepare.create_database",
            "sql_sha256": sha256_bytes(b"CREATE DATABASE sbtest"),
            "returncode": created.returncode,
        }
    finally:
        mysql(args, "DROP DATABASE IF EXISTS sbtest", check=False)


def run_suite(
    args: argparse.Namespace,
    manifest: dict[str, Any],
    suite: str,
    database: str,
) -> dict[str, Any]:
    selected = [
        case
        for case in manifest["cases"]
        if case["suite"] == suite and case["kind"] == "non_plan"
    ]
    prepare = [
        case
        for case in selected
        if case["phase"] == "prepare"
        and case["id"] != "sysbench.prepare.create_database"
    ]
    cleanup = [case for case in selected if case["phase"] == "cleanup"]
    names = table_names(prepare)
    if not names:
        raise CompatibilityError(f"{suite} has no CREATE TABLE cases")

    mysql(args, f"DROP DATABASE IF EXISTS `{database}`")
    mysql(args, f"CREATE DATABASE `{database}`")
    receipts: list[dict[str, Any]] = []
    try:
        for case in prepare:
            receipts.append(run_case(args, case, database))
        schema = {
            table: normalized_show_create(args, database, table) for table in names
        }
        for case in cleanup:
            receipts.append(run_case(args, case, database))
        remaining = mysql(args, "SHOW TABLES", database=database).stdout.splitlines()
        if remaining:
            raise CompatibilityError(f"{suite} cleanup left tables: {remaining!r}")
    finally:
        mysql(args, f"DROP DATABASE IF EXISTS `{database}`", check=False)

    encoded_schema = json.dumps(schema, sort_keys=True, separators=(",", ":")).encode()
    return {
        "database": database,
        "case_count": len(receipts),
        "cases": receipts,
        "schema": schema,
        "schema_sha256": sha256_bytes(encoded_schema),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--endpoint-name", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--mysql", default="mysql")
    parser.add_argument("--database-prefix", required=True)
    parser.add_argument("--connect-timeout", type=int, default=5)
    parser.add_argument("--statement-timeout", type=int, default=300)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not SAFE_NAME.fullmatch(args.database_prefix):
        raise CompatibilityError("database prefix contains unsafe characters")
    manifest = json.loads(args.manifest.read_text())
    if manifest.get("schema_version") != "1.0":
        raise CompatibilityError("unsupported manifest schema")

    report = {
        "schema_version": "1.0",
        "endpoint": {
            "name": args.endpoint_name,
            "host": args.host,
            "port": args.port,
        },
        "manifest_sha256": sha256_file(args.manifest),
        "transaction_control": transaction_receipt(args),
        "create_database": exact_create_database_receipt(args),
        "suites": {
            suite: run_suite(
                args,
                manifest,
                suite,
                f"{args.database_prefix}_{suite}",
            )
            for suite in ("tpcc", "sysbench")
        },
    }
    report["covered_non_plan_case_count"] = (
        len(report["transaction_control"]["covered_case_ids"])
        + 1
        + sum(suite["case_count"] for suite in report["suites"].values())
    )
    expected = sum(case["kind"] == "non_plan" for case in manifest["cases"])
    if report["covered_non_plan_case_count"] != expected:
        raise CompatibilityError(
            f"covered {report['covered_non_plan_case_count']} non-plan cases, expected {expected}"
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "covered": report["covered_non_plan_case_count"],
                "tpcc_schema": report["suites"]["tpcc"]["schema_sha256"],
                "sysbench_schema": report["suites"]["sysbench"]["schema_sha256"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (CompatibilityError, OSError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        print(f"non-plan compatibility error: {error}", file=sys.stderr)
        raise SystemExit(1)
