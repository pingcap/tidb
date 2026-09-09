#!/usr/bin/env python3
"""Load one generated TPC-DS slice through the MySQL protocol.

The script deliberately loads the same files into each endpoint separately;
it never copies storage data between Go and Rust TiDB.  That keeps result and
plan comparisons end-to-end and makes a failed LOAD DATA statement visible.
"""

from __future__ import annotations

import argparse
import pathlib
import time

import pymysql


TABLE_ORDER = [
    "dbgen_version",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "warehouse",
    "ship_mode",
    "time_dim",
    "reason",
    "income_band",
    "item",
    "store",
    "call_center",
    "customer",
    "web_site",
    "store_returns",
    "household_demographics",
    "web_page",
    "promotion",
    "catalog_page",
    "inventory",
    "catalog_returns",
    "web_returns",
    "web_sales",
    "catalog_sales",
    "store_sales",
]


def sql_literal(value: str) -> str:
    if value == "":
        return "NULL"
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def load_with_insert(cursor, table: str, path: pathlib.Path, batch_size: int = 100) -> None:
    rows: list[str] = []
    with path.open(encoding="utf-8", errors="strict") as source:
        for line_number, line in enumerate(source, start=1):
            fields = line.rstrip("\r\n").split("|")
            if fields and fields[-1] == "":
                fields.pop()
            if not fields:
                continue
            rows.append("(" + ",".join(sql_literal(value) for value in fields) + ")")
            if len(rows) >= batch_size:
                cursor.execute(f"INSERT INTO `{table}` VALUES " + ",".join(rows))
                rows.clear()
        if rows:
            cursor.execute(f"INSERT INTO `{table}` VALUES " + ",".join(rows))


def load(args: argparse.Namespace) -> None:
    data_dir = pathlib.Path(args.data).resolve()
    schema = data_dir / "tpcds.sql"
    if not schema.is_file():
        raise SystemExit(f"missing schema: {schema}")

    connect = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        autocommit=True,
        local_infile=True,
        charset="utf8mb4",
        read_timeout=args.timeout,
        write_timeout=args.timeout,
    )
    try:
        with connect.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS `{args.database}`")
            cursor.execute(f"CREATE DATABASE `{args.database}`")
            cursor.execute(f"USE `{args.database}`")
            schema_sql = "\n".join(
                line
                for line in schema.read_text(encoding="utf-8").splitlines()
                if not line.lstrip().startswith("--")
            )
            statements = [statement.strip() for statement in schema_sql.split(";") if statement.strip()]
            for statement in statements:
                cursor.execute(statement)

            for table in TABLE_ORDER:
                path = data_dir / f"{table}.dat"
                if not path.is_file():
                    raise SystemExit(f"missing data file: {path}")
                escaped = str(path).replace("\\", "\\\\").replace("'", "\\'")
                started = time.monotonic()
                if args.method == "load_data":
                    cursor.execute(
                        "LOAD DATA LOCAL INFILE "
                        f"'{escaped}' INTO TABLE `{table}` "
                        "FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n'"
                    )
                else:
                    load_with_insert(cursor, table, path)
                cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
                count = cursor.fetchone()[0]
                print(f"{args.port} {table} rows={count} seconds={time.monotonic() - started:.2f}", flush=True)

            if args.set_tiflash_replica:
                # Set replicas only after loading all rows.  TiFlash imports
                # the finished snapshot once per table, and running this
                # before the Rust endpoint joins the cluster avoids waiting
                # for a second schema-sync participant during each DDL.
                for table in TABLE_ORDER:
                    cursor.execute(f"ALTER TABLE `{table}` SET TIFLASH REPLICA 1")
                    print(f"{args.port} tiflash_replica table={table}", flush=True)
    finally:
        connect.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--database", default="tpcds_slice")
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--method", choices=("load_data", "insert"), default="load_data")
    parser.add_argument(
        "--set-tiflash-replica",
        action="store_true",
        help="request one TiFlash replica for every loaded table after the load",
    )
    parser.add_argument("data")
    load(parser.parse_args())


if __name__ == "__main__":
    main()
