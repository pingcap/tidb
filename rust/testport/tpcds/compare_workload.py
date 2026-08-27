#!/usr/bin/env python3
"""Compare generated TPC-DS statements over two MySQL endpoints.

The output is intentionally machine-readable so a later Rust correction can
be evaluated against exactly the same query IDs, session settings, and
fixture.  Plan text is normalized only for generated operator IDs; result
rows retain order and are hashed as JSON.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import statistics
import time
from typing import Any

import pymysql


CONCURRENCY_VARS = {
    "tidb_distsql_scan_concurrency": 1,
    "tidb_executor_concurrency": 1,
    "tidb_hash_join_concurrency": 1,
    "tidb_hashagg_final_concurrency": 1,
    "tidb_hashagg_partial_concurrency": 1,
    "tidb_index_lookup_concurrency": 1,
    "tidb_index_lookup_join_concurrency": 1,
    "tidb_index_merge_intersection_concurrency": 1,
    "tidb_projection_concurrency": 1,
    "tidb_streamagg_concurrency": 1,
    "tidb_window_concurrency": 1,
    "tidb_opt_concurrency_factor": 1,
    "tidb_opt_tiflash_concurrency_factor": 1,
}


def error_text(exc: BaseException) -> str:
    return str(exc).replace("\r", " ").replace("\n", " ")


def normalize_plan(rows: list[tuple[Any, ...]]) -> list[list[str]]:
    normalized: list[list[str]] = []
    for row in rows:
        fields = ["" if value is None else str(value) for value in row]
        if fields:
            fields[0] = re.sub(r"_[0-9]+\b", "_#", fields[0])
        normalized.append(fields)
    return normalized


def result_hash(rows: list[tuple[Any, ...]]) -> str:
    payload = json.dumps(rows, ensure_ascii=False, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def set_session(cursor, mode: str) -> list[str]:
    failures: list[str] = []
    settings = {
        "tidb_enforce_mpp": 1 if mode == "source" else 0,
        "tidb_broadcast_join_threshold_size": 0,
        "tidb_broadcast_join_threshold_count": 0,
        "tidb_isolation_read_engines": "tikv,tiflash,tidb" if mode == "source" else "tikv",
        **CONCURRENCY_VARS,
    }
    for variable, value in settings.items():
        try:
            if isinstance(value, str):
                escaped = value.replace("'", "''")
                cursor.execute(f"SET SESSION `{variable}` = '{escaped}'")
            else:
                cursor.execute(f"SET SESSION `{variable}` = {value}")
        except Exception as exc:  # keep both endpoints comparable and report unsupported vars
            failures.append(f"{variable}: {error_text(exc)}")
    cursor.execute("USE `tpcds_slice`")
    return failures


def run_endpoint(args: argparse.Namespace, port: int, mode: str) -> dict[str, Any]:
    queries = sorted(
        pathlib.Path(args.queries).glob("query_*.sql"),
        key=lambda path: int(re.search(r"query_(\d+)", path.name).group(1)),
    )
    conn = pymysql.connect(
        host=args.host,
        port=port,
        user=args.user,
        password=args.password,
        database="tpcds_slice",
        autocommit=True,
        read_timeout=args.timeout,
        write_timeout=args.timeout,
        charset="utf8mb4",
    )
    try:
        with conn.cursor() as cursor:
            setting_failures = set_session(cursor, mode)
            endpoint: dict[str, Any] = {
                "port": port,
                "mode": mode,
                "setting_failures": setting_failures,
                "queries": {},
            }
            for path in queries:
                query_id = int(re.search(r"query_(\d+)", path.name).group(1))
                sql = path.read_text(encoding="utf-8").strip().rstrip(";")
                item: dict[str, Any] = {"file": path.name}
                try:
                    cursor.execute("EXPLAIN FORMAT='plan_tree' " + sql)
                    plan_rows = cursor.fetchall()
                    normalized = normalize_plan(plan_rows)
                    item["plan_rows"] = len(normalized)
                    item["plan"] = normalized
                    item["plan_sha256"] = hashlib.sha256(
                        json.dumps(normalized, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                    ).hexdigest()
                except Exception as exc:
                    item["plan_error"] = error_text(exc)

                try:
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    item["result_rows"] = len(result)
                    item["result_sha256"] = result_hash(result)
                    item["result_preview"] = [list(row) for row in result[:3]]
                    timings: list[float] = []
                    for _ in range(args.warmups):
                        cursor.execute(sql)
                        cursor.fetchall()
                    for _ in range(args.runs):
                        started = time.perf_counter()
                        cursor.execute(sql)
                        cursor.fetchall()
                        timings.append((time.perf_counter() - started) * 1000.0)
                    if timings:
                        item["timing_ms"] = {
                            "min": min(timings),
                            "p50": statistics.median(timings),
                            "p95": sorted(timings)[max(0, int(len(timings) * 0.95) - 1)],
                            "mean": statistics.mean(timings),
                            "max": max(timings),
                        }
                except Exception as exc:
                    item["result_error"] = error_text(exc)
                endpoint["queries"][str(query_id)] = item
                print(f"port={port} mode={mode} q={query_id} plan={item.get('plan_rows', 'ERR')} result={item.get('result_rows', 'ERR')}", flush=True)
            return endpoint
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--queries", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--go-port", type=int, default=17000)
    parser.add_argument("--rust-port", type=int, default=18000)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    report: dict[str, Any] = {
        "queries": str(pathlib.Path(args.queries).resolve()),
        "mode_order": ["source", "control"],
        "go": {},
        "rust": {},
    }
    for mode in ("source", "control"):
        report["go"][mode] = run_endpoint(args, args.go_port, mode)
        report["rust"][mode] = run_endpoint(args, args.rust_port, mode)
    output = pathlib.Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
