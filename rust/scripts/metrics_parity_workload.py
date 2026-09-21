#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Metrics parity minimal workload driver.

Runs the identical SQL workload against a Go-master tidb-server and the Rust
SQL node, then scrapes both /metrics endpoints to files. The workload drives
every tidb.json dashboard family that a minimal SQL case can reach.

Usage: metrics_workload.py <go_mysql_port> <rust_mysql_port> <go_status_port> <rust_status_port> <outdir>
"""
import json
import subprocess
import sys
import time
import traceback

import pymysql

GO_PORT, RUST_PORT, GO_STATUS, RUST_STATUS, OUTDIR = (
    int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5],
)

STEPS = []


def step(name):
    def deco(fn):
        STEPS.append((name, fn))
        return fn
    return deco


def record(port, name, fn, journal):
    try:
        fn(port)
        journal.append({"step": name, "port": port, "ok": True})
    except Exception as exc:  # keep driving; record what failed for parity context
        journal.append({"step": name, "port": port, "ok": False, "error": repr(exc)})


def connect(port, user="root", password="", autocommit=True):
    return pymysql.connect(
        host="127.0.0.1", port=port, user=user, password=password,
        autocommit=autocommit, charset="utf8mb4",
        client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS,
    )


@step("setup_schema")
def setup_schema(port):
    conn = connect(port)
    with conn.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS mp")
        cur.execute("CREATE DATABASE mp")
        cur.execute("CREATE TABLE mp.t (id INT PRIMARY KEY, v VARCHAR(50))")
        cur.execute("CREATE TABLE mp.t2 (id INT PRIMARY KEY, v VARCHAR(50))")
        for i in range(1, 101):
            cur.execute("INSERT INTO mp.t VALUES (%s, %s)", (i, "v%d" % i))
    conn.close()


@step("queries_dml")
def queries_dml(port):
    conn = connect(port)
    with conn.cursor() as cur:
        for i in range(1, 51):
            cur.execute("SELECT v FROM mp.t WHERE id = %s", (i,))
            cur.fetchall()
        cur.execute("SELECT count(*), sum(id) FROM mp.t WHERE id < 50")
        cur.fetchall()
        cur.execute("SELECT a.v FROM mp.t a JOIN mp.t2 b ON a.id = b.id WHERE a.id < 30")
        cur.fetchall()
        cur.execute("SELECT v, count(*) FROM mp.t GROUP BY v ORDER BY v LIMIT 10")
        cur.fetchall()
        cur.execute("UPDATE mp.t SET v = CONCAT(v, '_u') WHERE id <= 10")
        cur.execute("DELETE FROM mp.t WHERE id > 95")
        cur.execute("INSERT INTO mp.t2 VALUES (1, 'x'), (2, 'y'), (3, 'z')")
    conn.close()


@step("failed_queries")
def failed_queries(port):
    for sql in ("SELEC 1", "SELECT nosuchcol FROM mp.t", "INSERT INTO mp.nosuch VALUES (1)",
                "SELECT 1/0 FROM mp.t LIMIT 1"):
        conn = connect(port)
        with conn.cursor() as cur:
            try:
                cur.execute(sql)
                cur.fetchall()
            except pymysql.MySQLError:
                pass
        conn.close()


@step("pessimistic_txns")
def pessimistic_txns(port):
    conn = connect(port)
    with conn.cursor() as cur:
        for i in range(20):
            cur.execute("BEGIN PESSIMISTIC")
            cur.execute("INSERT INTO mp.t VALUES (%s, 'txn')", (1000 + i,))
            cur.execute("SELECT * FROM mp.t WHERE id = 1 FOR UPDATE")
            cur.fetchall()
            cur.execute("COMMIT")
    conn.close()


@step("shared_locks")
def shared_locks(port):
    conn = connect(port)
    with conn.cursor() as cur:
        try:
            cur.execute("SET SESSION tidb_enable_noop_functions = 1")
        except pymysql.MySQLError:
            pass
        for i in range(5):
            cur.execute("BEGIN PESSIMISTIC")
            cur.execute("SELECT id FROM mp.t WHERE id = %s LOCK IN SHARE MODE", (i + 1,))
            cur.fetchall()
            cur.execute("COMMIT")
    conn.close()


@step("txn_heartbeat")
def txn_heartbeat(port):
    conn = connect(port)
    with conn.cursor() as cur:
        cur.execute("BEGIN PESSIMISTIC")
        cur.execute("INSERT INTO mp.t VALUES (5000, 'beat')")
        cur.execute("SELECT SLEEP(3)")
        cur.fetchall()
        cur.execute("COMMIT")
    conn.close()


@step("pipelined_flush")
def pipelined_flush(port):
    conn = connect(port)
    with conn.cursor() as cur:
        cur.execute("BEGIN PESSIMISTIC")
        for i in range(300):
            cur.execute("INSERT INTO mp.t2 VALUES (%s, 'flush')", (100 + i,))
        cur.execute("COMMIT")
    conn.close()


@step("slow_query")
def slow_query(port):
    conn = connect(port)
    with conn.cursor() as cur:
        cur.execute("SET SESSION tidb_slow_log_threshold = 1")
        cur.execute("SELECT SLEEP(0.05)")
        cur.fetchall()
        cur.execute("SELECT * FROM mp.t WHERE v LIKE 'v%%'")
        cur.fetchall()
        cur.execute("SET SESSION tidb_slow_log_threshold = 300")
    conn.close()


@step("table_cache")
def table_cache(port):
    conn = connect(port)
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE mp.t2 CACHE")
        for _ in range(3):
            cur.execute("SELECT * FROM mp.t2 LIMIT 5")
            cur.fetchall()
            time.sleep(0.6)
        cur.execute("ALTER TABLE mp.t2 NOCACHE")
    conn.close()


@step("internal_sql")
def internal_sql(port):
    conn = connect(port)
    with conn.cursor() as cur:
        cur.execute("ANALYZE TABLE mp.t")
        cur.fetchall()
        cur.execute("SELECT * FROM information_schema.tables WHERE table_schema = 'mp'")
        cur.fetchall()
    conn.close()


@step("prepared_plan_cache")
def prepared_plan_cache(port):
    conn = connect(port)
    with conn.cursor() as cur:
        cur.execute("SET SESSION tidb_enable_prepared_plan_cache = 1")
        for round_no in range(3):
            for i in range(1, 11):
                cur.execute("SELECT v FROM mp.t WHERE id = %s", (i,))
                cur.fetchall()
    conn.close()


@step("multi_statement")
def multi_statement(port):
    conn = connect(port)
    with conn.cursor() as cur:
        for i in range(5):
            cur.execute("INSERT INTO mp.t2 VALUES (%s, 'multi'); SELECT 1; SELECT 2", (500 + i,))
            cur.fetchall()
    conn.close()


@step("idle_connections")
def idle_connections(port):
    idle = connect(port)
    worker = connect(port)
    with worker.cursor() as cur:
        for i in range(10):
            cur.execute("SELECT 1")
            cur.fetchall()
            time.sleep(0.3)
    idle.close()
    worker.close()


@step("active_users")
def active_users(port):
    hold = [connect(port) for _ in range(3)]
    conn = connect(port)
    with conn.cursor() as cur:
        cur.execute("SELECT SLEEP(1)")
        cur.fetchall()
        cur.execute("SHOW PROCESSLIST")
        cur.fetchall()
    conn.close()
    for c in hold:
        c.close()


@step("handshake_errors")
def handshake_errors(port):
    for _ in range(3):
        try:
            pymysql.connect(host="127.0.0.1", port=port, user="root", password="wrongpw")
        except pymysql.MySQLError:
            pass
    for _ in range(2):
        subprocess.run(["mysql", "-h127.0.0.1", "-P", str(port), "-unosuchuser", "-e", "SELECT 1"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)


@step("show_metrics_sessions")
def show_metrics_sessions(port):
    conn = connect(port)
    with conn.cursor() as cur:
        cur.execute("SHOW STATUS LIKE 'tidb_%'")
        cur.fetchall()
    conn.close()


JOURNAL = []


def run_workload(port):
    for name, fn in STEPS:
        record(port, name, fn, JOURNAL)


def scrape(status_port, path):
    out = subprocess.run(["curl", "-sf", "http://127.0.0.1:%d/metrics" % status_port],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         universal_newlines=True, timeout=60)
    if out.returncode != 0:
        raise RuntimeError("scrape failed: %s" % out.stderr[:200])
    with open(path, "w") as handle:
        handle.write(out.stdout)
    return path


def main():
    run_workload(GO_PORT)
    run_workload(RUST_PORT)
    scrape(GO_STATUS, OUTDIR + "/go-master.metrics")
    scrape(RUST_STATUS, OUTDIR + "/rust.metrics")
    with open(OUTDIR + "/journal.json", "w") as handle:
        json.dump(JOURNAL, handle, indent=1)
    failed = [entry for entry in JOURNAL if not entry["ok"]]
    print("workload steps: %d, failures: %d" % (len(JOURNAL), len(failed)))
    for entry in failed:
        print("FAILED", entry["port"], entry["step"], entry.get("error", "")[:120])


if __name__ == "__main__":
    main()
