#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
"""Create immutable BR fixtures, or restore one into an unused target database."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import sys
from urllib.parse import parse_qs, urlparse

from record import Recorder, digest, now


def sql(rec, name, port, query):
    return rec.run(name, ["mysql", "--connect-timeout=10", "-h", "127.0.0.1",
                          "-P", str(port), "-u", "root", "-N", "-B", "-e", query])[0]


def table_query(db, index=False):
    source = f"{db}.rows_to_restore"
    if index:
        source += " FORCE INDEX(by_tag) WHERE tag >= 'tag_'"
    return f"SELECT id,tag,LENGTH(payload),SHA2(payload,256) FROM {source} ORDER BY id"


def freeze(root):
    entries = {str(p.relative_to(root)): digest(p) for p in sorted(root.rglob("*")) if p.is_file()}
    (root / "SHA256.json").write_text(json.dumps(entries, indent=2) + "\n")


def verify(root):
    entries = json.loads((root / "SHA256.json").read_text())
    for name, expected in entries.items():
        if digest(root / name) != expected:
            raise RuntimeError(f"fixture changed: {name}")


def prepare(args):
    root = Path(args.fixture).resolve()
    root.mkdir(parents=True, exist_ok=False, mode=0o700)
    rec = Recorder(root / "commands")
    db = args.database
    if not re.fullmatch(r"br_restore_region_[a-zA-Z0-9_]+", db):
        raise ValueError("fixture database must have the br_restore_region_ prefix")
    br = os.environ["SOURCE_BR"]
    storage = os.environ["RESTORE_REGION_STORAGE"]
    port = int(os.environ.get("SOURCE_SQL_PORT", "24000"))
    rows = 2 if args.kind == "minimal" else 32
    sql(rec, "create", port, f"CREATE DATABASE {db}; CREATE TABLE {db}.rows_to_restore "
        "(id BIGINT PRIMARY KEY,tag VARCHAR(64),payload MEDIUMTEXT,KEY by_tag(tag));")
    if args.kind == "full":
        sql(rec, "split", port, f"SPLIT TABLE {db}.rows_to_restore BETWEEN (0) AND (40000) REGIONS 4;")
    inserts = []
    for i in range(1, rows + 1):
        payload = f"REPEAT('long_{i}',1024)" if args.kind == "full" and i % 2 == 0 else f"'short_{i}'"
        inserts.append(f"INSERT INTO {db}.rows_to_restore VALUES ({i * 1000},'tag_{i}',{payload});")
    (root / "inserts.sql").write_text("\n".join(inserts) + "\n")
    sql(rec, "insert", port, "\n".join(inserts))
    expected = sql(rec, "expected", port, table_query(db))
    (root / "expected.tsv").write_text(expected)
    sql(rec, "counts", port, f"SELECT COUNT(*),SUM(id) FROM {db}.rows_to_restore")
    sql(rec, "schema", port, f"SHOW CREATE TABLE {db}.rows_to_restore")
    table_id = sql(rec, "table-id", port, "SELECT TIDB_TABLE_ID FROM information_schema.tables "
                   f"WHERE table_schema='{db}' AND table_name='rows_to_restore'").strip()
    sql(rec, "regions", port, f"SHOW TABLE {db}.rows_to_restore REGIONS")
    sql(rec, "index-regions", port, f"SHOW TABLE {db}.rows_to_restore INDEX by_tag REGIONS")
    rec.run("br-version", [br, "-V"])
    rec.run("backup", [br, "backup", "full", "--pd", os.environ["SOURCE_PD"], "--filter", db + ".*",
                       "--storage", storage, "--check-requirements=false", "--log-file", str(root / "backup.log")])
    rec.run("decode", [br, "debug", "decode", "--storage", storage, "--log-file", str(root / "decode.log")])
    url = urlparse(storage)
    endpoint = parse_qs(url.query)["endpoint"][0]
    object_url = f"s3://{url.netloc}{url.path}"
    rec.run("objects", ["aws", "--endpoint-url", endpoint, "s3", "ls", object_url + "/", "--recursive"])
    rec.run("archive-objects", ["aws", "--endpoint-url", endpoint, "s3", "sync", object_url, str(root / "objects")])
    files = []

    def walk(value):
        if isinstance(value, dict):
            if value.get("cf") in ("write", "default") and "name" in value:
                files.append(value)
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    for path in (root / "objects").rglob("*.json"):
        walk(json.loads(path.read_text()))
    (root / "backup-files.json").write_text(json.dumps(files, indent=2) + "\n")
    cfs = {f["cf"] for f in files}
    if "write" not in cfs or (args.kind == "full" and "default" not in cfs):
        raise RuntimeError(f"fixture CF coverage incomplete: {cfs}; preserve and inspect metadata")
    manifest = {"kind": args.kind, "database": db, "rows": rows, "sum_id": 1000 * rows * (rows + 1) // 2,
                "source_table_id": int(table_id), "storage": storage, "source_br": br,
                "source_br_sha256": digest(br), "created": now(), "cfs": sorted(cfs)}
    (root / "fixture.json").write_text(json.dumps(manifest, indent=2) + "\n")
    shutil.copy2(__file__, root / "fixture.py")
    freeze(root)
    print(f"FROZEN {root}: {rows} rows, CFs={sorted(cfs)}")


def restore(args):
    fixture = Path(args.fixture).resolve()
    verify(fixture)
    info = json.loads((fixture / "fixture.json").read_text())
    root = Path(args.run).resolve()
    root.mkdir(parents=True, exist_ok=False, mode=0o700)
    rec = Recorder(root / "commands")
    db = info["database"]
    port = int(os.environ.get("TARGET_SQL_PORT", "4001"))
    exists = sql(rec, "target-empty", port, "SELECT COUNT(*) FROM information_schema.schemata "
                 f"WHERE schema_name='{db}'").strip()
    if exists != "0":
        raise RuntimeError("target database exists; preserve/archive the previous target before a new attempt")
    br = os.environ["RESTORE_BR"]
    (root / "fixture-reference.json").write_text(json.dumps({"path": str(fixture),
        "manifest_sha256": digest(fixture / "SHA256.json"), "fixture": info}, indent=2) + "\n")
    rec.run("br-version", [br, "-V"])
    rec.run("restore", [br, "restore", "full", "--pd", os.environ["TARGET_PD"], "--keyspace-name",
        os.environ["TARGET_KEYSPACE"], "--filter", db + ".*", "--with-sys-table=false", "--load-stats=false",
        "--merge-region-key-count=3", "--experimental-restore-region", "--storage", info["storage"],
        "--check-requirements=false", "--checksum=true", "--log-level=debug", "--log-file", "/dev/stderr"], timeout=900)
    expected = (fixture / "expected.tsv").read_text()
    actual = sql(rec, "actual", port, table_query(db))
    (root / "actual.tsv").write_text(actual)
    if expected != actual:
        raise RuntimeError("table data differs from frozen fixture")
    counts = sql(rec, "counts", port, f"SELECT COUNT(*),SUM(id) FROM {db}.rows_to_restore").strip()
    if counts != f"{info['rows']}\t{info['sum_id']}":
        raise RuntimeError("count / primary-key sum mismatch")
    sql(rec, "index-plan", port, "EXPLAIN " + table_query(db, index=True))
    index = sql(rec, "index", port, table_query(db, index=True))
    (root / "index.tsv").write_text(index)
    if index != expected:
        raise RuntimeError("index data differs from frozen fixture")
    sql(rec, "admin-check", port, f"ADMIN CHECK TABLE {db}.rows_to_restore")
    sql(rec, "table-id", port, "SELECT TIDB_TABLE_ID FROM information_schema.tables "
        f"WHERE table_schema='{db}' AND table_name='rows_to_restore'")
    sql(rec, "regions", port, f"SHOW TABLE {db}.rows_to_restore REGIONS")
    sql(rec, "index-regions", port, f"SHOW TABLE {db}.rows_to_restore INDEX by_tag REGIONS")
    verify(fixture)
    (root / "data-checks-passed.json").write_text(json.dumps({"time": now(), "rows": info["rows"],
        "checksum": "restore command succeeded with checksum enabled",
        "remaining": "Review RPC/Worker/Apply evidence and actual Region/rewrite coverage before acceptance"}, indent=2) + "\n")
    print("DATA CHECKS PASSED; full acceptance still requires trace and coverage review")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--fixture", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--kind", choices=["minimal", "full"], required=True)
    r = sub.add_parser("restore")
    r.add_argument("--fixture", required=True)
    r.add_argument("--run", required=True)
    args = parser.parse_args()
    try:
        (prepare if args.action == "prepare" else restore)(args)
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    os.umask(0o077)
    sys.exit(main())
