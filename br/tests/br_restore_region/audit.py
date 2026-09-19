#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
"""Correlate a completed full fixture restore with raw BR, Worker, and Apply logs."""

import argparse
from collections import Counter, defaultdict
import json
import os
from pathlib import Path
import re

from fixture import verify


def audit(fixture, run, evidence):
    verify(fixture)
    info = json.loads((fixture / "fixture.json").read_text())
    raw = (run / "commands/restore/stderr").read_text()
    command = json.loads((run / "commands/restore/command.json").read_text())
    assert command["exit_code"] == 0, "BR restore failed"
    assert "success in validating checksum" in raw, "checksum success missing"
    assert "snapshot restore success" in raw
    assert "all checkpoint data removed" in raw, "normal checkpoint completion missing"
    assert (run / "actual.tsv").read_bytes() == (fixture / "expected.tsv").read_bytes()
    assert (run / "index.tsv").read_bytes() == (fixture / "expected.tsv").read_bytes()
    assert "index:by_tag(tag)" in (run / "commands/index-plan/stdout").read_text()
    assert json.loads((run / "commands/admin-check/command.json").read_text())["exit_code"] == 0
    target_id = int((run / "commands/table-id/stdout").read_text().strip())
    assert target_id != info["source_table_id"], "table ID rewrite not covered"

    def operations(event):
        return [(int(m[1]), int(m[2]), int(m[3])) for line in raw.splitlines() if event in line
                for m in [re.search(r'\[sn=(\d+)\].*\[region-id=(\d+)\].*\[store-id=(\d+)\]', line)] if m]

    started = operations('"RestoreRegion started"')
    completed = operations('"RestoreRegion completed"')
    assert len(started) >= 2 and Counter(started) == Counter(completed)
    assert all(n == 1 for n in Counter(started).values()), "an operation was sent more than once"
    import_events = "\n".join(line for line in raw.splitlines() if '"sending import RPC"' in line)
    methods = Counter(re.findall(r'\[method=([A-Za-z]+)\]', import_events))
    assert methods == {"RestoreRegion": len(started)}, methods
    sources = [line for line in raw.splitlines() if '"RestoreRegion source"' in line]
    cf_counts = Counter(re.findall(r'\[cf=(\w+)\]', "\n".join(sources)))
    assert cf_counts["write"] > 0 and cf_counts["default"] > 0
    regions = {region for _, region, _ in started}
    listed_regions = (run / "commands/regions/stdout").read_text().splitlines()
    peer_counts = {int(row.split("\t")[0]): len(row.split("\t")[5].split(",")) for row in listed_regions}
    assert all(peer_counts.get(region) == 3 for region in regions)

    worker = (evidence / "after/logs/docker-compose-tikv-worker-1/tikv-worker.log").read_text()
    created_ids = set(re.findall(r'create file (\d+)\.sst', worker))
    applied = defaultdict(dict)
    apply_lines = []
    for name in ("tikv-1", "tikv-2", "tikv-3"):
        path = evidence / f"after/logs/docker-compose-{name}-1/tikv.log"
        for line in path.read_text().splitlines():
            if "kvengine apply change set" not in line or "ingest_files" not in line:
                continue
            region = re.search(r'shard_id: (\d+)', line)
            if region is None or int(region[1]) not in regions:
                continue
            file_ids = re.findall(r'table_creates \{ id: (\d+)', line)
            assert file_ids and all(file_id in created_ids for file_id in file_ids)
            applied[int(region[1])][name] = file_ids
            apply_lines.append(line)
    assert all(len(applied[region]) == 3 for region in regions), dict(applied)

    def counters(path):
        return {m[1]: float(m[2]) for line in path.read_text().splitlines()
                for m in [re.match(r'(tikv_import_rpc_duration_count\{.*\}) ([0-9.e+]+)$', line)] if m
                if re.search(r'request="(?:download|multi-ingest)"', m[1])}

    deltas = {}
    for i, port in enumerate((20181, 20182, 20183), start=1):
        before = counters(evidence / f"before-metrics-{i}/stdout")
        after = counters(evidence / f"after/commands/metrics-{port}/stdout")
        changes = {key: after.get(key, 0) - before.get(key, 0) for key in before.keys() | after.keys()}
        assert all(value == 0 for value in changes.values()), changes
        deltas[str(port)] = changes
    before_state = json.loads((evidence / "before-state/stdout").read_text())
    for original in before_state:
        name = original["Name"].lstrip("/")
        current = json.loads((evidence / f"after/commands/{name}-inspect/stdout").read_text())[0]
        assert current["Image"] == original["Image"]
        assert current["State"]["StartedAt"] == original["State"]["StartedAt"]
        assert current["RestartCount"] == original["RestartCount"]
        assert current["State"]["Health"]["Status"] == "healthy"
    result = {"data_checks": "PASS", "br_exit": 0, "source_table_id": info["source_table_id"],
              "target_table_id": target_id, "rows": info["rows"], "operations": started,
              "source_cfs": cf_counts, "rpc_methods": methods, "apply_files_by_region_and_store": dict(applied),
              "legacy_rpc_counter_deltas": deltas, "container_restarts": 0,
              "checkpoint": "enabled; normal metadata/flush/cleanup observed; resume not tested",
              "scope": "Full fixture normal path only; version provenance and error-window review remain manual"}
    out = evidence / "audit.json"
    with out.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    (evidence / "apply-evidence.txt").write_text("\n".join(apply_lines) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("run", type=Path)
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    audit(args.fixture, args.run, args.evidence)
