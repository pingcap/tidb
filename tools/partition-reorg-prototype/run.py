#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Own a disposable Classic PD/TiKV cluster for the Go storage prototype."""

import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def get_json(url):
    with urllib.request.urlopen(url, timeout=2) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    installed = Path.home() / ".tiup/components"
    parser.add_argument("--pd-binary", type=Path, default=installed / "pd/v8.5.8/pd-server")
    parser.add_argument("--tikv-binary", type=Path, default=installed / "tikv/v8.5.8/tikv-server")
    parser.add_argument("--binary", type=Path, help="use an already built Go prototype")
    parser.add_argument("--production-test-binary", type=Path, help="instrumented DDL test binary for a fresh-cluster replay")
    parser.add_argument("--replay-fixtures", type=Path, help="pre-merge snapshot JSON exported by the prototype")
    parser.add_argument("--rows-per-parent", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--dml-iterations", type=int, default=64)
    parser.add_argument("--split-regions", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--keep-data", action="store_true")
    args = parser.parse_args()
    replay = args.production_test_binary is not None
    if replay != (args.replay_fixtures is not None):
        parser.error("--production-test-binary and --replay-fixtures must be supplied together")
    if replay and args.binary:
        parser.error("--binary cannot be combined with production replay")
    if replay:
        for file in (args.production_test_binary, args.replay_fixtures):
            if not file.is_file():
                parser.error(f"missing replay input {file}")
    if not 16 <= args.rows_per_parent <= 100000 or not 1 <= args.workers <= 16:
        parser.error("require 16..100000 rows per parent and 1..16 workers")
    if not 1 <= args.dml_iterations <= 10000 or not 1 <= args.timeout_seconds <= 3600:
        parser.error("require 1..10000 DML iterations and 1..3600 timeout seconds")
    for executable in (args.pd_binary, args.tikv_binary):
        if not executable.is_file():
            parser.error(f"missing binary {executable}; pass an installed Classic binary explicitly")
    base = ROOT / ".codex-tmp"
    base.mkdir(exist_ok=True)
    run_dir = Path(tempfile.mkdtemp(prefix="partition-reorg-prototype-", dir=base))
    binary = (args.production_test_binary.resolve() if replay else
              args.binary.resolve() if args.binary else run_dir / "prototype")
    go_environment = os.environ.copy()
    go_environment["GOMAXPROCS"] = "1"
    go_environment.setdefault("GOMEMLIMIT", "3GiB")
    if args.binary is None and not replay:
        subprocess.run(["go", "build", "-p", "1", "-o", str(binary), "./tools/partition-reorg-prototype"],
                       cwd=ROOT, env=go_environment, check=True)
    processes, logs = [], []
    result = {"status": "FAIL", "run_dir": str(run_dir)}
    ports = set()
    while len(ports) < 4:
        ports.add(port())
    pd_port, peer_port, kv_port, status_port = sorted(ports)
    endpoint = f"http://127.0.0.1:{pd_port}"
    pd_address = f"127.0.0.1:{pd_port}"
    pd_config = run_dir / "pd.toml"
    pd_config.write_text('[replication]\nmax-replicas = 1\n')
    kv_config = run_dir / "tikv.toml"
    kv_config.write_text('[storage]\nreserve-space = "0MB"\n[rocksdb]\nmax-open-files = 256\n[raftdb]\nmax-open-files = 256\n')

    def start(name, command):
        log = (run_dir / f"{name}.log").open("wb")
        logs.append(log)
        process = subprocess.Popen(command, cwd=run_dir, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        processes.append(process)
        return process

    try:
        versions = {}
        for name, executable in (("pd", args.pd_binary), ("tikv", args.tikv_binary)):
            versions[name] = subprocess.check_output([str(executable), "--version"], text=True, stderr=subprocess.STDOUT)
        (run_dir / "versions.json").write_text(json.dumps(versions, indent=2))
        start("pd", [str(args.pd_binary), "--name=prototype", f"--data-dir={run_dir / 'pd-data'}",
              f"--config={pd_config}", f"--client-urls={endpoint}", f"--advertise-client-urls={endpoint}",
              f"--peer-urls=http://127.0.0.1:{peer_port}", f"--advertise-peer-urls=http://127.0.0.1:{peer_port}",
              f"--initial-cluster=prototype=http://127.0.0.1:{peer_port}"])
        deadline = time.monotonic() + 90
        while True:
            if any(p.poll() is not None for p in processes):
                raise RuntimeError("owned PD exited; inspect pd.log")
            try:
                cluster = get_json(endpoint + "/pd/api/v1/cluster")
                cluster_id = cluster["id"]
                break
            except (OSError, ValueError, KeyError):
                if time.monotonic() >= deadline:
                    raise RuntimeError("owned PD readiness timeout")
                time.sleep(0.25)
        start("tikv", [str(args.tikv_binary), f"--addr=127.0.0.1:{kv_port}",
              f"--status-addr=127.0.0.1:{status_port}", f"--pd={pd_address}",
              f"--data-dir={run_dir / 'tikv-data'}", f"--config={kv_config}"])
        while True:
            if any(p.poll() is not None for p in processes):
                raise RuntimeError("owned component exited; inspect pd.log/tikv.log")
            try:
                stores = get_json(endpoint + "/pd/api/v1/stores")
                if stores.get("count") == 1 and stores["stores"][0]["store"]["state_name"] == "Up":
                    break
            except (OSError, ValueError, KeyError):
                pass
            if time.monotonic() >= deadline:
                raise RuntimeError("owned TiKV readiness timeout")
            time.sleep(0.25)
        command = [str(binary), "-pd", pd_address, "-cluster-id", str(cluster_id),
                   "-work-dir", str(run_dir / "output"), "-rows-per-parent", str(args.rows_per_parent),
                   "-workers", str(args.workers), "-dml-iterations", str(args.dml_iterations),
                   "-timeout", f"{args.timeout_seconds}s"]
        if args.split_regions:
            command.append("-split-regions")
        environment = go_environment.copy()
        if replay:
            (run_dir / "output").mkdir()
            command = [str(binary), "-test.run=^TestPartitionPrototypeProductionMerge$", "-test.count=1",
                       f"-test.timeout={args.timeout_seconds}s", "-test.v"]
            environment.update({"PARTITION_PROTO_PD": pd_address, "PARTITION_PROTO_CLUSTER_ID": str(cluster_id),
                                "PARTITION_PROTO_FIXTURES": str(args.replay_fixtures.resolve()),
                                "PARTITION_PROTO_REPORT": str(run_dir / "output/result.json")})
        (run_dir / "command.json").write_text(json.dumps(command, indent=2))
        with (run_dir / "prototype.log").open("wb") as log:
            completed = subprocess.run(command, cwd=ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT, timeout=args.timeout_seconds + 30)
        report_path = run_dir / "output/result.json"
        if report_path.exists():
            result.update(json.loads(report_path.read_text()))
        if completed.returncode != 0:
            raise RuntimeError(f"prototype exited {completed.returncode}; inspect prototype.log")
        if result["status"] != "PASS":
            raise RuntimeError("prototype returned no passing report")
    except Exception as error:
        result["status"] = "FAIL"
        result["runner_error"] = str(error)
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
        for log in logs:
            log.close()
        result["owned_processes_stopped"] = all(p.poll() is not None for p in processes)
        try:
            get_json(endpoint + "/pd/api/v1/cluster")
            result["pd_endpoint_stopped"] = False
        except (OSError, ValueError):
            result["pd_endpoint_stopped"] = True
        if not result["owned_processes_stopped"] or not result["pd_endpoint_stopped"]:
            result["status"] = "FAIL"
        if not args.keep_data:
            for name in ("pd-data", "tikv-data"):
                shutil.rmtree(run_dir / name, ignore_errors=True)
        (run_dir / "result.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
