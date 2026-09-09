#!/usr/bin/env python3
"""Compare running SQL servers on one prepared sysbench dataset.

Example: compare-sysbench.py --server before:53000 --server after:53001
  --server go:45000 --database sbtest --workload select_random_ranges
Servers and data are managed by the caller. No schema is created or removed.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import tempfile


def server(value):
    try:
        name, port = value.split(":")
        port = int(port)
        if not re.fullmatch(r"[A-Za-z0-9_-]+", name) or not 0 < port < 65536:
            raise ValueError()
        return name, port
    except ValueError:
        raise argparse.ArgumentTypeError("expected a label and port, e.g. before:53000")


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def metrics(output):
    result = {}
    for name, pattern in {
        "transactions": r"transactions:\s+(\d+)",
        "queries": r"queries:\s+(\d+)",
        "errors": r"ignored errors:\s+(\d+)",
        "reconnects": r"reconnects:\s+(\d+)",
        "seconds": r"total time:\s+([\d.]+)s",
        "latency_min_ms": r"min:\s+([\d.]+)",
        "latency_mean_ms": r"avg:\s+([\d.]+)",
        "latency_p95_ms": r"95th percentile:\s+([\d.]+)",
        "latency_max_ms": r"max:\s+([\d.]+)",
    }.items():
        match = re.search(pattern, output)
        if not match:
            raise ValueError(f"missing {name} in sysbench output")
        result[name] = (float(match[1]) if name == "seconds" or name.startswith("latency_")
                        else int(match[1]))
    if result["errors"] or result["reconnects"] or result["seconds"] <= 0:
        raise ValueError(f"invalid sample: {result}")
    if not result["transactions"] or not result["queries"]:
        raise ValueError("sample completed no transactions or queries")
    if (result["latency_p95_ms"] <= 0
            or not result["latency_min_ms"] <= result["latency_mean_ms"] <= result["latency_max_ms"]):
        raise ValueError("invalid latency statistics; verify the sysbench timer and histogram build")
    result["tps"] = result["transactions"] / result["seconds"]
    result["qps"] = result["queries"] / result["seconds"]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", action="append", type=server, required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--workload", action="append", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--user", default="root")
    parser.add_argument("--threads", type=positive, default=2)
    parser.add_argument("--seconds", type=positive, default=30)
    parser.add_argument("--rounds", type=positive, default=3)
    parser.add_argument("--table-size", type=positive, default=1000)
    parser.add_argument("--tables", type=positive, default=1)
    parser.add_argument("--ps-mode", choices=["auto", "disable"], default="auto")
    parser.add_argument("--sysbench", default="sysbench", help="benchmark executable path or name")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if len(args.server) < 2 or len({name for name, _ in args.server}) != len(args.server):
        parser.error("provide at least two servers with distinct labels")
    output = args.output or Path(tempfile.mkdtemp(prefix="tidb-sysbench-compare-"))
    if args.output:
        output.mkdir(parents=True, exist_ok=False)
    report = {"configuration": vars(args) | {"output": str(output)}, "samples": []}
    print(f"Artifacts: {output}", flush=True)
    try:
        executable = shutil.which(args.sysbench)
        if executable is None:
            raise ValueError(f"sysbench executable not found: {args.sysbench}")
        executable = str(Path(executable).resolve())
        report["sysbench"] = {
            "path": executable,
            "sha256": hashlib.sha256(Path(executable).read_bytes()).hexdigest(),
        }
        for workload_number, workload in enumerate(args.workload):
            for round_number in range(args.rounds):
                rotation = round_number % len(args.server)
                order = args.server[rotation:] + args.server[:rotation]
                for name, port in order:
                    command = [
                        executable, workload, "--db-driver=mysql", "--mysql-ssl=off",
                        f"--mysql-host={args.host}", f"--mysql-port={port}",
                        f"--mysql-user={args.user}", f"--mysql-db={args.database}",
                        f"--threads={args.threads}", f"--time={args.seconds}",
                        f"--table-size={args.table_size}", f"--tables={args.tables}",
                        f"--db-ps-mode={args.ps_mode}", "--rand-seed=1",
                        "--report-interval=0", "--percentile=95", "run",
                    ]
                    if "SYSBENCH_PASSWORD" in os.environ:
                        command.insert(-1, f"--mysql-password={os.environ['SYSBENCH_PASSWORD']}")
                    log = output / f"w{workload_number}-r{round_number}-{name}.log"
                    with log.open("w") as stream:
                        process = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT)
                    if process.returncode:
                        raise ValueError(f"sysbench failed ({process.returncode}); see {log}")
                    sample = metrics(log.read_text())
                    sample.update(server=name, workload=workload, round=round_number, log=str(log))
                    report["samples"].append(sample)
                    print(f"{workload} round {round_number + 1} {name}: {sample['tps']:.2f} TPS", flush=True)
        report["summary"] = []
        for workload in args.workload:
            for name, _ in args.server:
                samples = [s for s in report["samples"]
                           if s["server"] == name and s["workload"] == workload]
                values = [s["tps"] for s in samples]
                report["summary"].append(dict(
                    server=name, workload=workload, median_tps=statistics.median(values),
                    min_tps=min(values), max_tps=max(values), samples=len(values),
                    median_latency_mean_ms=statistics.median(s["latency_mean_ms"] for s in samples),
                    median_latency_p95_ms=statistics.median(s["latency_p95_ms"] for s in samples),
                ))
        print(json.dumps(report["summary"], indent=2))
    except (ValueError, OSError, KeyboardInterrupt) as error:
        report["failure"] = str(error) or "interrupted"
        raise SystemExit(report["failure"])
    finally:
        (output / "results.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
