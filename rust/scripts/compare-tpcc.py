#!/usr/bin/env python3
"""Compare input-seeded TPC-C runs, including optional repeated-baseline control.

Requires the pinned go-tpc input-seed patch in rust/benchmarks. Input seeds
do not reset data, timestamps or concurrent execution order. No schema changes.
"""
import argparse
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import time


def endpoint(value):
    try:
        name, port = value.rsplit(":", 1)
        port = int(port)
        if not re.fullmatch(r"[A-Za-z0-9_-]+", name) or not 0 < port < 65536:
            raise ValueError()
        return name, port
    except ValueError:
        raise argparse.ArgumentTypeError("expected label:port")


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--tool", type=Path, required=True)
parser.add_argument("--server", action="append", type=endpoint, required=True)
parser.add_argument("--database", required=True)
parser.add_argument("--warehouses", type=int, default=1)
parser.add_argument("--threads", type=int, nargs="+", default=[2, 8])
parser.add_argument("--count", type=int, default=10000)
parser.add_argument("--warmup-count", type=int, default=1000)
parser.add_argument("--rounds", type=int, default=4)
parser.add_argument("--seed", type=int, default=1)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
names = [name for name, _ in args.server]
if len(set(names)) != len(names) or not {"before", "after"}.issubset(names):
    parser.error("distinct labels including before and after are required")
if args.rounds < 1 or args.warehouses < 1 or len(set(args.threads)) != len(args.threads):
    parser.error("positive rounds/warehouses and distinct thread counts are required")
for threads in args.threads:
    if threads < 1 or any(count < threads or count % threads for count in (args.count, args.warmup_count)):
        parser.error("each count must be positive and divisible by each thread count")
if not -(1 << 63) <= args.seed - 1 <= args.seed + args.rounds - 1 < (1 << 63):
    parser.error("all warmup and round seeds must fit int64")
args.tool = args.tool.resolve(strict=True)
probe = subprocess.run([str(args.tool), "--help"], capture_output=True, text=True,
                       env=os.environ | {"GOTPC_TPCC_INPUT_SEED": "invalid"})
if probe.returncode == 0 or "GOTPC_TPCC_INPUT_SEED must be an int64" not in probe.stdout + probe.stderr:
    parser.error("tool does not enforce the input-seed contract")
args.output.mkdir(parents=True, exist_ok=False)
kinds = {"DELIVERY", "NEW_ORDER", "ORDER_STATUS", "PAYMENT", "STOCK_LEVEL"}
report = {"configuration": vars(args) | {"tool": str(args.tool), "output": str(args.output)},
          "note": "Input-seeded, fixed per-worker counts; data and concurrent scheduling still evolve",
          "samples": [], "warmups": []}
references = {}
try:
    for threads in args.threads:
        for round_number in range(-1, args.rounds):
            count = args.warmup_count if round_number == -1 else args.count
            seed = args.seed + round_number
            offset = round_number % len(args.server)
            for name, port in args.server[offset:] + args.server[:offset]:
                command = [str(args.tool), "tpcc", "run", "-H", "127.0.0.1", "-P", str(port),
                           "-U", "root", "-D", args.database, "--warehouses", str(args.warehouses), "-T", str(threads),
                           "--count", str(count), "--interval", "10s"]
                log = args.output / f"t{threads}-r{round_number}-{name}.log"
                with log.open("x") as stream:
                    start = time.perf_counter()
                    process = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT,
                                             env=os.environ | {"GOTPC_TPCC_INPUT_SEED": str(seed)})
                    elapsed = time.perf_counter() - start
                output = log.read_text()
                if process.returncode or re.search(r"\b(failed|error|timeout|panic)\b", output, re.I):
                    raise RuntimeError(f"invalid run: {log}")
                counts = {kind: int(value) for kind, value in re.findall(
                    r"\[Summary\] (\w+) - Takes\(s\): [\d.]+, Count: (\d+)", output)}
                if set(counts) != kinds or sum(counts.values()) != count:
                    raise RuntimeError(f"incomplete run: {log}: {counts}")
                sample = dict(server=name, threads=threads, round=round_number, seed=seed,
                              seconds=elapsed, tps=count / elapsed, counts=counts,
                              log=str(log), command=command)
                report["warmups" if round_number == -1 else "samples"].append(sample)
                if counts != references.setdefault((threads, round_number), counts):
                    raise RuntimeError(f"transaction counts differ despite equal seeds: {log}: {counts}")
                print(json.dumps({k: v for k, v in sample.items() if k != "command"}), flush=True)
                (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    report["summary"] = []
    for threads in args.threads:
        groups = {name: [s for s in report["samples"] if s["threads"] == threads and s["server"] == name]
                  for name in names}
        medians = {name: statistics.median(s["tps"] for s in samples) for name, samples in groups.items()}
        pairs = {name: [(groups[name][r]["tps"] / groups["before"][r]["tps"] - 1) * 100
                        for r in range(args.rounds)] for name in names if name != "before"}
        report["summary"].append(dict(threads=threads, median_tps=medians,
            after_median_change_percent=(medians["after"] / medians["before"] - 1) * 100,
            paired_change_vs_before_percent=pairs))
except Exception as error:
    report["failure"] = repr(error)
    raise
finally:
    (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
