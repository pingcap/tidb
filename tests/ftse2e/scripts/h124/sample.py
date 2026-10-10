#!/usr/bin/env python3
"""Sample /proc counters for an owned client and cluster process tree."""
import json
import os
import sys
import time

client, cluster = map(int, sys.argv[1:3])
interval = float(sys.argv[3]) if len(sys.argv) == 4 else 1.0
if not 0.05 <= interval <= 10:
    raise SystemExit("sample interval must be 0.05..10 seconds")
hz = os.sysconf("SC_CLK_TCK")
page = os.sysconf("SC_PAGE_SIZE")


def snapshot():
    processes = {}
    for entry in os.scandir("/proc"):
        if not entry.name.isdigit():
            continue
        try:
            with open(entry.path + "/stat", encoding="utf-8") as stream:
                text = stream.read()
            end = text.rfind(")")
            fields = text[end + 2:].split()
            pid = int(entry.name)
            processes[pid] = {
                "pid": pid, "ppid": int(fields[1]),
                "name": text[text.find("(") + 1:end],
                "start_ticks": int(fields[19]),
                "cpu_seconds": (int(fields[11]) + int(fields[12])) / hz,
                "rss_bytes": int(fields[21]) * page,
            }
            try:
                # VmHWM is a process-lifetime high-water mark, not a per-query
                # allocation peak. Keep both counters to distinguish them.
                with open(entry.path + "/status", encoding="utf-8") as stream:
                    for line in stream:
                        if line.startswith("VmHWM:"):
                            processes[pid]["rss_high_water_bytes"] = int(line.split()[1]) * 1024
            except (OSError, ValueError, IndexError):
                pass
            try:
                with open(entry.path + "/io", encoding="utf-8") as stream:
                    io = dict(line.split(":", 1) for line in stream)
                processes[pid]["read_bytes"] = int(io["read_bytes"])
                processes[pid]["write_bytes"] = int(io["write_bytes"])
            except (OSError, KeyError, ValueError):
                pass
        except (OSError, ValueError, IndexError):
            continue
    selected = {client, cluster}
    while True:
        children = {pid for pid, proc in processes.items() if proc["ppid"] in selected}
        updated = selected | children
        if updated == selected:
            break
        selected = updated
    return processes, [processes[pid] for pid in sorted(selected) if pid in processes]


initial, _ = snapshot()
client_start = initial.get(client, {}).get("start_ticks")
cluster_start = initial.get(cluster, {}).get("start_ticks")
while True:
    all_processes, owned = snapshot()
    current = all_processes.get(client)
    root = all_processes.get(cluster)
    if current is None or current["start_ticks"] != client_start or root is None or root["start_ticks"] != cluster_start:
        break
    print(json.dumps({"time": time.time(), "interval_seconds": interval, "processes": owned}), flush=True)
    time.sleep(interval)
