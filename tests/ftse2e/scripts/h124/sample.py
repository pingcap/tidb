#!/usr/bin/env python3
"""Sample /proc counters for an owned client and cluster process tree."""
import json
import os
import sys
import time

client, cluster = map(int, sys.argv[1:])
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
while True:
    all_processes, owned = snapshot()
    current = all_processes.get(client)
    if current is None or current["start_ticks"] != client_start:
        break
    print(json.dumps({"time": time.time(), "processes": owned}), flush=True)
    time.sleep(1)
