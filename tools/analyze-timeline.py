#!/usr/bin/env python3
"""
analyze-timeline.py -- Extract ANALYZE TABLE timing from TiDB logs.

Usage:
    python3 tools/analyze-timeline.py tidb.log
    python3 tools/analyze-timeline.py tidb.log --table bc_user_wallet
    grep "category=stats" tidb.log | python3 tools/analyze-timeline.py
    python3 tools/analyze-timeline.py tidb.log --table bc_user_wallet --no-debug

Reads TiDB log lines and prints a timeline of ANALYZE steps with
wall-clock timestamps, delta from the previous step, and elapsed
time from the start of the ANALYZE statement.

Log lines matched (in execution order)
=======================================

Phase 1 -- Setup
  "analyze started"
    Emitted when the ANALYZE statement begins executing.
    Fields: tasks, partitionTasks, concurrency,
            needGlobalStats, sampleBasedGlobalStats

Phase 2 -- Per-partition analyze (concurrent)
  "analyze partition start"
    Emitted when a worker picks up a partition task.
    Fields: table, partition, job

  "analyze table `db`.`tbl` has finished"
  "analyze table `db`.`tbl` has failed"
    Emitted when a partition task completes (pre-existing log).
    Fields: partition, job info, start time, end time, cost,
            sample rate reason

Phase 3 -- Global stats merge
  "analyze global stats merge starting"
    Emitted after all partition tasks are done, before global merge.
    Fields: tables, sampleBasedGlobalStats

  "analyze global: loading saved samples"
    Start of loading previously-saved partition samples from storage
    (sample-based path only). Fields: tables

  "analyze global: loading saved samples for table"
    Per-table detail during loading.
    Fields: tableID, analyzed, toLoad, total

  "analyze global: all partitions freshly analyzed, skip loading"
    Logged when all partitions were just analyzed; no loading needed.
    Fields: tableID, partitions

  "analyze global: loading saved samples done"
    Loading phase complete.

  "analyze global: saving partition samples"
    Start of persisting pruned samples for freshly-analyzed partitions
    (sample-based path only). Fields: partitions

  "analyze global: saving partition samples done"
    Saving phase complete.

  "analyze global: merge entry start"
    Start of one global-stats entry (columns or one index).
    Fields: job, tableID, indexID

  "analyze global: merge entry done (sample-based)"
    Entry completed via the sample-based path.
    Fields: job, tableID

  "analyze global: merge entry using partition-merge path"
    Entry falling back to the traditional partition-merge path.
    Fields: job, tableID

  "analyze global: merge entry done (partition-merge)"
    Entry completed via the partition-merge path.
    Fields: job, tableID, error

  "use async merge global stats"
  "use blocking merge global stats"
    Logged inside the partition-merge path to indicate which
    merge algorithm is used. Fields: tableID, table

  "analyze global stats merge done"
    Global merge phase complete. Fields: tables, error

Phase 4 -- Completion
  "analyze complete"
    The ANALYZE statement is done. Fields: tasks

Optional -- DEBUGMEM lines (shown with --debug, hidden with --no-debug)
  "DEBUGMEM ..."
    Temporary debug logging for memory tracking during ANALYZE.
    Shows memory consume/release at each sub-step.
"""

import re
import sys
from datetime import datetime

# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------
# TiDB log format: [2026/02/27 10:00:00.123 +00:00]
TS_RE = re.compile(r"\[(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{3})")


def parse_ts(line):
    m = TS_RE.search(line)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y/%m/%d %H:%M:%S.%f")


def fmt_delta(td):
    if td is None:
        return "-"
    secs = td.total_seconds()
    if secs < 0:
        return "-"
    if secs < 60:
        return f"{secs:.3f}s"
    if secs < 3600:
        m = int(secs) // 60
        s = secs - m * 60
        return f"{m}m{s:.1f}s"
    h = int(secs) // 3600
    rem = secs - h * 3600
    m = int(rem) // 60
    s = int(rem) % 60
    return f"{h}h{m}m{s}s"


def short_ts(ts):
    return ts.strftime("%H:%M:%S.%f")[:12]


# ---------------------------------------------------------------------------
# Field extraction from TiDB bracketed log fields: [key=value]
# ---------------------------------------------------------------------------
def extract(line, name):
    # Match [name=...] where value runs until the closing bracket.
    m = re.search(r"\[" + re.escape(name) + r"=([^\]]*)\]", line)
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# Pattern definitions
# ---------------------------------------------------------------------------
# Each pattern: (compiled_regex, handler_function)
# Handlers return (event_string, details_string) or None to skip.

def _analyze_started(line):
    return (
        "ANALYZE STARTED",
        f"tasks={extract(line, 'tasks')} partitions={extract(line, 'partitionTasks')} "
        f"concurrency={extract(line, 'concurrency')} globalStats={extract(line, 'needGlobalStats')} "
        f"sampleBased={extract(line, 'sampleBasedGlobalStats')}",
    )


def _partition_start(line):
    tbl = extract(line, "table")
    part = extract(line, "partition")
    return (f"  partition start: {tbl}/{part}", extract(line, "job"))


def _partition_finished(line):
    part = extract(line, "partition")
    cost = extract(line, "cost")
    return (f"  partition done: {part}", f"cost={cost}")


def _partition_failed(line):
    part = extract(line, "partition")
    cost = extract(line, "cost")
    return (f"  partition FAILED: {part}", f"cost={cost}")


def _global_merge_starting(line):
    return (
        "GLOBAL MERGE STARTING",
        f"entries={extract(line, 'tables')} sampleBased={extract(line, 'sampleBasedGlobalStats')}",
    )


def _load_samples_start(line):
    return ("  load saved samples start", f"tables={extract(line, 'tables')}")


def _load_samples_skip(line):
    return (
        "  load saved samples: skip (all fresh)",
        f"tableID={extract(line, 'tableID')} partitions={extract(line, 'partitions')}",
    )


def _load_samples_table(line):
    tid = extract(line, "tableID")
    return (
        f"  load saved samples: table {tid}",
        f"analyzed={extract(line, 'analyzed')} toLoad={extract(line, 'toLoad')} total={extract(line, 'total')}",
    )


def _load_samples_done(line):
    return ("  load saved samples done", "")


def _save_samples_start(line):
    return ("  save partition samples start", f"partitions={extract(line, 'partitions')}")


def _save_samples_done(line):
    return ("  save partition samples done", "")


def _merge_entry_start(line):
    iid = extract(line, "indexID")
    label = "columns" if iid == "-1" else f"index({iid})"
    tid = extract(line, "tableID")
    return (f"  merge entry start: {label} {tid}", extract(line, "job"))


def _merge_entry_done_sample(line):
    return ("  merge entry done (sample-based)", extract(line, "job"))


def _merge_entry_fallback(line):
    return ("  merge entry fallback to partition-merge", extract(line, "job"))


def _merge_entry_done_merge(line):
    err = extract(line, "error")
    detail = extract(line, "job")
    if err and err != "<nil>":
        detail += f" ERROR={err}"
    return ("  merge entry done (partition-merge)", detail)


def _async_merge(line):
    tbl = extract(line, "table")
    return (f"    async merge: {tbl}", f"tableID={extract(line, 'tableID')}")


def _blocking_merge(line):
    tbl = extract(line, "table")
    return (f"    blocking merge: {tbl}", f"tableID={extract(line, 'tableID')}")


def _global_merge_done(line):
    err = extract(line, "error")
    detail = f"entries={extract(line, 'tables')}"
    if err and err != "<nil>":
        detail += f" ERROR={err}"
    return ("GLOBAL MERGE DONE", detail)


def _analyze_complete(line):
    return ("ANALYZE COMPLETE", f"tasks={extract(line, 'tasks')}")


def _debugmem(line):
    # Extract the message between quotes: ["DEBUGMEM ..."]
    m = re.search(r'\["(DEBUGMEM[^"]*)"', line)
    msg = m.group(1) if m else "DEBUGMEM"
    # Collect all bracketed fields after the message as details.
    # Skip common noise fields.
    skip = {"category", "caller", "ts", "level", "msg"}
    fields = re.findall(r"\[([a-zA-Z]\w*)=([^\]]*)\]", line)
    details = " ".join(f"{k}={v}" for k, v in fields if k not in skip)
    return (f"  {msg}", details)


# Order matters: more specific patterns before less specific ones.
PATTERNS = [
    (re.compile(r'"analyze started"'), _analyze_started),
    (re.compile(r'"analyze partition start"'), _partition_start),
    (re.compile(r"analyze table.*has finished"), _partition_finished),
    (re.compile(r"analyze table.*has failed"), _partition_failed),
    (re.compile(r'"analyze global stats merge starting"'), _global_merge_starting),
    # "loading saved samples" but not "done" or "for table" or "skip"
    (re.compile(r'"analyze global: loading saved samples"'), _load_samples_start),
    (re.compile(r'"analyze global: all partitions freshly analyzed'), _load_samples_skip),
    (re.compile(r'"analyze global: loading saved samples for table"'), _load_samples_table),
    (re.compile(r'"analyze global: loading saved samples done"'), _load_samples_done),
    (re.compile(r'"analyze global: saving partition samples"'), _save_samples_start),
    (re.compile(r'"analyze global: saving partition samples done"'), _save_samples_done),
    (re.compile(r'"analyze global: merge entry start"'), _merge_entry_start),
    (re.compile(r'"analyze global: merge entry done \(sample-based\)"'), _merge_entry_done_sample),
    (re.compile(r'"analyze global: merge entry using partition-merge path"'), _merge_entry_fallback),
    (re.compile(r'"analyze global: merge entry done \(partition-merge\)"'), _merge_entry_done_merge),
    (re.compile(r'"use async merge global stats"'), _async_merge),
    (re.compile(r'"use blocking merge global stats"'), _blocking_merge),
    (re.compile(r'"analyze global stats merge done"'), _global_merge_done),
    (re.compile(r'"analyze complete"'), _analyze_complete),
]

DEBUGMEM_RE = re.compile(r'"DEBUGMEM ')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract ANALYZE TABLE timing from TiDB logs."
    )
    parser.add_argument("logfile", nargs="?", help="TiDB log file (default: stdin)")
    parser.add_argument("--table", "-t", help="Only show events mentioning this table name")
    debug_group = parser.add_mutually_exclusive_group()
    debug_group.add_argument(
        "--debug", action="store_true", default=False,
        help="Include DEBUGMEM lines in output"
    )
    debug_group.add_argument(
        "--no-debug", action="store_true", default=False,
        help="Exclude DEBUGMEM lines (default)"
    )
    args = parser.parse_args()

    show_debug = args.debug and not args.no_debug

    if args.logfile:
        try:
            fh = open(args.logfile, "r", errors="replace")
        except OSError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        fh = sys.stdin

    first_ts = None
    prev_ts = None
    count = 0

    hdr_fmt = "{:<12}  {:>8}  {:>8}  {:<50}  {}"
    row_fmt = "{:<12}  {:>8}  {:>8}  {:<50}  {}"
    print(hdr_fmt.format("TIMESTAMP", "DELTA", "ELAPSED", "EVENT", "DETAILS"))
    print(
        hdr_fmt.format(
            "------------",
            "--------",
            "--------",
            "--------------------------------------------------",
            "-------",
        )
    )

    for line in fh:
        if args.table and args.table not in line:
            continue

        result = None
        for pat, handler in PATTERNS:
            if pat.search(line):
                result = handler(line)
                break

        if result is None and show_debug and DEBUGMEM_RE.search(line):
            result = _debugmem(line)

        if result is None:
            continue

        ts = parse_ts(line)
        if ts is None:
            continue

        event, details = result
        if first_ts is None:
            first_ts = ts
        delta = (ts - prev_ts) if prev_ts else None
        elapsed = ts - first_ts
        prev_ts = ts
        count += 1

        print(
            row_fmt.format(
                short_ts(ts),
                fmt_delta(delta),
                fmt_delta(elapsed),
                event,
                details,
            )
        )

    if fh is not sys.stdin:
        fh.close()

    if count == 0:
        print("(no analyze timeline events found)")
    else:
        total = fmt_delta(prev_ts - first_ts) if first_ts and prev_ts else "-"
        print(f"\n{count} events, total elapsed: {total}")


if __name__ == "__main__":
    main()
