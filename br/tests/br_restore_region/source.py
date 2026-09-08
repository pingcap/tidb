#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
"""Run an isolated fixture source; stop only its processes when STOP is created."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time
from urllib.request import Request, urlopen

from record import now


def main(root):
    root.mkdir(parents=True, exist_ok=False, mode=0o700)
    children = []

    def start(name, argv):
        out = root / name
        out.mkdir()
        argv += ["--log-file", str(out / "service.log")]
        stdout = (out / "stdout").open("wb")
        stderr = (out / "stderr").open("wb")
        process = subprocess.Popen(argv, stdout=stdout, stderr=stderr)
        meta = {"argv": argv, "cwd": os.getcwd(), "start": now(), "pid": process.pid}
        (out / "command.json").write_text(json.dumps(meta, indent=2) + "\n")
        children.append((process, stdout, stderr, out, meta))

    def ready(url):
        for _ in range(90):
            if any(p.poll() is not None for p, *_ in children):
                raise RuntimeError("source service exited during startup")
            try:
                with urlopen(url, timeout=2) as response:
                    if response.status == 200:
                        return
            except OSError:
                pass
            time.sleep(1)
        raise RuntimeError(f"source readiness timed out: {url}")

    def interrupted(signum, _frame):
        raise InterruptedError(f"source supervisor interrupted: {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        start("pd", ["bin/pd-server", "--name=br-fixture-source", "--data-dir", str(root / "pd-data"),
            "--client-urls=http://127.0.0.1:22379", "--advertise-client-urls=http://127.0.0.1:22379",
            "--peer-urls=http://127.0.0.1:22380", "--advertise-peer-urls=http://127.0.0.1:22380"])
        ready("http://127.0.0.1:22379/health")
        req = Request("http://127.0.0.1:22379/pd/api/v1/config/replicate", data=b'{"max-replicas":1}',
                      headers={"Content-Type": "application/json"})
        (root / "replication-request.json").write_text(json.dumps({"url": req.full_url, "method": "POST", "body": {"max-replicas": 1}, "time": now()}))
        with urlopen(req, timeout=10) as response:
            (root / "replication-response").write_bytes(response.read())
        start("tikv", ["bin/tikv-server", "--addr=127.0.0.1:23160", "--advertise-addr=127.0.0.1:23160",
            "--status-addr=127.0.0.1:23180", "--pd-endpoints=127.0.0.1:22379", "--data-dir", str(root / "tikv-data")])
        ready("http://127.0.0.1:23180/status")
        start("tidb", ["bin/tidb-server", "--store=tikv", "--path=127.0.0.1:22379", "--host=127.0.0.1",
            "-P=24000", "--status=20080", "--temp-dir", str(root / "tidb-tmp")])
        ready("http://127.0.0.1:20080/status")
        (root / "READY").write_text(now() + "\n")
        print("source status endpoints ready; check SQL before fixture generation", flush=True)
        while not (root / "STOP").exists():
            if any(p.poll() is not None for p, *_ in children):
                raise RuntimeError("source service exited unexpectedly")
            time.sleep(1)
    finally:
        for process, stdout, stderr, out, meta in reversed(children):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            stdout.close()
            stderr.close()
            meta.update(end=now(), exit_code=process.returncode)
            (out / "command.json").write_text(json.dumps(meta, indent=2) + "\n")
        (root / "STOPPED").write_text(now() + "\n")


if __name__ == "__main__":
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root")
    main(Path(parser.parse_args().root).resolve())
