#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
"""Record a command without a shell; keep raw output and exact arguments locally."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


class Recorder:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)

    def run(self, name, argv, *, check=True, timeout=None):
        # Exclusive directories prevent a rerun from overwriting failed evidence.
        out = self.root / name
        out.mkdir(mode=0o700)
        meta = {"argv": [str(x) for x in argv], "cwd": os.getcwd(), "start": now(),
                "env": {k: v for k, v in os.environ.items() if k in {
                    "PATH", "GOFLAGS", "GOCACHE", "NEXT_GEN", "AWS_ACCESS_KEY_ID",
                    "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION", "RUSTUP_TOOLCHAIN"}}}
        record = out / "command.json"
        record.write_text(json.dumps(meta, indent=2) + "\n")
        with (out / "stdout").open("wb") as stdout, (out / "stderr").open("wb") as stderr:
            try:
                result = subprocess.run(argv, stdout=stdout, stderr=stderr, timeout=timeout)
                code = result.returncode
            except subprocess.TimeoutExpired:
                code = 124
                meta["timeout"] = True
            except OSError as error:
                code = 127
                meta["launch_error"] = str(error)
        meta.update(end=now(), exit_code=code)
        meta["stdout_sha256"] = digest(out / "stdout")
        meta["stderr_sha256"] = digest(out / "stderr")
        record.write_text(json.dumps(meta, indent=2) + "\n")
        print(f"{name}: exit={code}", flush=True)
        if check and code:
            raise RuntimeError(f"{name} failed ({code}); see {out}")
        return (out / "stdout").read_text(errors="replace"), code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root")
    parser.add_argument("name")
    parser.add_argument("argv", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.argv[:1] == ["--"]:
        args.argv.pop(0)
    if not args.argv:
        parser.error("a command is required")
    _, code = Recorder(args.root).run(args.name, args.argv, check=False)
    return code if code >= 0 else 128 - code


if __name__ == "__main__":
    os.umask(0o077)
    sys.exit(main())
