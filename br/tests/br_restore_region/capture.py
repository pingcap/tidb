#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
"""Capture raw local component identities, configurations, and runtime evidence."""

import argparse
import json
import os
from pathlib import Path
import shutil
import tarfile

from record import Recorder, digest, now


CONTAINERS = ["docker-compose-" + name + "-1" for name in (
    "pd", "tikv-1", "tikv-2", "tikv-3", "tikv-worker", "copr-worker", "tidb-normal", "tidb-system", "minio")]


def capture(root, repositories, binaries, snapshots):
    root.mkdir(parents=True, exist_ok=False, mode=0o700)
    rec = Recorder(root / "commands")
    identities = {"time": now(), "repositories": {}, "binaries": {}, "containers": {}}
    for name, repo in repositories.items():
        head, _ = rec.run(name + "-head", ["git", "-C", repo, "rev-parse", "HEAD"])
        status, _ = rec.run(name + "-status", ["git", "-C", repo, "status", "--porcelain=v1"])
        rec.run(name + "-diff", ["git", "-C", repo, "diff", "--binary", "HEAD"])
        rec.run(name + "-log", ["git", "-C", repo, "log", "-8", "--format=fuller"])
        untracked, _ = rec.run(name + "-untracked", ["git", "-C", repo, "ls-files", "--others", "--exclude-standard"])
        with tarfile.open(root / (name + "-untracked.tar.gz"), "w:gz") as archive:
            for path in untracked.splitlines():
                archive.add(Path(repo) / path, arcname=path)
        if snapshots:
            rec.run(name + "-source", ["git", "-C", repo, "archive", "--format=tar.gz",
                "--output", str(root / (name + "-source.tar.gz")), "HEAD"])
        identities["repositories"][name] = {"path": repo, "head": head.strip(), "status": status}
    for name, path in binaries.items():
        _, code = rec.run(name + "-version", [path, "-V" if "br" in name else "--version"], check=False)
        identities["binaries"][name] = {"path": path, "sha256": digest(path), "version_exit": code}
        if snapshots:
            dest = root / "binaries" / name
            dest.parent.mkdir(exist_ok=True)
            shutil.copy2(path, dest)
    for container in CONTAINERS:
        raw, code = rec.run(container + "-inspect", ["docker", "inspect", container], check=False)
        if code:
            continue
        info = json.loads(raw)[0]
        image = info["Image"]
        identities["containers"][container] = {"image": image, "configured_image": info["Config"]["Image"]}
        rec.run(container + "-image", ["docker", "image", "inspect", image])
        rec.run(container + "-stdout", ["docker", "logs", "--timestamps", container], check=False)
        rec.run(container + "-runtime-args", ["docker", "exec", container, "cat", "/proc/1/cmdline"], check=False)
        for mount in info.get("Mounts", []):
            source = Path(mount["Source"])
            if mount["Type"] == "bind" and source.is_file():
                dest = root / "configs" / container / source.name
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, dest)
        if "tikv-worker" in container:
            binary, logfile = "/tikv-worker", "/var/log/tikv-worker"
            config = "/tmp/restore-region-worker.toml"
        elif "tikv-" in container:
            binary, logfile = "/tikv-server", "/var/log/tikv"
            config = "/tmp/restore-region-tikv.toml"
        elif "tidb-" in container:
            binary, logfile, config = "/tidb-server", None, "/etc/tidb/tidb.toml"
        elif "copr-worker" in container:
            binary, logfile, config = "/copr-worker", None, None
        elif "pd-" in container:
            binary, logfile, config = "/pd-server", "/var/log/pd", "/etc/pd/pd.toml"
        else:
            binary, logfile, config = "/opt/bin/minio", None, None
        rec.run(container + "-version", ["docker", "exec", container, binary, "--version"], check=False)
        rec.run(container + "-binary-sha", ["docker", "exec", container, "sha256sum", binary], check=False)
        if config:
            rec.run(container + "-config", ["docker", "exec", container, "cat", config], check=False)
        if logfile:
            dest = root / "logs" / container
            dest.parent.mkdir(parents=True, exist_ok=True)
            rec.run(container + "-logs", ["docker", "cp", container + ":" + logfile, str(dest)], check=False)
    for port in (20181, 20182, 20183):
        rec.run("metrics-" + str(port), ["curl", "--max-time", "10", "-fsS", f"http://127.0.0.1:{port}/metrics"], check=False)
    rec.run("target-pd-stores", ["curl", "--max-time", "10", "-fsS", "http://127.0.0.1:12379/pd/api/v1/stores"], check=False)
    rec.run("target-keyspaces", ["curl", "--max-time", "10", "-fsS", "http://127.0.0.1:12379/pd/api/v2/keyspaces/local_normal"], check=False)
    (root / "identities.json").write_text(json.dumps(identities, indent=2) + "\n")
    manifest = {str(p.relative_to(root)): digest(p) for p in sorted(root.rglob("*")) if p.is_file()}
    (root / "SHA256.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root")
    parser.add_argument("--snapshots", action="store_true")
    args = parser.parse_args()
    capture(Path(args.root).resolve(), {
        "br": "/home/hanzhen/tidb",
        "cse": "/home/hanzhen/cloud-storage-engine-converter",
        "docs": "/home/hanzhen/cloud-storage-engine",
        "kvproto-r3": "/tmp/classic-restore-r3-kvproto",
    }, {
        "source-br": "/home/hanzhen/tidb/bin/br",
        "source-tidb": "/home/hanzhen/tidb/bin/tidb-server",
        "source-tikv": "/home/hanzhen/tidb/bin/tikv-server",
        "source-pd": "/home/hanzhen/tidb/bin/pd-server",
        "restore-br": os.environ["RESTORE_BR_BINARY"],
    }, args.snapshots)
