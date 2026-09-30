#!/usr/bin/env python3
"""Inventory audit coverage, without certifying semantic parity.

Run from any directory with --go-ref <fetched-master-ref>. Review receipts live
separately: regenerating this inventory must never turn a package into accepted.
"""
import argparse
import hashlib
import json
import pathlib
import re
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "rust/docs/parity/current-audit"
CLIENT_GO = "github.com/tikv/client-go/v2"


def run(*command):
    return subprocess.check_output(command, cwd=ROOT, text=True)


def package_inventory(files):
    packages = sorted({str(pathlib.PurePosixPath(p).parent) for p in files if p.endswith(".go")})
    entries = {p: {"package": p, "review": "unreviewed-at-current-master", "artifacts": []}
               for p in packages}
    unassigned = []
    for path, digest in sorted(files.items()):
        parent = pathlib.PurePosixPath(path).parent
        while str(parent) not in entries and str(parent) != ".":
            parent = parent.parent
        target = entries.get(str(parent))
        if target is None:
            unassigned.append([path, digest])
        else:
            target["artifacts"].append([path, digest])
    return {"packages": list(entries.values()), "unassigned_artifacts": unassigned}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-ref", required=True)
    args = parser.parse_args()
    revision = run("git", "rev-parse", "--verify", f"{args.go_ref}^{{commit}}").strip()
    files = {}
    for line in run("git", "ls-tree", "-r", revision).splitlines():
        meta, path = line.split("\t", 1)
        files[path] = meta.split()[2]
    report = {"go_master": revision, "digest": "git blob object id",
              "scope": "All tracked artifacts grouped by nearest Go package directory, including build variants, generated inputs, tests and fixtures. Unassigned artifacts are retained. This is inventory, not semantic acceptance.",
              **package_inventory(files),
              "rust_crates": [{"path": str(p.parent.relative_to(ROOT)),
                               "review": "unreviewed-at-current-master"}
                              for p in sorted((ROOT / "rust/crates").glob("*/Cargo.toml"))]}
    go_mod = run("git", "show", f"{revision}:go.mod")
    versions = re.findall(r"^\s*" + re.escape(CLIENT_GO) + r"\s+(v\S+)", go_mod, re.M)
    if len(versions) != 1:
        raise ValueError("expected one client-go module pin")
    module = json.loads(run("go", "mod", "download", "-json", f"{CLIENT_GO}@{versions[0]}"))
    directory = pathlib.Path(module["Dir"])
    external = {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in directory.rglob("*") if p.is_file()}
    client_report = {"go_master": revision, "module": CLIENT_GO, "version": versions[0],
                     "digest": "sha256", **package_inventory(external)}
    OUTPUT.mkdir(parents=True, exist_ok=True)
    for name, data in [("package-coverage.json", report), ("client-go-package-coverage.json", client_report)]:
        (OUTPUT / name).write_text(json.dumps(data, indent=1) + "\n")
    pattern = re.compile(r"go-parity-gap|not implemented|not supported yet|unimplemented!")
    lines = ["path\tline\tevidence"]
    for base in [ROOT / "rust/crates", ROOT / "rust/third_party/tikv-client-rs/src"]:
        for path in sorted(base.rglob("*.rs")):
            for num, line in enumerate(path.read_text().splitlines(), 1):
                if pattern.search(line):
                    evidence = line.strip().replace("\t", " ")
                    lines.append(f"{path.relative_to(ROOT)}\t{num}\t{evidence}")
    (OUTPUT / "candidate-gaps.tsv").write_text("\n".join(lines) + "\n")
    print(f"{len(report['packages'])} TiDB package directories; {len(client_report['packages'])} client-go package directories; {len(report['rust_crates'])} Rust crates; {len(lines)-1} candidate lines (not defect counts)")


if __name__ == "__main__":
    main()
