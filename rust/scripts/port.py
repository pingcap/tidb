#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Fast current-state tracking for complete Go-package transcreation.

The Go tree is the inventory. Cargo is the test runner. Git is the history.
This script only records which source digest maps to which Rust crates.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


RUST_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = RUST_ROOT.parent
MANIFEST = RUST_ROOT / "ported-packages.json"
MODULE_PREFIX = "github.com/pingcap/tidb/"
GO_TEST_RE = re.compile(
    r"(?m)^func\s+(Test[A-Za-z0-9_]*|Benchmark[A-Za-z0-9_]*|"
    r"Fuzz[A-Za-z0-9_]*|Example[A-Za-z0-9_]*)\s*\("
)
GO_RUN_RE = re.compile(r"\.Run\s*\(\s*(?:\"((?:[^\"\\]|\\.)*)\"|`([^`]*)`)")
IMPORT_RE = re.compile(
    r'(?:^|\s)(?:[._A-Za-z][A-Za-z0-9_]*\s+)?(?:"([^"]+)"|`([^`]+)`)'
)


@dataclass(frozen=True)
class Inventory:
    package: str
    digest: str
    sources: tuple[str, ...]
    test_files: tuple[str, ...]
    supports: tuple[str, ...]
    tests: tuple[str, ...]
    dependencies: tuple[str, ...]


def fail(message: str) -> None:
    raise ValueError(message)


def package_dir(package: str) -> Path:
    relative = Path(package)
    if relative.is_absolute() or ".." in relative.parts or not package:
        fail(f"invalid repository-relative Go package: {package!r}")
    path = REPO_ROOT / relative
    if not path.is_dir():
        fail(f"Go package does not exist: {package}")
    return path


def inventory_files(directory: Path) -> list[Path]:
    files = [path for path in directory.iterdir() if path.is_file()]

    def support_tree(path: Path) -> list[Path]:
        if any(candidate.is_file() for candidate in path.glob("*.go")):
            return []
        owned = [candidate for candidate in path.iterdir() if candidate.is_file()]
        for child in sorted(candidate for candidate in path.iterdir() if candidate.is_dir()):
            if child.name == "testdata":
                owned.extend(candidate for candidate in child.rglob("*") if candidate.is_file())
            else:
                owned.extend(support_tree(child))
        return owned

    for child in sorted(path for path in directory.iterdir() if path.is_dir()):
        if child.name == "testdata":
            files.extend(path for path in child.rglob("*") if path.is_file())
        elif not any(path.is_file() for path in child.glob("*.go")):
            files.extend(support_tree(child))
    return sorted(files)


def quoted_imports(text: str) -> set[str]:
    imports = {
        match.group(1) or match.group(2)
        for match in re.finditer(
            r'(?m)^import\s+(?:[._A-Za-z][A-Za-z0-9_]*\s+)?(?:"([^"]+)"|`([^`]+)`)',
            text,
        )
    }
    for block in re.finditer(r"(?s)\bimport\s*\((.*?)\)", text):
        imports.update(
            match.group(1) or match.group(2)
            for match in IMPORT_RE.finditer(block.group(1))
        )
    return imports


def package_inventory(package: str) -> Inventory:
    directory = package_dir(package)
    files = inventory_files(directory)
    sources = [
        path
        for path in files
        if path.parent == directory
        and path.suffix == ".go"
        and not path.name.endswith("_test.go")
    ]
    test_files = [
        path
        for path in files
        if path.parent == directory and path.name.endswith("_test.go")
    ]
    supports = [path for path in files if path not in sources and path not in test_files]

    digest = hashlib.sha256()
    for path in files:
        relative = path.relative_to(REPO_ROOT).as_posix()
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
        digest.update(b"\0")

    tests: list[str] = []
    for path in test_files:
        text = path.read_text(encoding="utf-8")
        relative = path.relative_to(REPO_ROOT).as_posix()
        for match in GO_TEST_RE.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            tests.append(f"{relative}:{line}:{match.group(1)}")
        for match in GO_RUN_RE.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            name = match.group(1) if match.group(1) is not None else match.group(2)
            tests.append(f"{relative}:{line}:subtest:{name}")

    dependencies = {
        imported.removeprefix(MODULE_PREFIX)
        for path in sources
        for imported in quoted_imports(path.read_text(encoding="utf-8"))
        if imported.startswith(MODULE_PREFIX)
        and imported.removeprefix(MODULE_PREFIX) != package
    }
    relative = lambda path: path.relative_to(REPO_ROOT).as_posix()
    return Inventory(
        package=package,
        digest=digest.hexdigest(),
        sources=tuple(relative(path) for path in sources),
        test_files=tuple(relative(path) for path in test_files),
        supports=tuple(relative(path) for path in supports),
        tests=tuple(sorted(set(tests))),
        dependencies=tuple(sorted(dependencies)),
    )


def load_manifest() -> dict[str, dict[str, object]]:
    raw = json.loads(MANIFEST.read_text(encoding="utf-8"))
    if raw.get("schema") != 1 or not isinstance(raw.get("packages"), dict):
        fail(f"{MANIFEST}: expected schema 1 and packages object")
    packages = raw["packages"]
    for package, record in packages.items():
        if not isinstance(package, str) or not isinstance(record, dict):
            fail(f"{MANIFEST}: invalid package record")
        digest = record.get("digest")
        crates = record.get("crates")
        if not isinstance(digest, str) or not isinstance(crates, list) or not crates:
            fail(f"{MANIFEST}: {package} needs digest and crates")
        if not all(isinstance(crate, str) and crate for crate in crates):
            fail(f"{MANIFEST}: {package} has invalid crates")
    return packages


def write_manifest(packages: dict[str, dict[str, object]]) -> None:
    value = {"schema": 1, "packages": dict(sorted(packages.items()))}
    temporary = MANIFEST.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    temporary.replace(MANIFEST)


def validate(packages: dict[str, dict[str, object]], selected: list[str]) -> None:
    validated: set[str] = set()
    visiting: set[str] = set()

    def visit(package: str) -> None:
        if package in validated:
            return
        if package in visiting:
            fail(f"package dependency cycle reaches {package}")
        record = packages.get(package)
        if record is None:
            fail(f"no current record for dependency {package}")
        inventory = package_inventory(package)
        if record["digest"] != inventory.digest:
            fail(f"source drift in {package}; rerun record after transcreation")
        visiting.add(package)
        for dependency in inventory.dependencies:
            visit(dependency)
        visiting.remove(package)
        validated.add(package)

    for package in selected:
        visit(package)


def run(command: list[str]) -> None:
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=RUST_ROOT, check=True)


def show_inventory(inventory: Inventory, verbose: bool) -> None:
    print(
        f"{inventory.package}\tsources={len(inventory.sources)}\t"
        f"test_files={len(inventory.test_files)}\ttests={len(inventory.tests)}\t"
        f"supports={len(inventory.supports)}\tdigest={inventory.digest}"
    )
    if verbose:
        for label, values in (
            ("source", inventory.sources),
            ("test-file", inventory.test_files),
            ("test", inventory.tests),
            ("support", inventory.supports),
            ("depends", inventory.dependencies),
        ):
            for value in values:
                print(f"{label}\t{value}")
    else:
        for dependency in inventory.dependencies:
            print(f"depends\t{dependency}")


def record(package: str, crates: list[str]) -> None:
    packages = load_manifest()
    existing = packages.get(package)
    if not crates and existing is not None:
        crates = list(existing["crates"])
    crates = sorted(set(crates))
    if not crates:
        fail("first record requires at least one --package/-p Rust crate")
    inventory = package_inventory(package)
    missing = [dependency for dependency in inventory.dependencies if dependency not in packages]
    if missing:
        fail("unrecorded direct dependencies: " + ", ".join(missing))
    validate(packages, list(inventory.dependencies))

    command = ["cargo", "test", "--offline", "--locked", "-j12"]
    for crate in crates:
        command.extend(["-p", crate])
    run(command + ["--all-targets"])
    packages[package] = {"digest": inventory.digest, "crates": crates}
    write_manifest(packages)
    print(
        f"recorded\t{package}\tsources={len(inventory.sources)}\t"
        f"tests={len(inventory.tests)}\tsupports={len(inventory.supports)}"
    )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    inventory = commands.add_parser("inventory", help="read one live Go package")
    inventory.add_argument("package")
    inventory.add_argument("--verbose", action="store_true")
    record_command = commands.add_parser("record", help="test and record one complete package")
    record_command.add_argument("package")
    record_command.add_argument("--package", "-p", action="append", default=[], dest="crates")
    return result


def main() -> int:
    arguments = parser().parse_args()
    try:
        if arguments.command == "inventory":
            show_inventory(package_inventory(arguments.package), arguments.verbose)
        else:
            record(arguments.package, arguments.crates)
        return 0
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
