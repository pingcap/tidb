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

"""One-command, whole-package Go-to-Rust acceptance.

There is deliberately no queue, claim, campaign, transfer ledger, generated
status transaction, or separate receipt.  A checked package proof is the only
bookkeeping artifact.  Its inventory is derived from the Go tree, and Git is
the history and rollback mechanism.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib


RUST_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = RUST_ROOT.parent
PROOF_ROOT = RUST_ROOT / "ports"
MODULE_PREFIX = "github.com/pingcap/tidb/"
GO_TEST_RE = re.compile(
    r"(?m)^func\s+(Test[A-Za-z0-9_]*|Benchmark[A-Za-z0-9_]*|"
    r"Fuzz[A-Za-z0-9_]*|Example[A-Za-z0-9_]*)\s*\("
)
GO_RUN_RE = re.compile(
    r"\.Run\s*\(\s*(?:\"((?:[^\"\\]|\\.)*)\"|`([^`]*)`)"
)
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
        # Any directory whose own files declare a Go package is independently
        # portable, even when it is more than one level below this package.
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
            continue
        # A nested directory with Go files is a different Go package. A
        # non-package directory is package-owned support (fixtures, generators,
        # embedded assets, or goldens) and must not disappear from the digest.
        if any(path.is_file() for path in child.glob("*.go")):
            continue
        files.extend(support_tree(child))
    return sorted(files)


def quoted_imports(text: str) -> set[str]:
    imports: set[str] = set()
    single = re.finditer(
        r'(?m)^import\s+(?:[._A-Za-z][A-Za-z0-9_]*\s+)?(?:"([^"]+)"|`([^`]+)`)',
        text,
    )
    imports.update(match.group(1) or match.group(2) for match in single)
    for block in re.finditer(r"(?s)\bimport\s*\((.*?)\)", text):
        imports.update(
            match.group(1) or match.group(2)
            for match in IMPORT_RE.finditer(block.group(1))
        )
    return imports


def package_inventory(package: str, extra_supports: tuple[str, ...] = ()) -> Inventory:
    directory = package_dir(package)
    files = inventory_files(directory)
    for relative in extra_supports:
        path = REPO_ROOT / relative
        if not path.is_file():
            fail(f"extra Go support does not exist: {relative}")
        files.append(path)
    files = sorted(set(files))
    source_paths = [path for path in files if path.parent == directory and path.suffix == ".go" and not path.name.endswith("_test.go")]
    test_paths = [path for path in files if path.parent == directory and path.name.endswith("_test.go")]
    support_paths = [path for path in files if path not in source_paths and path not in test_paths]

    digest = hashlib.sha256()
    for path in files:
        relative = path.relative_to(REPO_ROOT).as_posix()
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
        digest.update(b"\0")

    tests: list[str] = []
    for path in test_paths:
        text = path.read_text(encoding="utf-8")
        relative = path.relative_to(REPO_ROOT).as_posix()
        for match in GO_TEST_RE.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            tests.append(f"{relative}:{line}:{match.group(1)}")
        for match in GO_RUN_RE.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            name = match.group(1) if match.group(1) is not None else match.group(2)
            tests.append(f"{relative}:{line}:subtest:{name}")

    dependencies: set[str] = set()
    for path in source_paths:
        for imported in quoted_imports(path.read_text(encoding="utf-8")):
            if imported.startswith(MODULE_PREFIX):
                dependency = imported.removeprefix(MODULE_PREFIX)
                if dependency != package:
                    dependencies.add(dependency)

    relative = lambda path: path.relative_to(REPO_ROOT).as_posix()
    return Inventory(
        package=package,
        digest=digest.hexdigest(),
        sources=tuple(relative(path) for path in source_paths),
        test_files=tuple(relative(path) for path in test_paths),
        supports=tuple(relative(path) for path in support_paths),
        tests=tuple(sorted(set(tests))),
        dependencies=tuple(sorted(dependencies)),
    )


def proof_path(package: str) -> Path:
    return PROOF_ROOT / f"{package}.toml"


def load_proofs() -> dict[str, dict[str, object]]:
    proofs: dict[str, dict[str, object]] = {}
    if not PROOF_ROOT.exists():
        return proofs
    for path in sorted(PROOF_ROOT.rglob("*.toml")):
        with path.open("rb") as source:
            record = tomllib.load(source)
        package = record.get("go_package")
        if not isinstance(package, str) or not package:
            fail(f"{path}: missing go_package")
        if package in proofs:
            fail(f"duplicate package proof for {package}")
        record["_path"] = path
        proofs[package] = record
    return proofs


def toml_array(values: list[str] | tuple[str, ...]) -> str:
    if not values:
        return "[]"
    return "[\n" + "".join(f"  {json.dumps(value)},\n" for value in values) + "]"


def render_proof(
    inventory: Inventory,
    crates: list[str],
    rust_paths: list[str],
    test_targets: list[str],
    extra_supports: list[str],
) -> str:
    fields = [
        "# Generated by scripts/package-port.py; edit Rust mappings, not Go inventory.",
        "schema = 1",
        f"go_package = {json.dumps(inventory.package)}",
        f"go_digest = {json.dumps(inventory.digest)}",
        f"source_count = {len(inventory.sources)}",
        f"test_file_count = {len(inventory.test_files)}",
        f"test_count = {len(inventory.tests)}",
        f"support_count = {len(inventory.supports)}",
        f"rust_crates = {toml_array(sorted(set(crates)))}",
        f"rust_paths = {toml_array(sorted(set(rust_paths)))}",
        f"test_targets = {toml_array(sorted(set(test_targets)))}",
        f"extra_go_supports = {toml_array(sorted(set(extra_supports)))}",
        f"dependencies = {toml_array(inventory.dependencies)}",
        f"go_sources = {toml_array(inventory.sources)}",
        f"go_test_files = {toml_array(inventory.test_files)}",
        f"go_tests = {toml_array(inventory.tests)}",
        f"go_supports = {toml_array(inventory.supports)}",
    ]
    return "\n".join(fields) + "\n"


def string_list(record: dict[str, object], field: str, path: Path) -> list[str]:
    value = record.get(field)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        fail(f"{path}: {field} must be an array of strings")
    return list(value)


def validate_proof(record: dict[str, object], proofs: dict[str, dict[str, object]]) -> Inventory:
    path = Path(record["_path"])
    if record.get("schema") != 1:
        fail(f"{path}: expected schema 1")
    package = str(record["go_package"])
    extra_supports = tuple(string_list(record, "extra_go_supports", path))
    inventory = package_inventory(package, extra_supports)
    expected = {
        "go_digest": inventory.digest,
        "source_count": len(inventory.sources),
        "test_file_count": len(inventory.test_files),
        "test_count": len(inventory.tests),
        "support_count": len(inventory.supports),
        "dependencies": list(inventory.dependencies),
        "go_sources": list(inventory.sources),
        "go_test_files": list(inventory.test_files),
        "go_tests": list(inventory.tests),
        "go_supports": list(inventory.supports),
    }
    for field, value in expected.items():
        if record.get(field) != value:
            fail(f"{path}: stale {field}; rerun finish for {package}")

    crates = string_list(record, "rust_crates", path)
    rust_paths = string_list(record, "rust_paths", path)
    test_targets = string_list(record, "test_targets", path)
    if not crates or not rust_paths:
        fail(f"{path}: a package proof requires Rust crates and paths")
    if inventory.test_files and not test_targets:
        fail(f"{path}: package has Go tests but no Rust test target")
    for relative in rust_paths:
        candidate = REPO_ROOT / relative
        if not candidate.is_file():
            fail(f"{path}: missing Rust path {relative}")
    for target in test_targets:
        crate, separator, test = target.partition(":")
        if not separator or crate not in crates or not test:
            fail(f"{path}: invalid test target {target!r}; use crate:test")
    missing = [dependency for dependency in inventory.dependencies if dependency not in proofs]
    if missing:
        fail(f"{path}: uncovered direct Go dependencies: {', '.join(missing)}")
    return inventory


def validate_dependency_closure(
    packages: list[str], proofs: dict[str, dict[str, object]]
) -> dict[str, Inventory]:
    validated: dict[str, Inventory] = {}
    visiting: set[str] = set()

    def visit(package: str) -> None:
        if package in validated:
            return
        if package in visiting:
            fail(f"package dependency cycle reaches {package}")
        record = proofs.get(package)
        if record is None:
            fail(f"no package proof for {package}")
        visiting.add(package)
        inventory = validate_proof(record, proofs)
        for dependency in inventory.dependencies:
            visit(dependency)
        visiting.remove(package)
        validated[package] = inventory

    for package in packages:
        visit(package)
    return validated


def run(command: list[str]) -> None:
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=RUST_ROOT, check=True)


def cargo_test_executables(json_messages: str) -> list[str]:
    executables: set[str] = set()
    for line in json_messages.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (
            message.get("reason") == "compiler-artifact"
            and message.get("profile", {}).get("test") is True
            and isinstance(message.get("executable"), str)
        ):
            executables.add(message["executable"])
    return sorted(executables)


def is_aggregate_test_executable(path: str) -> bool:
    return Path(path).name.startswith("all-")


def workspace_tests() -> None:
    command = [
        "cargo",
        "test",
        "--offline",
        "--locked",
        "-j12",
        "--workspace",
        "--no-run",
        "--message-format=json",
    ]
    print("+", " ".join(command), flush=True)
    build = subprocess.run(
        command,
        cwd=RUST_ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if build.stderr:
        print(build.stderr, file=sys.stderr, end="")
    executables = cargo_test_executables(build.stdout)
    if not executables:
        fail("Cargo produced no workspace test executables")

    def execute(
        path: str, test_threads: int
    ) -> tuple[str, subprocess.CompletedProcess[str]]:
        environment = dict(os.environ)
        environment["RUST_TEST_THREADS"] = str(test_threads)
        result = subprocess.run(
            [path, "--quiet"],
            cwd=RUST_ROOT,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return path, result

    failures: list[tuple[str, subprocess.CompletedProcess[str]]] = []
    aggregates = [path for path in executables if is_aggregate_test_executable(path)]
    standalones = [path for path in executables if path not in aggregates]
    for path in aggregates:
        _, result = execute(path, 12)
        if result.returncode != 0:
            failures.append((path, result))
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = [pool.submit(execute, path, 1) for path in standalones]
        for future in as_completed(futures):
            path, result = future.result()
            if result.returncode != 0:
                failures.append((path, result))
    for path, result in failures:
        print(f"FAILED {path}", file=sys.stderr)
        print(result.stdout, file=sys.stderr, end="")
        print(result.stderr, file=sys.stderr, end="")
    if failures:
        fail(f"{len(failures)} of {len(executables)} workspace test binaries failed")
    print(
        f"workspace-tests\tpassed\tbinaries={len(executables)}\t"
        f"aggregates={len(aggregates)}\tparallel=12"
    )
    run(["cargo", "test", "--offline", "--locked", "-j12", "--workspace", "--doc", "-q"])


def finish(arguments: argparse.Namespace) -> None:
    proofs = load_proofs()
    existing = proofs.get(arguments.package)
    if existing is not None:
        path = Path(existing["_path"])
        crates = arguments.crate or string_list(existing, "rust_crates", path)
        rust_paths = arguments.rust_path or string_list(existing, "rust_paths", path)
        test_targets = arguments.test_target or string_list(existing, "test_targets", path)
        extra_supports = arguments.go_support_path or string_list(
            existing, "extra_go_supports", path
        )
    else:
        crates = arguments.crate
        rust_paths = arguments.rust_path
        test_targets = arguments.test_target
        extra_supports = arguments.go_support_path
    if not crates or not rust_paths:
        fail("first finish requires --crate and --rust-path")

    inventory = package_inventory(arguments.package, tuple(sorted(set(extra_supports))))
    missing = [dependency for dependency in inventory.dependencies if dependency not in proofs]
    if missing:
        fail("uncovered direct Go dependencies: " + ", ".join(missing))
    validate_dependency_closure(list(inventory.dependencies), proofs)
    for relative in rust_paths:
        if not (REPO_ROOT / relative).is_file():
            fail(f"missing Rust path: {relative}")
    if inventory.test_files and not test_targets:
        fail("package has Go tests; provide at least one --test-target crate:test")

    run(["cargo", "fmt", "--all", "--", "--check"])
    clippy = ["cargo", "clippy", "--offline", "--locked", "-j12"]
    for crate in sorted(set(crates)):
        clippy.extend(["-p", crate])
    run(clippy + ["--all-targets", "--", "-D", "warnings"])
    tests = ["cargo", "test", "--offline", "--locked", "-j12"]
    for crate in sorted(set(crates)):
        tests.extend(["-p", crate])
    run(tests + ["--lib"])
    for specification in sorted(set(test_targets)):
        crate, separator, target = specification.partition(":")
        if not separator or crate not in crates or not target:
            fail(f"invalid --test-target {specification!r}; use crate:test")
        run(["cargo", "test", "--offline", "--locked", "-j12", "-p", crate, "--test", target])

    path = proof_path(arguments.package)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        render_proof(inventory, crates, rust_paths, test_targets, extra_supports),
        encoding="utf-8",
    )
    print(
        f"covered\t{arguments.package}\tsources={len(inventory.sources)}\t"
        f"tests={len(inventory.tests)}\tsupports={len(inventory.supports)}"
    )


def check(package: str | None) -> None:
    proofs = load_proofs()
    selected = [package] if package else sorted(proofs)
    if package and package not in proofs:
        fail(f"no package proof for {package}")
    validated = validate_dependency_closure(selected, proofs)
    for name in selected:
        inventory = validated[name]
        print(
            f"covered\t{name}\tsources={len(inventory.sources)}\t"
            f"tests={len(inventory.tests)}\tsupports={len(inventory.supports)}"
        )


def checkpoint() -> None:
    """Run the workspace-wide proof once before push or shared-foundation changes."""
    check(None)
    run(["cargo", "fmt", "--all", "--", "--check"])
    run(
        [
            "cargo",
            "clippy",
            "--offline",
            "--locked",
            "-j12",
            "--workspace",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ]
    )
    workspace_tests()
    run([sys.executable, "-m", "unittest", "scripts/test_package_port.py"])
    run(["git", "-C", "..", "diff", "--check", "--", "rust"])


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    finish_command = commands.add_parser("finish", help="verify and record one whole package")
    finish_command.add_argument("package")
    finish_command.add_argument("--crate", action="append", default=[])
    finish_command.add_argument("--rust-path", action="append", default=[])
    finish_command.add_argument(
        "--test-target", action="append", default=[], metavar="CRATE:TARGET"
    )
    finish_command.add_argument(
        "--go-support-path", action="append", default=[], metavar="PATH"
    )
    check_command = commands.add_parser("check", help="check package proofs without compiling")
    check_command.add_argument("package", nargs="?")
    inventory_command = commands.add_parser("inventory", help="show one live Go package inventory")
    inventory_command.add_argument("package")
    commands.add_parser("checkpoint", help="run the pre-push workspace checkpoint")
    return result


def main() -> int:
    arguments = parser().parse_args()
    try:
        if arguments.command == "finish":
            finish(arguments)
        elif arguments.command == "check":
            check(arguments.package)
        elif arguments.command == "inventory":
            inventory = package_inventory(arguments.package)
            print(
                f"{inventory.package}\tsources={len(inventory.sources)}\t"
                f"test_files={len(inventory.test_files)}\ttests={len(inventory.tests)}\t"
                f"supports={len(inventory.supports)}\tdigest={inventory.digest}"
            )
            for dependency in inventory.dependencies:
                print(f"depends\t{dependency}")
        else:
            checkpoint()
        return 0
    except (OSError, subprocess.CalledProcessError, tomllib.TOMLDecodeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
