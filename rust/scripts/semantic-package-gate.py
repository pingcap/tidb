#!/usr/bin/env python3
"""Verify pinned Go sources against their Rust implementation and tests."""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
import tomllib
from typing import Any


SPEC_SCHEMA = "semantic-package-gate-v3"
CLAIMS = {"package-seed", "whole-go-package"}
SPEC_FIELDS = {
    "schema",
    "claim",
    "go_package",
    "source_commit",
    "accepted_go_files",
    "excluded_go_paths",
    "rust_files",
    "extra_artifacts",
    "tests",
}
TEST_FIELDS = {"name", "cwd", "argv"}


class GateError(RuntimeError):
    """A fail-closed specification or verification error."""


def run(root: Path, argv: list[str], cwd: str = ".") -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=root / cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )


def checked_output(root: Path, argv: list[str]) -> str:
    result = run(root, argv)
    if result.returncode != 0:
        raise GateError(f"command failed ({result.returncode}): {' '.join(argv)}\n{result.stdout}")
    return result.stdout


def repo_path(value: str, label: str) -> Path:
    if not isinstance(value, str) or not value:
        raise GateError(f"{label} must be a non-empty repository-relative path")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or path == Path("."):
        raise GateError(f"{label} must be a repository-relative path: {value!r}")
    return path


def string_list(value: Any, label: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise GateError(f"{label} must be a list of non-empty strings")
    if len(set(value)) != len(value):
        raise GateError(f"{label} contains duplicates")
    return value


def load_spec(root: Path, spec_path: Path) -> dict[str, Any]:
    absolute = root / spec_path
    if not absolute.is_file():
        raise GateError(f"missing specification: {spec_path}")
    with absolute.open("rb") as source:
        spec = tomllib.load(source)
    if spec.get("schema") != SPEC_SCHEMA:
        raise GateError(f"unsupported schema: {spec.get('schema')!r}")
    unknown = sorted(set(spec) - SPEC_FIELDS)
    if unknown:
        raise GateError(f"unknown specification fields: {unknown}")
    for key in ("go_package", "source_commit", "claim"):
        if not isinstance(spec.get(key), str) or not spec[key]:
            raise GateError(f"missing specification field: {key}")
    if spec["claim"] not in CLAIMS:
        raise GateError(f"invalid claim: {spec['claim']!r}")
    package = repo_path(spec["go_package"], "go_package").as_posix()
    accepted = string_list(spec.get("accepted_go_files"), "accepted_go_files")
    if spec["claim"] == "package-seed" and not accepted:
        raise GateError("package-seed requires accepted_go_files")
    if spec["claim"] == "whole-go-package" and accepted:
        raise GateError("whole-go-package inventories the package; do not list accepted_go_files")
    for value in accepted:
        path = repo_path(value, "accepted_go_files").as_posix()
        if not (path == package or path.startswith(f"{package}/")):
            raise GateError(f"accepted Go file is outside {package}: {path}")
    for key in ("excluded_go_paths", "rust_files", "extra_artifacts"):
        for value in string_list(spec.get(key), key):
            repo_path(value, key)
    if not string_list(spec.get("rust_files"), "rust_files"):
        raise GateError("rust_files must name the native implementation and semantic tests")
    tests = spec.get("tests")
    if not isinstance(tests, list) or not tests:
        raise GateError("tests must contain at least one executable semantic test")
    names: set[str] = set()
    for index, test in enumerate(tests):
        if not isinstance(test, dict):
            raise GateError(f"tests[{index}] must be a table")
        unknown = sorted(set(test) - TEST_FIELDS)
        if unknown:
            raise GateError(f"tests[{index}] has unknown fields: {unknown}")
        name = test.get("name")
        if not isinstance(name, str) or not name:
            raise GateError(f"tests[{index}] has no name")
        if name in names:
            raise GateError(f"duplicate test name: {name}")
        names.add(name)
        cwd = test.get("cwd", ".")
        if cwd != ".":
            repo_path(cwd, f"{name}.cwd")
        argv = string_list(test.get("argv"), f"{name}.argv")
        if not argv:
            raise GateError(f"{name} has an empty command")
        if argv[0] == "cargo" and "-j12" not in argv:
            raise GateError(f"{name} cargo command must use -j12")
    return spec


def accepted_artifacts(root: Path, spec: dict[str, Any]) -> list[str]:
    package = spec["go_package"]
    commit = spec["source_commit"]
    excluded = string_list(spec.get("excluded_go_paths"), "excluded_go_paths")

    def included(path: str) -> bool:
        return not any(path == prefix or path.startswith(f"{prefix}/") for prefix in excluded)

    accepted = string_list(spec.get("accepted_go_files"), "accepted_go_files")
    if accepted:
        pinned = sorted(accepted)
        current = checked_output(root, ["git", "ls-files", "--", *pinned]).splitlines()
        if current != pinned:
            raise GateError(f"accepted_go_files are not tracked exactly: {current}")
    else:
        pinned = [
            path
            for path in checked_output(
                root, ["git", "ls-tree", "-r", "--name-only", commit, "--", package]
            ).splitlines()
            if included(path)
        ]
        current = [
            path
            for path in checked_output(root, ["git", "ls-files", "--", package]).splitlines()
            if included(path)
        ]
        untracked = checked_output(
            root, ["git", "ls-files", "--others", "--exclude-standard", "--", package]
        ).splitlines()
        if untracked:
            raise GateError(f"untracked accepted-package artifacts: {untracked}")
        if pinned != current:
            raise GateError(
                "accepted package inventory drift; "
                f"missing={sorted(set(pinned) - set(current))}, "
                f"added={sorted(set(current) - set(pinned))}"
            )
    for path in pinned:
        accepted_bytes = subprocess.run(
            ["git", "show", f"{commit}:{path}"],
            cwd=root,
            stdout=subprocess.PIPE,
            check=False,
        )
        if accepted_bytes.returncode != 0:
            raise GateError(f"cannot read accepted artifact: {path}")
        current_path = root / path
        if not current_path.is_file() or current_path.read_bytes() != accepted_bytes.stdout:
            raise GateError(f"accepted artifact changed from {commit}: {path}")
    return pinned


def require_files(root: Path, values: list[str], label: str) -> None:
    for value in values:
        if not (root / repo_path(value, label)).is_file():
            raise GateError(f"missing {label}: {value}")


def verify_specs(root: Path, specs: list[tuple[Path, dict[str, Any]]], no_tests: bool) -> None:
    commands: dict[tuple[str, tuple[str, ...]], list[str]] = {}
    for spec_path, spec in specs:
        accepted_artifacts(root, spec)
        require_files(root, string_list(spec["rust_files"], "rust_files"), "Rust file")
        require_files(
            root,
            string_list(spec.get("extra_artifacts"), "extra_artifacts"),
            "extra artifact",
        )
        for test in spec["tests"]:
            key = (test.get("cwd", "."), tuple(test["argv"]))
            commands.setdefault(key, []).append(f"{spec_path}:{test['name']}")
    if no_tests:
        return
    for (cwd, argv), owners in commands.items():
        result = run(root, list(argv), cwd)
        if result.returncode != 0:
            raise GateError(
                f"semantic test failed for {', '.join(owners)}\n"
                f"command: (cd {cwd} && {' '.join(argv)})\n{result.stdout}"
            )


def discover_specs(root: Path) -> list[Path]:
    return sorted(path.relative_to(root) for path in (root / "rust/crates").rglob("*.semantic.toml"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("spec", nargs="*", type=Path)
    parser.add_argument("--all", action="store_true", help="verify every *.semantic.toml")
    parser.add_argument("--no-tests", action="store_true", help="only validate boundaries and files")
    parser.add_argument("--root", type=Path, default=Path("."))
    args = parser.parse_args()
    root = args.root.resolve()
    paths = discover_specs(root) if args.all else args.spec
    if not paths:
        parser.error("provide one or more specs, or use --all")
    normalized = [repo_path(path.as_posix(), "spec") for path in paths]
    specs = [(path, load_spec(root, path)) for path in normalized]
    verify_specs(root, specs, args.no_tests)
    test_count = len({
        (test.get("cwd", "."), tuple(test["argv"]))
        for _, spec in specs
        for test in spec["tests"]
    })
    print(f"semantic package gate: {len(specs)} specs, {test_count} unique test commands")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except GateError as error:
        print(f"semantic package gate failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
