#!/usr/bin/env python3
"""Verify pinned Go packages against their Rust semantic tests."""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
import tomllib
from typing import Any


SPEC_FIELDS = {"go_package", "source_commit", "evidence_files", "commands"}
CARGO_SUBCOMMANDS = {"check", "test"}


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


def string_list(value: Any, label: str, *, unique: bool = True) -> list[str]:
    if not isinstance(value, list) or not value:
        raise GateError(f"{label} must be a non-empty list")
    if not all(isinstance(item, str) and item for item in value):
        raise GateError(f"{label} must contain only non-empty strings")
    if unique and len(set(value)) != len(value):
        raise GateError(f"{label} contains duplicates")
    return value


def command_list(value: Any) -> list[list[str]]:
    if not isinstance(value, list) or not value:
        raise GateError("commands must be a non-empty list")
    commands: list[list[str]] = []
    for index, command in enumerate(value):
        argv = string_list(command, f"commands[{index}]", unique=False)
        if argv[0] not in CARGO_SUBCOMMANDS:
            raise GateError(f"commands[{index}] must start with check or test")
        commands.append(argv)
    return commands


def load_spec(root: Path, spec_path: Path) -> dict[str, Any]:
    absolute = root / spec_path
    if not absolute.is_file():
        raise GateError(f"missing specification: {spec_path}")
    with absolute.open("rb") as source:
        spec = tomllib.load(source)
    unknown = sorted(set(spec) - SPEC_FIELDS)
    missing = sorted(SPEC_FIELDS - set(spec))
    if unknown:
        raise GateError(f"unknown specification fields: {unknown}")
    if missing:
        raise GateError(f"missing specification fields: {missing}")
    for key in ("go_package", "source_commit"):
        if not isinstance(spec[key], str) or not spec[key]:
            raise GateError(f"{key} must be a non-empty string")
    repo_path(spec["go_package"], "go_package")
    for value in string_list(spec["evidence_files"], "evidence_files"):
        repo_path(value, "evidence_files")
    command_list(spec["commands"])
    return spec


def direct_package_files(paths: list[str], package: str) -> list[str]:
    return sorted(path for path in paths if Path(path).parent.as_posix() == package)


def accepted_artifacts(root: Path, spec: dict[str, Any]) -> list[str]:
    package = spec["go_package"]
    commit = spec["source_commit"]
    pinned = direct_package_files(
        checked_output(root, ["git", "ls-tree", "-r", "--name-only", commit, "--", package])
        .splitlines(),
        package,
    )
    current = direct_package_files(
        checked_output(root, ["git", "ls-files", "--", package]).splitlines(), package
    )
    untracked = direct_package_files(
        checked_output(
            root, ["git", "ls-files", "--others", "--exclude-standard", "--", package]
        ).splitlines(),
        package,
    )
    if untracked:
        raise GateError(f"untracked accepted-package artifacts: {untracked}")
    if pinned != current:
        raise GateError(
            "accepted package inventory drift; "
            f"missing={sorted(set(pinned) - set(current))}, "
            f"added={sorted(set(current) - set(pinned))}"
        )
    if not pinned:
        raise GateError(f"accepted package has no tracked artifacts: {package}")
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


def require_evidence_files(root: Path, values: list[str]) -> None:
    expected = sorted(values)
    tracked = checked_output(root, ["git", "ls-files", "--", *expected]).splitlines()
    if tracked != expected:
        raise GateError(
            "evidence files must be tracked exactly; "
            f"missing={sorted(set(expected) - set(tracked))}"
        )
    for value in expected:
        if not (root / value).is_file():
            raise GateError(f"missing evidence file: {value}")


def cargo_argv(command: list[str]) -> list[str]:
    return ["cargo", command[0], "--offline", "--locked", "-j12", *command[1:]]


def verify_specs(root: Path, specs: list[tuple[Path, dict[str, Any]]], no_tests: bool) -> None:
    commands: dict[tuple[str, ...], list[str]] = {}
    for spec_path, spec in specs:
        accepted_artifacts(root, spec)
        require_evidence_files(root, string_list(spec["evidence_files"], "evidence_files"))
        for command in command_list(spec["commands"]):
            commands.setdefault(tuple(command), []).append(str(spec_path))
    if no_tests:
        return
    for command, owners in commands.items():
        argv = cargo_argv(list(command))
        result = run(root, argv, "rust")
        if result.returncode != 0:
            raise GateError(
                f"semantic command failed for {', '.join(owners)}\n"
                f"command: (cd rust && {' '.join(argv)})\n{result.stdout}"
            )


def discover_specs(root: Path) -> list[Path]:
    return sorted(path.relative_to(root) for path in (root / "rust/crates").rglob("*.semantic.toml"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("spec", nargs="*", type=Path)
    parser.add_argument("--all", action="store_true", help="verify every *.semantic.toml")
    parser.add_argument("--no-tests", action="store_true", help="only validate sources and files")
    parser.add_argument("--root", type=Path, default=Path("."))
    args = parser.parse_args()
    root = args.root.resolve()
    paths = discover_specs(root) if args.all else args.spec
    if not paths:
        parser.error("provide one or more specs, or use --all")
    normalized = [repo_path(path.as_posix(), "spec") for path in paths]
    specs = [(path, load_spec(root, path)) for path in normalized]
    verify_specs(root, specs, args.no_tests)
    command_count = len(
        {tuple(command) for _, spec in specs for command in command_list(spec["commands"])}
    )
    print(f"semantic package gate: {len(specs)} packages, {command_count} unique commands")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except GateError as error:
        print(f"semantic package gate failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
