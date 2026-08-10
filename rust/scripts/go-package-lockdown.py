#!/usr/bin/env python3
"""Generate and verify one complete Go-package lockdown receipt.

The checker deliberately has no package-specific policy. Human decisions live
in package.toml and the TSV evidence files beside it; Go syntax comes from the
checked-in go_package_lockdown_inventory tool.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shlex
import subprocess
import sys
import tempfile
import tomllib
from typing import Iterable


CHECKER_SCHEMA = "go-package-lockdown-checker-v2"
SPEC_SCHEMA = "go-package-lockdown-spec-v2"
LEDGER_SCHEMA = "go-package-lockdown-ledger-v2"
ARTIFACT_SCHEMA = "go-package-lockdown-artifacts-v1"
RECEIPT_SCHEMA = "go-package-lockdown-receipt-v2"
EVIDENCE_SCHEMA = "go-package-lockdown-evidence-v1"
OBSERVATION_SCHEMA = "go-package-lockdown-observation-v1"
RUNTIME_OBSERVATION_SCHEMA = "go-package-lockdown-runtime-observation-v1"
MUTATION_OPERATOR_SCHEMA = "go-package-lockdown-mutation-operator-v1"
MUTATION_ATTEMPT_PLAN_SCHEMA = "go-package-lockdown-mutation-attempt-plan-v1"
MUTATION_RUN_SCHEMA = "go-package-lockdown-mutation-run-v1"
EVIDENCE_RUN_SCHEMA = "go-package-lockdown-evidence-run-v1"
MUTATION_HISTORY_SCHEMA = "go-package-lockdown-mutation-history-v1"
GO_INVENTORY_TOOL = Path("rust/difftests/tools/go_package_lockdown_inventory")
GO_FIXTURE_TOOL = Path("rust/difftests/tools/go_test_fixture_inventory")
GO_HELPER_CALL_TOOL = Path("rust/difftests/tools/go_test_helper_call_inventory")
GO_EMBED_TOOL = Path("rust/difftests/tools/go_package_embed_inventory")

ARTIFACT_HEADER = [
    "path", "role", "traits", "sha256", "bytes", "lines", "source_blob_oid",
]
LEDGER_HEADER = [
    "obligation_id",
    "category",
    "source_path",
    "ast_anchor",
    "node_sha256",
    "owner",
    "source_blob_sha256",
    "status",
    "symbol_id",
    "evidence",
    "rule_id",
]
SYMBOL_HEADER = [
    "symbol_id", "rust_crate", "rust_symbol", "definition_path", "anchor_path",
    "anchor_target", "anchor_name",
]
RULE_HEADER = [
    "rule_id",
    "cluster_id",
    "description",
    "obligation_ids",
    "boundary_cases",
    "mutation_ids",
]
MUTATION_PLAN_HEADER = [
    "mutation_id",
    "cluster_id",
    "rule_ids",
    "baseline_commit",
    "rust_path",
    "source_sha256",
    "runner",
    "test_subject",
    "test_target",
    "named_test",
    "operator_path",
    "operator_sha256",
]
MUTATION_RESULT_HEADER = [
    "sequence",
    "attempt_id",
    "mutation_id",
    "prior_history_sha256",
    "prior_checkpoint_sha256",
    "attempt_plan_path",
    "attempt_plan_sha256",
    "run_artifact_path",
    "run_artifact_sha256",
    "verification_artifact_path",
    "verification_artifact_sha256",
    "history_sha256",
]
PROBE_RESULT_HEADER = [
    "probe_id",
    "run_artifact_path",
    "run_artifact_sha256",
    "verification_artifact_path",
    "verification_artifact_sha256",
]
HELPER_CALL_HEADER = [
    "call_id", "source_path", "source_line", "source_column", "callee", "call_node_sha256",
    "fixture_api", "first_argument",
]
HELPER_CONTRACT_HEADER = [
    "helper_id", "callee", "call_ids", "call_set_sha256", "status", "evidence",
]
EMBED_HEADER = [
    "source_path", "source_line", "source_column", "patterns",
]
ALLOWED_HELPER_STATUSES = {"DIRECT-FIXTURE", "NO-FIXTURE", "FIXTURE"}

ALLOWED_ARTIFACT_ROLES = {
    "production-go",
    "test-go",
    "generated-production-go",
    "generated-test-go",
    "build",
    "fixture",
    "generated-input",
    "generated-output",
    "support",
}
ALLOWED_VERDICTS = {"PORTED", "DECLINED", "UNREACHABLE"}
SPEC_FIELDS = {
    "schema", "claim", "go_package", "source_commit", "primary_rust_crate",
    "mapped_rust_crates", "extra_artifacts", "owned_rust_files",
    "excluded_subpackages", "artifact_roles", "unresolved_fixture_evidence",
}
PLATFORM_PARTS = {
    "aix", "android", "darwin", "dragonfly", "freebsd", "illumos", "ios",
    "js", "linux", "netbsd", "openbsd", "plan9", "solaris", "wasip1",
    "windows", "386", "amd64", "arm", "arm64", "loong64", "mips",
    "mips64", "mips64le", "mipsle", "ppc64", "ppc64le", "riscv64",
    "s390x", "wasm",
}


class LockdownError(RuntimeError):
    """A deterministic, user-actionable lockdown validation failure."""


def run(root: Path, command: list[str]) -> str:
    try:
        completed = subprocess.run(
            command,
            cwd=root,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as error:
        detail = error.stderr.strip() or error.stdout.strip() or f"exit {error.returncode}"
        raise LockdownError(f"command failed: {' '.join(command)}: {detail}") from error
    return completed.stdout


def run_bytes(root: Path, command: list[str]) -> bytes:
    try:
        completed = subprocess.run(
            command,
            cwd=root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as error:
        detail = (error.stderr or error.stdout).decode("utf-8", errors="replace").strip()
        raise LockdownError(f"command failed: {' '.join(command)}: {detail}") from error
    return completed.stdout


def repository_path(value: str, field: str) -> Path:
    if not isinstance(value, str) or not value:
        raise LockdownError(f"{field} must be a non-empty repository-relative path")
    pure = PurePosixPath(value)
    if pure.is_absolute() or value != pure.as_posix() or ".." in pure.parts or value == ".":
        raise LockdownError(f"unsafe {field}: {value!r}")
    return Path(*pure.parts)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def canonical_json_text(value: object) -> str:
    return canonical_json_bytes(value).decode("utf-8") + "\n"


def strict_json_loads(data: str, context: str) -> object:
    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise LockdownError(f"{context} contains duplicate JSON key {key!r}")
            result[key] = value
        return result

    try:
        return json.loads(data, object_pairs_hook=unique_object)
    except json.JSONDecodeError as error:
        raise LockdownError(f"cannot parse {context}: {error}") from error


def read_strict_json(path: Path, context: str, *, canonical: bool = True) -> object:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as error:
        raise LockdownError(f"cannot read {context} {path}: {error}") from error
    payload = strict_json_loads(raw, f"{context} {path}")
    if canonical and raw != canonical_json_text(payload):
        raise LockdownError(f"{context} is not exact canonical JSON: {path}")
    return payload


def source_lines(path: Path) -> int:
    data = path.read_bytes()
    return data.count(b"\n") + (1 if data and not data.endswith(b"\n") else 0)


def split_list(value: str) -> list[str]:
    return [item for item in value.split(";") if item]


def command_output(stdout: bytes, stderr: bytes) -> bytes:
    return b"--- stdout ---\n" + stdout + b"\n--- stderr ---\n" + stderr


def parse_command_output(data: bytes, context: str) -> tuple[bytes, bytes]:
    prefix = b"--- stdout ---\n"
    separator = b"\n--- stderr ---\n"
    if not data.startswith(prefix) or separator not in data:
        raise LockdownError(f"{context} output is not canonical")
    stdout, stderr = data[len(prefix):].split(separator, 1)
    return stdout, stderr


def normalized_test_observation(
    runner: str, named_test: str, exit_code: int, stdout: bytes, stderr: bytes
) -> str:
    text = command_output(stdout, stderr).decode("utf-8", errors="replace")
    status = "PASS" if exit_code == 0 else "FAIL"
    if runner in {"cargo-test", "cargo-test-pretty"}:
        pass_marker = rf"(?m)^test {re.escape(named_test)} \.\.\. ok$"
        fail_marker = rf"(?m)^test {re.escape(named_test)} \.\.\. FAILED$"
    elif runner == "go-test":
        pass_marker = rf"(?m)^--- PASS: {re.escape(named_test)}(?: \([^\n]*\))?$"
        fail_marker = rf"(?m)^--- FAIL: {re.escape(named_test)}(?: \([^\n]*\))?$"
        start_marker = rf"(?m)^=== RUN   {re.escape(named_test)}$"
    else:
        raise LockdownError(f"cannot normalize unknown runner {runner!r}")
    expected = pass_marker if status == "PASS" else fail_marker
    opposite = fail_marker if status == "PASS" else pass_marker
    if (
        len(re.findall(expected, text)) != 1
        or re.search(opposite, text) is not None
        or (runner == "go-test" and len(re.findall(start_marker, text)) != 1)
    ):
        raise LockdownError(
            f"fixed runner output does not prove exact named test {named_test} {status.lower()}ed"
        )
    normalized = f"{runner}\0{named_test}\0{status}\n".encode("utf-8")
    return hashlib.sha256(normalized).hexdigest()


def validate_tsv_field(value: str, context: str) -> None:
    if "\t" in value or "\n" in value or "\r" in value:
        raise LockdownError(f"{context} contains a tab or newline")


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as output:
        output.write(text)
        temporary = Path(output.name)
    temporary.replace(path)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as output:
        output.write(data)
        temporary = Path(output.name)
    temporary.replace(path)


def go_package_name(data: bytes, path: Path) -> str:
    """Read the mandatory leading Go package clause without matching comments."""
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise LockdownError(f"Go source is not UTF-8: {path}") from error
    index = 0
    while index < len(text):
        if text[index].isspace():
            index += 1
            continue
        if text.startswith("//", index):
            newline = text.find("\n", index + 2)
            index = len(text) if newline < 0 else newline + 1
            continue
        if text.startswith("/*", index):
            end = text.find("*/", index + 2)
            if end < 0:
                raise LockdownError(f"unterminated Go block comment: {path}")
            index = end + 2
            continue
        break
    match = re.match(r"package\s+([A-Za-z_][A-Za-z0-9_]*)\b", text[index:])
    if match is None:
        raise LockdownError(f"Go source has no leading package clause: {path}")
    return match.group(1)


def rust_tokens(text: str) -> list[str]:
    """Tokenize identifiers/punctuation while discarding Rust comments and literals."""
    tokens: list[str] = []
    index = 0
    length = len(text)
    while index < length:
        if text[index].isspace():
            index += 1
            continue
        if text.startswith("//", index):
            newline = text.find("\n", index + 2)
            index = length if newline < 0 else newline + 1
            continue
        if text.startswith("/*", index):
            depth = 1
            index += 2
            while index < length and depth:
                if text.startswith("/*", index):
                    depth += 1
                    index += 2
                elif text.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            if depth:
                raise LockdownError("unterminated Rust block comment in compile anchor")
            continue
        raw = re.match(r"(?:br|rb|r)(?P<hashes>#{0,255})\"", text[index:])
        if raw is not None:
            delimiter = '"' + raw.group("hashes")
            start = index + raw.end()
            end = text.find(delimiter, start)
            if end < 0:
                raise LockdownError("unterminated Rust raw string in compile anchor")
            index = end + len(delimiter)
            continue
        prefix = 2 if text.startswith('b"', index) else 1 if text[index] == '"' else 0
        if prefix:
            index += prefix
            escaped = False
            while index < length:
                char = text[index]
                index += 1
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    break
            else:
                raise LockdownError("unterminated Rust string in compile anchor")
            continue
        character = re.match(r"(?:b)?'(?:\\.|[^'\\\n])'", text[index:])
        if character is not None:
            index += character.end()
            continue
        identifier = re.match(r"[A-Za-z_][A-Za-z0-9_]*", text[index:])
        if identifier is not None:
            tokens.append(identifier.group(0))
            index += identifier.end()
            continue
        if text.startswith("::", index):
            tokens.append("::")
            index += 2
            continue
        tokens.append(text[index])
        index += 1
    return tokens


def contains_token_sequence(tokens: list[str], expected: list[str]) -> bool:
    return any(tokens[index:index + len(expected)] == expected for index in range(len(tokens)))


def rust_executable_symbol_use(tokens: list[str], expected: list[str]) -> bool:
    """Require a qualified symbol to participate in an executable expression.

    Merely binding or naming a path (`let _ = crate::symbol;` or
    `crate::symbol;`) does not exercise production behavior and cannot be a
    boundary-test anchor.
    """
    for index in range(len(tokens) - len(expected) + 1):
        if tokens[index:index + len(expected)] != expected:
            continue
        after = index + len(expected)
        if after >= len(tokens) or tokens[after] == ";":
            continue
        return True
    return False


def rust_matching_brace(tokens: list[str], opening: int) -> int:
    depth = 1
    cursor = opening + 1
    while cursor < len(tokens) and depth:
        if tokens[cursor] == "{":
            depth += 1
        elif tokens[cursor] == "}":
            depth -= 1
        cursor += 1
    if depth:
        raise LockdownError("unterminated Rust declaration body")
    return cursor - 1


def rust_impl_target(tokens: list[str]) -> tuple[str, ...] | None:
    segment = tokens
    if "for" in segment:
        segment = segment[segment.index("for") + 1:]
    if "where" in segment:
        segment = segment[:segment.index("where")]
    angle = 0
    path: list[str] = []
    pending_separator = False
    for token in segment:
        if token == "<":
            angle += 1
        elif token == ">" and angle:
            angle -= 1
        elif angle == 0 and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token) and token not in {
            "impl", "unsafe", "const", "dyn", "mut",
        }:
            if path and not pending_separator:
                path = []
            path.append(token)
            pending_separator = False
        elif angle == 0 and token == "::" and path:
            pending_separator = True
        elif angle == 0 and path:
            path = []
            pending_separator = False
    return tuple(path) if path else None


def rust_resolve_relative_identity(
    scope: tuple[str, ...], path: tuple[str, ...]
) -> tuple[str, ...]:
    if path[0] == "crate":
        return path
    resolved = list(scope)
    cursor = 0
    while cursor < len(path) and path[cursor] in {"self", "super"}:
        if path[cursor] == "super":
            if len(resolved) <= 1:
                raise LockdownError("Rust impl target escapes its crate module")
            resolved.pop()
        cursor += 1
    return (*resolved, *path[cursor:])


def rust_declared_identities(
    tokens: list[str], module_path: tuple[str, ...]
) -> set[tuple[str, ...]]:
    declarations: set[tuple[str, ...]] = set()

    def scan(start: int, end: int, scope: tuple[str, ...]) -> None:
        cursor = start
        while cursor < end:
            token = tokens[cursor]
            if token == "mod" and cursor + 1 < end and re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]*", tokens[cursor + 1]
            ):
                name = tokens[cursor + 1]
                declarations.add(scope + (name,))
                opening = cursor + 2
                while opening < end and tokens[opening] not in {"{", ";"}:
                    opening += 1
                if opening < end and tokens[opening] == "{":
                    closing = rust_matching_brace(tokens, opening)
                    scan(opening + 1, closing, scope + (name,))
                    cursor = closing + 1
                    continue
            if token in {"trait", "struct", "enum"} and cursor + 1 < end and re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]*", tokens[cursor + 1]
            ):
                name = tokens[cursor + 1]
                declarations.add(scope + (name,))
                opening = cursor + 2
                while opening < end and tokens[opening] not in {"{", ";"}:
                    opening += 1
                if token == "trait" and opening < end and tokens[opening] == "{":
                    closing = rust_matching_brace(tokens, opening)
                    scan(opening + 1, closing, scope + (name,))
                    cursor = closing + 1
                    continue
            if token == "impl":
                opening = cursor + 1
                while opening < end and tokens[opening] != "{":
                    opening += 1
                if opening < end:
                    target = rust_impl_target(tokens[cursor + 1:opening])
                    closing = rust_matching_brace(tokens, opening)
                    if target is not None:
                        scan(
                            opening + 1, closing,
                            rust_resolve_relative_identity(scope, target),
                        )
                    cursor = closing + 1
                    continue
            if token in {"fn", "type", "const", "static"} and cursor + 1 < end and re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]*", tokens[cursor + 1]
            ):
                declarations.add(scope + (tokens[cursor + 1],))
                if token == "fn":
                    opening = cursor + 2
                    while opening < end and tokens[opening] not in {"{", ";"}:
                        opening += 1
                    if opening < end and tokens[opening] == "{":
                        cursor = rust_matching_brace(tokens, opening) + 1
                        continue
            cursor += 1

    scan(0, len(tokens), module_path)
    return declarations


def rust_test_function_bodies(tokens: list[str]) -> dict[str, list[str]]:
    functions: dict[str, list[str]] = {}
    qualifiers = {"pub", "crate", "super", "self", "async", "unsafe", "extern", "const", "(", ")"}

    def scan(start: int, end: int, scope: tuple[str, ...]) -> None:
        index = start
        while index < end - 1:
            if tokens[index] == "mod" and re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]*", tokens[index + 1]
            ):
                opening = index + 2
                while opening < end and tokens[opening] not in {"{", ";"}:
                    opening += 1
                if opening < end and tokens[opening] == "{":
                    closing = rust_matching_brace(tokens, opening)
                    scan(opening + 1, closing, scope + (tokens[index + 1],))
                    index = closing + 1
                    continue
            if tokens[index] != "fn" or not re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]*", tokens[index + 1]
            ):
                index += 1
                continue
            cursor = index - 1
            while cursor >= start and tokens[cursor] in qualifiers:
                cursor -= 1
            found_test = False
            while cursor >= start and tokens[cursor] == "]":
                depth = 1
                attribute_start = cursor - 1
                while attribute_start >= start and depth:
                    if tokens[attribute_start] == "]":
                        depth += 1
                    elif tokens[attribute_start] == "[":
                        depth -= 1
                    attribute_start -= 1
                if depth or attribute_start < start or tokens[attribute_start] != "#":
                    break
                if tokens[attribute_start + 2:cursor] == ["test"]:
                    found_test = True
                cursor = attribute_start - 1
                while cursor >= start and tokens[cursor] in qualifiers:
                    cursor -= 1
            body_start = index + 2
            while body_start < end and tokens[body_start] not in {"{", ";"}:
                body_start += 1
            if body_start >= end or tokens[body_start] != "{":
                index = body_start + 1
                continue
            body_end = rust_matching_brace(tokens, body_start)
            if found_test:
                identity = "::".join((*scope, tokens[index + 1]))
                if identity in functions:
                    raise LockdownError(f"duplicate Rust test identity: {identity}")
                functions[identity] = tokens[body_start + 1:body_end]
            index = body_end + 1

    scan(0, len(tokens), ())
    return functions


def code_mask(text: str) -> str:
    """Mask C-style comments and literals while preserving character offsets."""
    output = list(text)
    index = 0
    while index < len(text):
        if text.startswith("//", index):
            end = text.find("\n", index + 2)
            end = len(text) if end < 0 else end
            output[index:end] = " " * (end - index)
            index = end
            continue
        if text.startswith("/*", index):
            end = text.find("*/", index + 2)
            if end < 0:
                raise LockdownError("unterminated block comment in test anchor")
            end += 2
            output[index:end] = " " * (end - index)
            index = end
            continue
        raw = re.match(r"(?:br|rb|r)?(?P<hashes>#{0,255})\"", text[index:])
        if raw is not None:
            delimiter = '"' + raw.group("hashes")
            end = text.find(delimiter, index + raw.end())
            if end < 0:
                raise LockdownError("unterminated raw string in test anchor")
            end += len(delimiter)
            output[index:end] = " " * (end - index)
            index = end
            continue
        if text[index] in {'"', '`'} or text.startswith('b"', index):
            quote = text[index] if text[index] in {'"', '`'} else '"'
            cursor = index + (2 if text.startswith('b"', index) else 1)
            escaped = False
            while cursor < len(text):
                char = text[cursor]
                cursor += 1
                if quote == '`':
                    if char == '`':
                        break
                elif escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == quote:
                    break
            else:
                raise LockdownError("unterminated string in test anchor")
            output[index:cursor] = " " * (cursor - index)
            index = cursor
            continue
        index += 1
    return "".join(output)


def has_static_observation_literal(text: str) -> bool:
    """Reject observation payloads embedded in source string literals."""
    forbidden = (
        "LOCKDOWN_OBSERVATION", "boundary_observations",
        "go-package-lockdown-runtime-observation",
    )
    index = 0
    while index < len(text):
        if text.startswith("//", index):
            newline = text.find("\n", index + 2)
            index = len(text) if newline < 0 else newline + 1
            continue
        if text.startswith("/*", index):
            end = text.find("*/", index + 2)
            if end < 0:
                raise LockdownError("unterminated block comment in test anchor")
            index = end + 2
            continue
        raw = re.match(r"(?:br|rb|r)?(?P<hashes>#{0,255})\"", text[index:])
        if raw is not None:
            delimiter = '"' + raw.group("hashes")
            end = text.find(delimiter, index + raw.end())
            if end < 0:
                raise LockdownError("unterminated raw string in test anchor")
            literal = text[index:end + len(delimiter)]
            if any(value in literal for value in forbidden):
                return True
            index = end + len(delimiter)
            continue
        prefix = 2 if text.startswith('b"', index) else 1 if text[index:index + 1] in {'"', '`'} else 0
        if prefix:
            quote = text[index + prefix - 1]
            cursor = index + prefix
            escaped = False
            while cursor < len(text):
                char = text[cursor]
                cursor += 1
                if quote == '`':
                    if char == '`':
                        break
                elif escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == quote:
                    break
            else:
                raise LockdownError("unterminated string in test anchor")
            literal = text[index:cursor]
            if any(value in literal for value in forbidden):
                return True
            index = cursor
            continue
        index += 1
    return False


def tsv_text(schema: str, header: list[str], rows: Iterable[Iterable[str]]) -> str:
    lines = [f"# {schema}", "\t".join(header)]
    for row in rows:
        fields = list(row)
        if len(fields) != len(header):
            raise LockdownError(f"invalid {schema} row width: {fields}")
        for field in fields:
            validate_tsv_field(field, schema)
        lines.append("\t".join(fields))
    return "\n".join(lines) + "\n"


def read_tsv(path: Path, header: list[str]) -> list[dict[str, str]]:
    if not path.is_file():
        raise LockdownError(f"missing evidence file: {path}")
    lines = [
        line for line in path.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]
    if not lines:
        raise LockdownError(f"missing TSV header: {path}")
    reader = csv.DictReader(lines, delimiter="\t")
    if reader.fieldnames != header:
        raise LockdownError(f"invalid TSV header in {path}: {reader.fieldnames}, expected {header}")
    rows = list(reader)
    if any(None in row or None in row.values() for row in rows):
        raise LockdownError(f"invalid TSV row width in {path}")
    return rows


class PackageLockdown:
    def __init__(self, root: Path, spec_path: Path, accepted_source_commit: str):
        self.root = root.resolve()
        if not self.root.is_dir():
            raise LockdownError(f"repository root is not a directory: {self.root}")
        self.spec_relative = repository_path(spec_path.as_posix(), "--spec")
        self.spec_path = self.root / self.spec_relative
        if not self.spec_path.is_file():
            raise LockdownError(f"package spec does not exist: {self.spec_relative}")
        try:
            self.spec = tomllib.loads(self.spec_path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise LockdownError(f"cannot read {self.spec_relative}: {error}") from error
        if not re.fullmatch(r"[0-9a-f]{40}", accepted_source_commit):
            raise LockdownError(
                "--accepted-source-commit must be a full lowercase 40-character Git SHA"
            )
        self.accepted_source_commit = accepted_source_commit
        self._load_spec()

    def _required_string(self, name: str) -> str:
        value = self.spec.get(name)
        if not isinstance(value, str) or not value:
            raise LockdownError(f"package.toml field {name!r} must be a non-empty string")
        return value

    def _string_list(self, name: str) -> list[str]:
        value = self.spec.get(name)
        if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
            raise LockdownError(f"package.toml field {name!r} must be a list of strings")
        if len(value) != len(set(value)):
            raise LockdownError(f"package.toml field {name!r} contains duplicates")
        return value

    def _load_spec(self) -> None:
        unknown_fields = set(self.spec) - SPEC_FIELDS
        if unknown_fields:
            raise LockdownError(f"package.toml contains unknown fields: {sorted(unknown_fields)}")
        if self.spec.get("schema") != SPEC_SCHEMA:
            raise LockdownError(f"package.toml schema must be {SPEC_SCHEMA!r}")
        if self.spec.get("claim") != "whole-go-package":
            raise LockdownError("package.toml claim must be 'whole-go-package'")
        self.go_package = repository_path(self._required_string("go_package"), "go_package")
        self.source_commit = self._required_string("source_commit")
        if not re.fullmatch(r"[0-9a-f]{40}", self.source_commit):
            raise LockdownError("source_commit must be a full lowercase 40-character Git SHA")
        if self.source_commit != self.accepted_source_commit:
            raise LockdownError(
                "package.toml source_commit differs from coordinator-supplied "
                "--accepted-source-commit"
            )
        try:
            run(self.root, [
                "git", "cat-file", "-e", f"{self.accepted_source_commit}^{{commit}}",
            ])
        except LockdownError as error:
            raise LockdownError(
                f"accepted source commit is not available: {self.accepted_source_commit}"
            ) from error
        try:
            run(self.root, [
                "git", "merge-base", "--is-ancestor", self.accepted_source_commit, "HEAD",
            ])
        except LockdownError as error:
            raise LockdownError(
                "accepted source commit must be an ancestor of the current checkout"
            ) from error
        self.git_object_format = run(
            self.root, ["git", "rev-parse", "--show-object-format"]
        ).strip()
        if self.git_object_format not in hashlib.algorithms_available:
            raise LockdownError(f"unsupported Git object format: {self.git_object_format}")
        self.source_tree = run(
            self.root, ["git", "rev-parse", f"{self.accepted_source_commit}^{{tree}}"]
        ).strip()
        self._go_test_uses_failpoint_wrapper_cache: bool | None = None

        self.primary_rust_crate = self._required_string("primary_rust_crate")
        self.mapped_rust_crates = self._string_list("mapped_rust_crates")
        if not all(re.fullmatch(r"[A-Za-z0-9_-]+", crate) for crate in self.mapped_rust_crates):
            raise LockdownError("mapped_rust_crates contains an invalid crate name")
        if self.primary_rust_crate not in self.mapped_rust_crates:
            raise LockdownError("primary_rust_crate must appear in mapped_rust_crates")

        self.extra_artifacts = [
            repository_path(value, "extra_artifacts") for value in self._string_list("extra_artifacts")
        ]
        self.owned_rust_files = [
            repository_path(value, "owned_rust_files") for value in self._string_list("owned_rust_files")
        ]
        for path in self.owned_rust_files:
            if not any(path.is_relative_to(Path("rust/crates") / crate) for crate in self.mapped_rust_crates):
                raise LockdownError(f"owned Rust file is outside mapped crates: {path}")
        raw_fixture_evidence = self.spec.get("unresolved_fixture_evidence", {})
        if not isinstance(raw_fixture_evidence, dict) or not all(
            isinstance(key, str) and key and isinstance(value, str) and value
            for key, value in raw_fixture_evidence.items()
        ):
            raise LockdownError("unresolved_fixture_evidence must be a string-to-string TOML table")
        self.unresolved_fixture_evidence: dict[str, str] = raw_fixture_evidence

        raw_roles = self.spec.get("artifact_roles", {})
        if not isinstance(raw_roles, dict):
            raise LockdownError("artifact_roles must be a TOML table")
        self.artifact_roles: dict[Path, str] = {}
        for raw_path, role in raw_roles.items():
            path = repository_path(raw_path, "artifact_roles path")
            if not isinstance(role, str) or role not in ALLOWED_ARTIFACT_ROLES:
                raise LockdownError(f"invalid artifact role for {path}: {role!r}")
            self.artifact_roles[path] = role

        raw_exclusions = self.spec.get("excluded_subpackages")
        if not isinstance(raw_exclusions, list):
            raise LockdownError("excluded_subpackages must be a list of {path, proof} tables")
        self.excluded_subpackages: dict[Path, str] = {}
        for item in raw_exclusions:
            if not isinstance(item, dict) or set(item) != {"path", "proof"}:
                raise LockdownError("each excluded_subpackages entry must contain only path and proof")
            path = repository_path(item["path"], "excluded_subpackages path")
            proof = item["proof"]
            if not isinstance(proof, str) or not proof.strip():
                raise LockdownError(f"excluded subpackage {path} has no structural proof")
            if path.parent == path or not path.is_relative_to(self.go_package) or path == self.go_package:
                raise LockdownError(f"excluded subpackage is not below {self.go_package}: {path}")
            if path in self.excluded_subpackages:
                raise LockdownError(f"duplicate excluded subpackage: {path}")
            self.excluded_subpackages[path] = proof

        exclusions = sorted(self.excluded_subpackages)
        for index, path in enumerate(exclusions):
            if any(path.is_relative_to(parent) for parent in exclusions[:index]):
                raise LockdownError(f"overlapping excluded subpackages are not allowed: {path}")

        self.receipt_dir = self.spec_path.parent
        self.artifacts_path = self.receipt_dir / "artifacts.tsv"
        self.ledgers_dir = self.receipt_dir / "ledgers"
        self.symbols_path = self.receipt_dir / "symbols.tsv"
        self.rules_path = self.receipt_dir / "rules.tsv"
        self.mutation_plan_path = self.receipt_dir / "mutation-plan.tsv"
        self.mutation_results_path = self.receipt_dir / "mutation-results.tsv"
        self.probe_results_path = self.receipt_dir / "probe-results.tsv"
        self.helper_calls_path = self.receipt_dir / "helper-calls.tsv"
        self.helper_contracts_path = self.receipt_dir / "helper-contracts.tsv"
        self.receipt_path = self.receipt_dir / "receipt.json"
        self.symbol_support_paths: set[Path] = set()

    def _mutation_history_genesis(self) -> str:
        return hashlib.sha256(canonical_json_bytes({
            "schema": MUTATION_HISTORY_SCHEMA,
            "go_package": self.go_package.as_posix(),
        })).hexdigest()

    def _mutation_history_hash(self, row: dict[str, str]) -> str:
        payload = {
            "schema": MUTATION_HISTORY_SCHEMA,
            **{
                column: row[column]
                for column in MUTATION_RESULT_HEADER
                if column != "history_sha256"
            },
        }
        return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()

    def _accepted_blob(self, path: Path) -> bytes | None:
        relative = path.relative_to(self.root).as_posix()
        completed = subprocess.run(
            ["git", "show", f"{self.accepted_source_commit}:{relative}"],
            cwd=self.root, check=False,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        return completed.stdout if completed.returncode == 0 else None

    def _history_chain_only(
        self, results: list[dict[str, str]], *, allow_unverified_tail: bool = False
    ) -> str:
        prior = self._mutation_history_genesis()
        for index, row in enumerate(results, start=1):
            if row["sequence"] != str(index):
                raise LockdownError("mutation history sequence is not contiguous and append-only")
            if row["prior_history_sha256"] != prior:
                raise LockdownError(f"mutation history predecessor drifted at sequence {index}")
            if row["prior_checkpoint_sha256"] != "-" and not re.fullmatch(
                r"[0-9a-f]{64}", row["prior_checkpoint_sha256"]
            ):
                raise LockdownError(f"mutation history checkpoint binding is invalid at sequence {index}")
            if row["history_sha256"] == "-" and allow_unverified_tail and index == len(results):
                if row["verification_artifact_path"] != "-" or row[
                    "verification_artifact_sha256"
                ] != "-":
                    raise LockdownError("unverified mutation history tail claims verification")
                break
            expected = self._mutation_history_hash(row)
            if row["history_sha256"] != expected:
                raise LockdownError(f"mutation history hash drifted at sequence {index}")
            prior = expected
        return prior

    def _accepted_history_checkpoint(self) -> tuple[str, list[dict[str, str]]] | None:
        receipt_blob = self._accepted_blob(self.receipt_path)
        receipt_rows: list[dict[str, str]] | None = None
        if receipt_blob is not None:
            try:
                raw_receipt = receipt_blob.decode("utf-8")
                payload = strict_json_loads(raw_receipt, "accepted package receipt")
            except UnicodeDecodeError as error:
                raise LockdownError(f"committed package receipt is unreadable: {error}") from error
            if raw_receipt != canonical_json_text(payload):
                raise LockdownError("accepted package receipt is not exact canonical JSON")
            if not isinstance(payload, dict) or payload.get("schema") != RECEIPT_SCHEMA:
                raise LockdownError("committed package receipt has an incompatible schema")
            previous = payload.get("mutation_history")
            if not isinstance(previous, list) or not all(isinstance(row, dict) for row in previous):
                raise LockdownError("committed package receipt lacks mutation history")
            receipt_rows = [
                {column: str(row.get(column, "")) for column in MUTATION_RESULT_HEADER}
                for row in previous
            ]
            head = self._history_chain_only(receipt_rows)
            if payload.get("mutation_history_head_sha256") != head:
                raise LockdownError("committed package receipt mutation-history head drifted")

        results_blob = self._accepted_blob(self.mutation_results_path)
        if results_blob is None:
            if receipt_blob is None or receipt_rows is None:
                return None
            return hashlib.sha256(receipt_blob).hexdigest(), receipt_rows
        try:
            lines = [
                line for line in results_blob.decode("utf-8").splitlines()
                if line and not line.startswith("#")
            ]
        except UnicodeDecodeError as error:
            raise LockdownError(f"committed mutation history is unreadable: {error}") from error
        reader = csv.DictReader(lines, delimiter="\t")
        if reader.fieldnames != MUTATION_RESULT_HEADER:
            raise LockdownError("committed mutation history has an incompatible header")
        rows = list(reader)
        if any(None in row or None in row.values() for row in rows):
            raise LockdownError("committed mutation history has an invalid row")
        self._history_chain_only(rows)
        if receipt_rows is not None and (
            len(receipt_rows) > len(rows) or rows[:len(receipt_rows)] != receipt_rows
        ):
            raise LockdownError("accepted receipt and mutation history disagree")
        return hashlib.sha256(results_blob).hexdigest(), rows

    def _validate_mutation_history(
        self, results: list[dict[str, str]], *, allow_unverified_tail: bool = False
    ) -> tuple[str, list[dict[str, str]]]:
        prior = self._history_chain_only(results, allow_unverified_tail=allow_unverified_tail)
        accepted = self._accepted_history_checkpoint()
        if accepted is None:
            if any(row["prior_checkpoint_sha256"] != "-" for row in results):
                raise LockdownError("initial mutation history claims a nonexistent prior receipt")
            return prior, []

        accepted_digest, previous_rows = accepted
        if len(previous_rows) > len(results) or results[:len(previous_rows)] != previous_rows:
            raise LockdownError("mutation history deleted, reordered, or rewrote a committed attempt")
        for row in results[len(previous_rows):]:
            if row["prior_checkpoint_sha256"] != accepted_digest:
                raise LockdownError("new mutation attempt is not bound to the accepted history checkpoint")
        return prior, previous_rows

    def _is_excluded(self, path: Path) -> bool:
        return any(path == excluded or path.is_relative_to(excluded) for excluded in self.excluded_subpackages)

    def _go_embed_inventory(
        self, source_entries: dict[Path, str]
    ) -> tuple[list[dict[str, str]], set[Path]]:
        package_dirs = [self.go_package, *sorted(self.excluded_subpackages)]
        command = ["go", "run", f"./{GO_EMBED_TOOL}", "-root", "."]
        for package_dir in package_dirs:
            command.extend(["-package", package_dir.as_posix()])
        output = run(self.root, command)
        lines = [line for line in output.splitlines() if line and not line.startswith("#")]
        if not lines or lines[0].split("\t") != EMBED_HEADER:
            raise LockdownError("Go embed inventory returned no valid header")
        rows: list[dict[str, str]] = []
        dependencies: set[Path] = set()
        unsupported: list[str] = []
        allowed_source_dirs = set(package_dirs)
        for fields in csv.DictReader(lines, delimiter="\t"):
            row = {key: str(value) for key, value in fields.items()}
            if set(row) != set(EMBED_HEADER):
                raise LockdownError(f"invalid Go embed inventory row: {row}")
            source = repository_path(row["source_path"], "go:embed source_path")
            if (
                not row["source_line"].isdigit()
                or not row["source_column"].isdigit()
                or int(row["source_line"]) < 1
                or int(row["source_column"]) < 1
                or not row["patterns"]
                or source.parent not in allowed_source_dirs
                or source.suffix != ".go"
                or source not in source_entries
            ):
                raise LockdownError(f"Go embed inventory escaped its pinned package: {row}")
            rows.append(row)
            patterns = row["patterns"].split("\x1f")
            for pattern in patterns:
                # This deliberately supports only the cmd/go subset whose
                # resolution is unambiguous without duplicating its glob and
                # directory-walk implementation: one safe basename in the
                # claimed package, resolving to an already pinned regular
                # artifact. Every broader form fails closed below.
                if (
                    source.parent != self.go_package
                    or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", pattern)
                    or pattern.startswith((".", "_"))
                ):
                    unsupported.append(
                        f"{row['source_path']}:{row['source_line']}:{pattern}"
                    )
                    continue
                resolved = source.parent / pattern
                if resolved not in source_entries:
                    unsupported.append(
                        f"{row['source_path']}:{row['source_line']}:{pattern}"
                    )
                    continue
                dependencies.add(resolved)
        if unsupported:
            raise LockdownError(
                "//go:embed pattern is outside schema v2's exact direct-file subset and "
                "fails closed; cmd/go-compatible glob/directory resolution is required: "
                + ", ".join(unsupported)
            )
        return rows, dependencies

    def _git_paths(self, arguments: list[str]) -> list[Path]:
        output = run(self.root, ["git", *arguments])
        return [repository_path(value, "Git path") for value in output.split("\0") if value]

    def _source_tree_entries(self, path: Path | None = None) -> dict[Path, str]:
        command = ["git", "ls-tree", "-r", "-z", self.accepted_source_commit]
        if path is not None:
            command.extend(["--", path.as_posix()])
        output = run_bytes(
            self.root,
            command,
        )
        entries: dict[Path, str] = {}
        for raw_entry in output.split(b"\0"):
            if not raw_entry:
                continue
            try:
                metadata, raw_path = raw_entry.split(b"\t", 1)
                _mode, kind, object_id = metadata.decode("ascii").split(" ")
                decoded_path = raw_path.decode("utf-8")
            except (UnicodeDecodeError, ValueError) as error:
                raise LockdownError("cannot decode source_commit tree entry") from error
            if kind != "blob":
                raise LockdownError(f"non-blob package artifact at source_commit: {decoded_path}")
            entries[repository_path(decoded_path, "source_commit tree path")] = object_id
        return entries

    def _source_blob(self, path: Path) -> bytes:
        return run_bytes(
            self.root, [
                "git", "cat-file", "blob",
                f"{self.accepted_source_commit}:{path.as_posix()}",
            ]
        )

    def _git_blob_oid(self, data: bytes) -> str:
        digest = hashlib.new(self.git_object_format)
        digest.update(f"blob {len(data)}\0".encode("ascii"))
        digest.update(data)
        return digest.hexdigest()

    def _validate_excluded_subpackages(
        self, current_tracked: set[Path], source_entries: dict[Path, str]
    ) -> None:
        direct_parent_go = sorted(
            path for path in source_entries
            if path.parent == self.go_package and path.suffix == ".go" and not path.name.endswith("_test.go")
        )
        if not direct_parent_go:
            raise LockdownError(f"source_commit has no production Go file in {self.go_package}")
        parent_names = {go_package_name(self._source_blob(path), path) for path in direct_parent_go}
        if len(parent_names) != 1:
            raise LockdownError(f"source_commit has inconsistent parent package names: {parent_names}")

        for excluded, proof in self.excluded_subpackages.items():
            if "testdata" in excluded.parts:
                raise LockdownError(f"fixture/testdata subtree cannot be excluded as a Go package: {excluded}")
            pinned_subtree = {
                path: object_id for path, object_id in source_entries.items()
                if path == excluded or path.is_relative_to(excluded)
            }
            current_subtree = {
                path for path in current_tracked if path == excluded or path.is_relative_to(excluded)
            }
            if not pinned_subtree or current_subtree != set(pinned_subtree):
                raise LockdownError(f"excluded subpackage tree drifted or is empty: {excluded}")
            direct_go = sorted(
                path for path in pinned_subtree if path.parent == excluded and path.suffix == ".go"
            )
            production = [path for path in direct_go if not path.name.endswith("_test.go")]
            if not production:
                raise LockdownError(f"excluded subtree is not a distinct Go package: {excluded}")
            production_names = {go_package_name(self._source_blob(path), path) for path in production}
            if len(production_names) != 1:
                raise LockdownError(f"excluded Go package has inconsistent names: {excluded}")
            package_name = next(iter(production_names))
            test_names = {go_package_name(self._source_blob(path), path) for path in direct_go}
            if test_names - {package_name, package_name + "_test"}:
                raise LockdownError(f"excluded Go package has inconsistent test package names: {excluded}")
            expected_proof = f"go-package-dir:{excluded.as_posix()}#package:{package_name}"
            if proof != expected_proof:
                raise LockdownError(
                    f"excluded subtree {excluded} is not proved distinct by directory; "
                    f"expected proof '{expected_proof}'"
                )
            for path, object_id in pinned_subtree.items():
                current = self.root / path
                if not current.is_file() or current.is_symlink():
                    raise LockdownError(f"excluded package artifact disappeared or became a symlink: {path}")
                if self._git_blob_oid(current.read_bytes()) != object_id:
                    raise LockdownError(f"excluded package artifact drifted from source_commit: {path}")

    def _tracked_artifact_paths(self) -> list[Path]:
        tracked = self._git_paths(["ls-files", "-z", "--", self.go_package.as_posix()])
        source_entries = self._source_tree_entries(self.go_package)
        self._validate_excluded_subpackages(set(tracked), source_entries)
        self.go_embed_rows, self.embed_dependency_paths = self._go_embed_inventory(source_entries)
        package_paths = [path for path in tracked if not self._is_excluded(path)]
        paths = set(package_paths) | self.embed_dependency_paths
        for extra in self.extra_artifacts:
            if extra in paths:
                raise LockdownError(f"extra_artifact is already package-owned: {extra}")
            if extra not in self._git_paths(["ls-files", "-z", "--", extra.as_posix()]):
                raise LockdownError(f"extra_artifact is not tracked: {extra}")
            paths.add(extra)
            source_entries.update(self._source_tree_entries(extra))
        pinned_paths = {
            path for path in source_entries
            if (not self._is_excluded(path) or path in self.embed_dependency_paths) and (
                path.is_relative_to(self.go_package) or path in self.extra_artifacts
            )
        }
        if paths != pinned_paths:
            raise LockdownError(
                "claimed artifact set differs from source_commit: "
                f"current={sorted(path.as_posix() for path in paths)}, "
                f"pinned={sorted(path.as_posix() for path in pinned_paths)}"
            )
        self.source_entry_oids = {path: source_entries[path] for path in pinned_paths}
        return sorted(paths, key=lambda path: path.as_posix())

    def _untracked_artifact_paths(self) -> list[Path]:
        untracked = self._git_paths(
            ["ls-files", "-z", "--others", "--exclude-standard", "--", self.go_package.as_posix()]
        )
        found = {path for path in untracked if not self._is_excluded(path)}
        for extra in self.extra_artifacts:
            found.update(self._git_paths(
                ["ls-files", "-z", "--others", "--exclude-standard", "--", extra.as_posix()]
            ))
        return sorted(found, key=lambda path: path.as_posix())

    def _validate_mapped_rust_change_census(self) -> None:
        crate_roots = [f"rust/crates/{crate}" for crate in self.mapped_rust_crates]
        changed = set(self._git_paths([
            "diff", "--name-only", "-z", self.accepted_source_commit, "--", *crate_roots,
        ]))
        for crate_root in crate_roots:
            changed.update(self._git_paths([
                "ls-files", "-z", "--others", "--exclude-standard", "--", crate_root,
            ]))
        proof_outputs = getattr(self, "receipt_owned_paths", set()) | {
            self.receipt_path.relative_to(self.root)
        }
        unowned = changed - set(self.owned_rust_files) - proof_outputs
        if unowned:
            raise LockdownError(
                "mapped Rust change census is not exact: "
                f"unowned={sorted(path.as_posix() for path in unowned)}"
            )

    def _initialize_receipt_artifact_census(self) -> None:
        """Resolve only concrete proof paths declared by the receipt graph."""
        receipt_relative = self.receipt_dir.relative_to(self.root)
        actual = set(self._git_paths([
            "ls-files", "-z", "--", receipt_relative.as_posix(),
        ]))
        actual.update(self._git_paths([
            "ls-files", "-z", "--others", "--exclude-standard", "--",
            receipt_relative.as_posix(),
        ]))
        core = {
            self.spec_relative,
            self.artifacts_path.relative_to(self.root),
            self.symbols_path.relative_to(self.root),
            self.rules_path.relative_to(self.root),
            self.mutation_plan_path.relative_to(self.root),
            self.mutation_results_path.relative_to(self.root),
            self.probe_results_path.relative_to(self.root),
            self.helper_calls_path.relative_to(self.root),
            self.helper_contracts_path.relative_to(self.root),
            self.receipt_path.relative_to(self.root),
        }
        declared = {path for path in core if path in actual}
        explicit_receipt_artifacts = {
            path for path in self.owned_rust_files if path.is_relative_to(receipt_relative)
        }
        missing_explicit = explicit_receipt_artifacts - actual
        if missing_explicit:
            raise LockdownError(
                "explicit receipt proof artifact disappeared: "
                + ", ".join(path.as_posix() for path in sorted(
                    missing_explicit, key=lambda item: item.as_posix()
                ))
            )
        declared.update(explicit_receipt_artifacts)
        declared.update(
            path.relative_to(self.root) for path in self.ledgers_dir.glob("*.tsv")
            if path.is_file()
        )

        artifact_reference = re.compile(
            r"(?:evidence-artifact:)?([^;@]+)@sha256:[0-9a-f]{64}"
        )

        def add_reference(raw: object) -> None:
            if not isinstance(raw, str):
                return
            values = [match.group(1) for match in artifact_reference.finditer(raw)]
            if not values and raw and "\n" not in raw and "\t" not in raw:
                values = [raw]
            for value in values:
                try:
                    path = repository_path(value, "receipt-declared artifact path")
                except LockdownError:
                    continue
                if path.is_relative_to(receipt_relative) and path in actual:
                    declared.add(path)

        for value in self.unresolved_fixture_evidence.values():
            add_reference(value)
        scanned: set[Path] = set()
        while True:
            pending = sorted(declared - scanned, key=lambda item: item.as_posix())
            if not pending:
                break
            for path in pending:
                scanned.add(path)
                full_path = self.root / path
                if path == self.receipt_path.relative_to(self.root) or not full_path.is_file():
                    continue
                if path.suffix == ".tsv":
                    lines = [
                        line for line in full_path.read_text(encoding="utf-8").splitlines()
                        if line and not line.startswith("#")
                    ]
                    if not lines:
                        continue
                    for row in csv.DictReader(lines, delimiter="\t"):
                        for key, value in row.items():
                            if key.endswith("_path") or key == "evidence":
                                add_reference(value)
                elif path.suffix == ".json":
                    payload = read_strict_json(full_path, "receipt-declared proof artifact")

                    def walk(value: object, key: str = "") -> None:
                        if isinstance(value, dict):
                            for child_key, child in value.items():
                                walk(child, str(child_key))
                        elif isinstance(value, list):
                            for child in value:
                                walk(child, key)
                        elif key.endswith("_path") or key == "evidence":
                            add_reference(value)

                    walk(payload)
        extra = actual - declared
        if extra:
            raise LockdownError(
                "receipt proof directory contains an undeclared artifact: "
                + ", ".join(path.as_posix() for path in sorted(
                    extra, key=lambda item: item.as_posix()
                ))
            )
        for path in declared:
            if path.suffix in {".rs", ".go"}:
                raise LockdownError(
                    f"receipt proof directory cannot hide production/test source: {path}"
                )
        self.receipt_owned_paths = declared

    def _automatic_role(self, path: Path, data: bytes) -> str | None:
        if path in self.artifact_roles:
            return self.artifact_roles[path]
        if path in self.embed_dependency_paths and self._is_excluded(path):
            return "generated-input"
        if path.name in {"BUILD", "BUILD.bazel"} or path.suffix == ".bzl":
            return "build"
        if "testdata" in path.parts:
            return "fixture"
        if path.suffix == ".go":
            generated = b"// Code generated" in data
            test = path.name.endswith("_test.go")
            if generated and test:
                return "generated-test-go"
            if generated:
                return "generated-production-go"
            return "test-go" if test else "production-go"
        return None

    def _artifact_traits(self, path: Path, data: bytes) -> str:
        traits: list[str] = []
        if b"//go:build" in data or b"// +build" in data:
            traits.append("build-tag")
        if any(part in PLATFORM_PARTS for part in path.stem.split("_")[1:]):
            traits.append("platform-variant")
        if b"// Code generated" in data:
            traits.append("generated")
        if b"//go:generate" in data:
            traits.append("go-generate")
        if b"//go:embed" in data:
            traits.append("go-embed")
        if path in self.embed_dependency_paths:
            traits.append("go-embed-input")
        if "testdata" in path.parts:
            traits.append("testdata")
        return ",".join(traits) if traits else "-"

    def artifact_rows(self) -> list[list[str]]:
        untracked = self._untracked_artifact_paths()
        if untracked:
            raise LockdownError(
                "untracked package artifacts must be staged or removed before generation: "
                + ", ".join(path.as_posix() for path in untracked)
            )
        paths = self._tracked_artifact_paths()
        if not paths:
            raise LockdownError(f"Go package has no tracked artifacts: {self.go_package}")
        rows: list[list[str]] = []
        go_paths: set[Path] = set()
        for path in paths:
            full_path = self.root / path
            if not full_path.is_file() or full_path.is_symlink():
                raise LockdownError(f"package artifact is not a regular file: {path}")
            data = full_path.read_bytes()
            source_oid = self.source_entry_oids[path]
            if self._git_blob_oid(data) != source_oid:
                raise LockdownError(f"package artifact differs from source_commit: {path}")
            role = self._automatic_role(path, data)
            if role is None:
                raise LockdownError(
                    f"unclassified package artifact {path}; add an explicit artifact_roles entry"
                )
            if role not in ALLOWED_ARTIFACT_ROLES:
                raise LockdownError(f"invalid artifact role {role!r} for {path}")
            go_role = role in {
                "production-go", "test-go", "generated-production-go", "generated-test-go",
            }
            if go_role and path.suffix != ".go":
                raise LockdownError(f"Go suffix and artifact role disagree for {path}: {role}")
            if (
                path.suffix == ".go"
                and not go_role
                and "testdata" not in path.parts
                and path not in self.embed_dependency_paths
                and not (path in self.extra_artifacts and role in {"generated-input", "generated-output"})
            ):
                raise LockdownError(f"Go suffix and artifact role disagree for {path}: {role}")
            if role.endswith("-go"):
                if path.parent != self.go_package:
                    raise LockdownError(
                        f"Go artifact {path} is outside the non-recursive package directory; "
                        "declare a nested Go package exclusion"
                    )
                go_paths.add(path)
            rows.append(
                [
                    path.as_posix(), role, self._artifact_traits(path, data),
                    hashlib.sha256(data).hexdigest(), str(len(data)), str(source_lines(full_path)),
                    source_oid,
                ]
            )
        unused_overrides = set(self.artifact_roles) - set(paths)
        if unused_overrides:
            raise LockdownError(
                "artifact_roles contains paths outside the census: "
                + ", ".join(path.as_posix() for path in sorted(unused_overrides))
            )
        if not go_paths:
            raise LockdownError(f"Go package has no direct Go sources: {self.go_package}")
        return rows

    def _validate_observation_test_source(
        self, plan: dict[str, object], context: str
    ) -> None:
        named_test = str(plan.get("named_test", ""))
        leaf = named_test.split("::")[-1]
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", leaf):
            raise LockdownError(f"{context} has an invalid named test")
        candidates: list[Path]
        if plan.get("runner") == "go-test":
            candidates = sorted(
                path for path in self.source_entry_oids
                if path.parent == self.go_package and path.name.endswith("_test.go")
            )
            definitions: list[tuple[Path, str]] = []
            declaration = re.compile(rf"\bfunc\s+{re.escape(named_test)}\s*\(")
            for path in candidates:
                text = (self.root / path).read_text(encoding="utf-8")
                if declaration.search(code_mask(text)):
                    definitions.append((path, text))
        else:
            modules = self._cargo_test_modules(
                str(plan.get("test_subject", "")), str(plan.get("test_target", ""))
            )
            definitions = []
            for path, prefix in modules.items():
                text = (self.root / path).read_text(encoding="utf-8")
                bodies = rust_test_function_bodies(rust_tokens(text))
                identities = {
                    "::".join((*prefix, *local.split("::"))) for local in bodies
                }
                if named_test in identities:
                    definitions.append((path, text))
        if len(definitions) != 1:
            raise LockdownError(
                f"{context} named test source is not uniquely syntax-bound: {named_test}"
            )
        path, text = definitions[0]
        if has_static_observation_literal(text):
            raise LockdownError(
                f"{context} named test hardcodes a machine observation in {path}; "
                "serialize caller-supplied observed values through a separate emitter helper"
            )

    def _load_observation_artifact(
        self, plan: dict[str, object], context: str
    ) -> tuple[Path, dict[str, object]]:
        path = repository_path(str(plan.get("observation_path", "")), f"{context} observation_path")
        if not (self.root / path).is_relative_to(self.receipt_dir):
            raise LockdownError(f"{context} observation is outside the package receipt")
        full_path = self.root / path
        if full_path.is_symlink() or not full_path.is_file() or sha256(full_path) != plan.get(
            "observation_sha256"
        ):
            raise LockdownError(f"{context} observation disappeared or drifted: {path}")
        observation = read_strict_json(full_path, f"{context} observation")
        expected_keys = {
            "schema", "probe_id", "source_commit", "conclusion", "boundary_observations",
        }
        cases = observation.get("boundary_observations") if isinstance(observation, dict) else None
        if (
            not isinstance(observation, dict)
            or set(observation) != expected_keys
            or observation.get("schema") != OBSERVATION_SCHEMA
            or observation.get("probe_id") != plan.get("probe_id")
            or observation.get("source_commit") != self.source_commit
            or observation.get("conclusion") != plan.get("conclusion")
            or not isinstance(cases, list)
            or len(cases) < 2
        ):
            raise LockdownError(f"{context} observation has the wrong schema or binding: {path}")
        names: list[str] = []
        inputs: list[str] = []
        for case in cases:
            if (
                not isinstance(case, dict)
                or set(case) != {"name", "input", "expected"}
                or not all(isinstance(case[key], str) and case[key] for key in case)
            ):
                raise LockdownError(f"{context} observation has an invalid boundary case: {path}")
            names.append(case["name"])
            inputs.append(case["input"])
        if (
            len(names) != len(set(names))
            or names != plan.get("boundary_cases")
            or len(set(inputs)) < 2
        ):
            raise LockdownError(f"{context} observation boundary set differs from its plan: {path}")
        self._validate_observation_test_source(plan, context)
        return path, observation

    def _validate_observation_artifact(
        self, plan: dict[str, object], context: str
    ) -> Path:
        return self._load_observation_artifact(plan, context)[0]

    def _validate_observation_output(
        self, plan: dict[str, object], stdout: bytes, stderr: bytes, context: str
    ) -> None:
        _path, expected_plan = self._load_observation_artifact(plan, context)
        del stderr  # an observation must be emitted by the named test on stdout
        lines = stdout.decode("utf-8", errors="replace").splitlines()
        observations = [line for line in lines if line.startswith("LOCKDOWN_OBSERVATION ")]
        if len(observations) != 1:
            raise LockdownError(f"{context} did not emit its exact machine-readable observation")
        emitted = observations[0]
        payload_text = emitted.removeprefix("LOCKDOWN_OBSERVATION ")
        payload = strict_json_loads(payload_text, f"{context} runtime observation")
        expected_runtime = {
            **{key: expected_plan[key] for key in [
                "probe_id", "source_commit", "conclusion",
            ]},
            "schema": RUNTIME_OBSERVATION_SCHEMA,
            "boundary_observations": [
                {"name": case["name"], "input": case["input"], "observed": case["expected"]}
                for case in expected_plan["boundary_observations"]
            ],
        }
        if payload_text != canonical_json_bytes(payload).decode("utf-8") or payload != expected_runtime:
            raise LockdownError(
                f"{context} emitted boundary observations or conclusion that differ from evidence"
            )
        marker_index = lines.index(emitted)
        runner = str(plan["runner"])
        named_test = str(plan["named_test"])
        if runner == "go-test":
            starts = [index for index, line in enumerate(lines) if line == f"=== RUN   {named_test}"]
            finishes = [
                index for index, line in enumerate(lines)
                if re.fullmatch(rf"--- PASS: {re.escape(named_test)}(?: \([^\n]*\))?", line)
            ]
            inside_named_test = (
                len(starts) == 1 and len(finishes) == 1
                and starts[0] < marker_index < finishes[0]
            )
        else:
            finishes = [
                index for index, line in enumerate(lines)
                if line == f"test {named_test} ... ok"
            ]
            inside_named_test = len(finishes) == 1 and marker_index < finishes[0]
        if not inside_named_test:
            raise LockdownError(
                f"{context} observation was not emitted during its exact named test"
            )

    def helper_call_rows(self) -> list[list[str]]:
        output = run(
            self.root,
            ["go", "run", f"./{GO_HELPER_CALL_TOOL}", "-root", self.go_package.as_posix()],
        )
        rows: list[list[str]] = []
        seen: set[str] = set()
        for line in output.rstrip("\n").splitlines():
            if not line or line.startswith("#"):
                continue
            row = line.split("\t")
            if len(row) != len(HELPER_CALL_HEADER):
                raise LockdownError(f"invalid Go helper-call inventory row: {row}")
            (
                call_id, raw_source, line_number, column, callee, node_hash,
                fixture_api, first_argument,
            ) = row
            source = self.go_package / repository_path(raw_source, "helper-call source_path")
            if source.parent != self.go_package or not source.name.endswith("_test.go"):
                raise LockdownError(f"helper-call inventory escaped the direct Go package: {source}")
            if (
                not re.fullmatch(r"C[0-9a-f]{64}", call_id)
                or call_id in seen
                or not line_number.isdigit()
                or not column.isdigit()
                or int(line_number) < 1
                or int(column) < 1
                or not callee
                or not re.fullmatch(r"[0-9a-f]{64}", node_hash)
                or fixture_api not in {
                    "-", "os.ReadFile", "os.Open", "os.OpenFile", "os.Stat", "os.ReadDir",
                }
                or not first_argument
            ):
                raise LockdownError(f"invalid Go helper-call inventory identity: {row}")
            seen.add(call_id)
            rows.append([
                call_id, source.as_posix(), line_number, column, callee, node_hash,
                fixture_api, first_argument,
            ])
        return rows

    def _helper_contract_raw(self, calls: list[list[str]]) -> list[list[str]]:
        grouped: dict[str, list[list[str]]] = {}
        for row in calls:
            grouped.setdefault(row[4], []).append(row)
        result: list[list[str]] = []
        for callee in sorted(grouped):
            rows = grouped[callee]
            helper_id = "H" + hashlib.sha256(
                ("go-test-helper-contract-v1\0" + callee).encode("utf-8")
            ).hexdigest()
            call_ids = ";".join(row[0] for row in rows)
            call_set_hash = hashlib.sha256(canonical_json_bytes([
                dict(zip(HELPER_CALL_HEADER, row, strict=True)) for row in rows
            ])).hexdigest()
            result.append([helper_id, callee, call_ids, call_set_hash])
        return result

    def _stored_helper_contracts(self, calls: list[list[str]]) -> list[dict[str, str]]:
        stored = read_tsv(self.helper_contracts_path, HELPER_CONTRACT_HEADER)
        expected = self._helper_contract_raw(calls)
        if len(stored) != len(expected):
            raise LockdownError("helper-call contract inventory drifted; run generate and classify it")
        for row, raw in zip(stored, expected, strict=True):
            if [row[column] for column in HELPER_CONTRACT_HEADER[:4]] != raw:
                raise LockdownError("helper-call contract AST binding drifted; run generate and classify it")
        return stored

    def _validate_helper_no_fixture(
        self, row: dict[str, str], evidence: str
    ) -> Path:
        match = re.fullmatch(r"evidence-artifact:([^@]+)@sha256:([0-9a-f]{64})", evidence)
        if match is None:
            raise LockdownError(f"helper {row['helper_id']} lacks a content-addressed proof")
        path = repository_path(match.group(1), "helper no-fixture evidence path")
        full_path = self.root / path
        if not full_path.is_relative_to(self.receipt_dir):
            raise LockdownError(f"helper proof is outside the package receipt: {path}")
        if full_path.is_symlink() or not full_path.is_file() or sha256(full_path) != match.group(2):
            raise LockdownError(f"helper proof disappeared or drifted: {path}")
        payload = read_strict_json(full_path, "helper proof")
        expected = {
            "schema", "kind", "source_commit", "helper_id", "callee", "call_ids",
            "call_set_sha256", "conclusion", "boundary_cases", "proof_steps",
        }
        boundaries = payload.get("boundary_cases") if isinstance(payload, dict) else None
        steps = payload.get("proof_steps") if isinstance(payload, dict) else None
        if (
            not isinstance(payload, dict)
            or set(payload) != expected
            or payload.get("schema") != EVIDENCE_SCHEMA
            or payload.get("kind") != "helper-no-fixture"
            or payload.get("source_commit") != self.source_commit
            or payload.get("helper_id") != row["helper_id"]
            or payload.get("callee") != row["callee"]
            or payload.get("call_ids") != split_list(row["call_ids"])
            or payload.get("call_set_sha256") != row["call_set_sha256"]
            or not isinstance(payload.get("conclusion"), str)
            or not str(payload["conclusion"]).strip()
            or not isinstance(boundaries, list)
            or len(boundaries) < 2
            or not all(isinstance(item, str) and item.strip() for item in boundaries)
            or len(boundaries) != len(set(boundaries))
            or not isinstance(steps, list)
            or len(steps) < 2
            or not all(isinstance(item, str) and item.strip() for item in steps)
        ):
            raise LockdownError(f"helper proof has the wrong schema or exact binding: {path}")
        return path

    def validate_helper_contracts(
        self,
        calls: list[list[str]],
        fixture_rows: list[list[str]],
        manifested: set[Path],
    ) -> tuple[list[dict[str, str]], set[Path]]:
        contracts = self._stored_helper_contracts(calls)
        direct: dict[tuple[str, str, str, str], set[str]] = {}
        for fixture in fixture_rows:
            direct.setdefault((fixture[0], fixture[1], fixture[2], fixture[3]), set()).add(
                hashlib.sha256(canonical_json_bytes(fixture)).hexdigest()
            )
        calls_by_id = {row[0]: row for row in calls}
        owned: set[Path] = set()
        for row in contracts:
            status = row["status"]
            if status not in ALLOWED_HELPER_STATUSES:
                raise LockdownError(
                    f"helper {row['helper_id']} has no final fixture verdict: {status!r}"
                )
            call_rows = [calls_by_id[call_id] for call_id in split_list(row["call_ids"])]
            if status == "DIRECT-FIXTURE":
                hashes: set[str] = set()
                for call in call_rows:
                    if call[6] == "-":
                        raise LockdownError(
                            f"helper {row['helper_id']} is not an imported os fixture call"
                        )
                    matches = direct.get((call[1], call[2], call[6], call[7]), set())
                    if len(matches) != 1:
                        raise LockdownError(
                            f"helper {row['helper_id']} is not exactly linked to a direct fixture access"
                        )
                    hashes.update(matches)
                expected = "direct-fixture-sha256:" + ";".join(sorted(hashes))
                if row["evidence"] != expected:
                    raise LockdownError(f"helper {row['helper_id']} direct fixture binding drifted")
            elif status == "NO-FIXTURE":
                owned.add(self._validate_helper_no_fixture(row, row["evidence"]))
            else:
                key = f"helper:{row['helper_id']}@sha256:{row['call_set_sha256']}"
                path, payload = self._validate_fixture_evidence(key, row["evidence"], manifested)
                probe_id = str(payload["probe_id"])
                if probe_id in self.fixture_probe_plans:
                    raise LockdownError(f"duplicate fixture probe_id: {probe_id}")
                self.fixture_probe_plans[probe_id] = (path, payload)
                paths = {
                    path,
                    repository_path(str(payload["observation_path"]), "helper observation_path"),
                }
                self.fixture_evidence_paths.update(paths)
                owned.update(paths)
        return contracts, owned

    def fixture_accesses(self, artifact_rows: list[list[str]]) -> list[list[str]]:
        manifested = {Path(row[0]) for row in artifact_rows}
        self.fixture_probe_plans: dict[str, tuple[Path, dict[str, object]]] = {}
        self.fixture_evidence_paths: set[Path] = set()
        output = run(
            self.root,
            ["go", "run", f"./{GO_FIXTURE_TOOL}", "--root", "."],
        )
        lines = output.rstrip("\n").splitlines()
        if not lines or not lines[0].startswith("# source_path"):
            raise LockdownError("Go fixture inventory returned no valid header")
        accesses: list[list[str]] = []
        unresolved: set[str] = set()
        for line in lines[1:]:
            row = line.split("\t")
            if len(row) != 5:
                raise LockdownError(f"invalid Go fixture inventory row: {row}")
            source = repository_path(row[0], "fixture source_path")
            if source.parent != self.go_package:
                continue
            if row[2] == "go:embed":
                # Exact production/test pattern expansion is owned by the
                # dedicated Go embed inventory, including hidden files and
                # excluded nested-package dependencies.
                continue
            accesses.append(row)
            if row[4]:
                resolved = repository_path(row[4], "resolved fixture path")
                if resolved not in manifested:
                    raise LockdownError(
                        f"resolved fixture {resolved} used by {source}:{row[1]} is not manifested; "
                        "add it to extra_artifacts when it is outside the package"
                    )
            else:
                key = ":".join(row[:4])
                unresolved.add(key)
        if unresolved != set(self.unresolved_fixture_evidence):
            raise LockdownError(
                f"unresolved fixture evidence is not exact: discovered={sorted(unresolved)}, "
                f"classified={sorted(self.unresolved_fixture_evidence)}"
            )
        for key, evidence in self.unresolved_fixture_evidence.items():
            path, payload = self._validate_fixture_evidence(key, evidence, manifested)
            probe_id = str(payload["probe_id"])
            if probe_id in self.fixture_probe_plans:
                raise LockdownError(f"duplicate fixture probe_id: {probe_id}")
            self.fixture_probe_plans[probe_id] = (path, payload)
            self.fixture_evidence_paths.update({
                path,
                repository_path(str(payload["observation_path"]), "fixture observation_path"),
            })
        return accesses

    def _validate_fixture_evidence(
        self, key: str, evidence: str, manifested: set[Path]
    ) -> tuple[Path, dict[str, object]]:
        match = re.fullmatch(r"evidence-artifact:([^@]+)@sha256:([0-9a-f]{64})", evidence)
        if match is None:
            raise LockdownError(
                f"unresolved fixture {key} lacks a content-addressed evidence artifact"
            )
        path = repository_path(match.group(1), "unresolved fixture evidence path")
        if not (self.root / path).is_relative_to(self.receipt_dir):
            raise LockdownError(f"fixture evidence must live under the package receipt: {path}")
        full_path = self.root / path
        if not full_path.is_file() or full_path.is_symlink() or sha256(full_path) != match.group(2):
            raise LockdownError(f"fixture evidence disappeared or drifted: {path}")
        payload = read_strict_json(full_path, "fixture evidence")
        expected_keys = {
            "schema", "kind", "probe_id", "source_commit", "fixture_access", "conclusion",
            "boundary_cases", "resolved_artifacts", "no_artifact_conclusion", "runner",
            "test_subject", "test_target", "named_test", "expected_exit_code",
            "observation_path", "observation_sha256",
        }
        if (
            not isinstance(payload, dict)
            or set(payload) != expected_keys
            or payload.get("schema") != EVIDENCE_SCHEMA
            or payload.get("kind") != "fixture-resolution"
            or payload.get("source_commit") != self.source_commit
            or payload.get("fixture_access") != key
            or not isinstance(payload.get("probe_id"), str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", payload["probe_id"])
            or payload.get("expected_exit_code") != 0
            or not isinstance(payload.get("conclusion"), str)
            or not payload["conclusion"].strip()
        ):
            raise LockdownError(f"fixture evidence has the wrong schema or binding: {path}")
        boundaries = payload.get("boundary_cases")
        if (
            not isinstance(boundaries, list)
            or len(boundaries) < 2
            or not all(isinstance(item, str) and item.strip() for item in boundaries)
            or len(boundaries) != len(set(boundaries))
        ):
            raise LockdownError(f"fixture evidence lacks two distinct boundary cases: {path}")
        raw_resolved = payload.get("resolved_artifacts")
        if not isinstance(raw_resolved, list) or not all(
            isinstance(item, str) and item for item in raw_resolved
        ) or len(raw_resolved) != len(set(raw_resolved)):
            raise LockdownError(f"fixture evidence has an invalid resolved artifact set: {path}")
        resolved = {repository_path(item, "fixture resolved artifact") for item in raw_resolved}
        no_artifact = payload.get("no_artifact_conclusion")
        if (
            resolved and no_artifact != "-"
        ) or (
            not resolved and (not isinstance(no_artifact, str) or not no_artifact.strip() or no_artifact == "-")
        ):
            raise LockdownError(
                f"fixture evidence must choose an exact resolved set or no-artifact conclusion: {path}"
            )
        if not resolved.issubset(manifested):
            raise LockdownError(f"fixture evidence names unmanifested resolved artifacts: {path}")
        self._runner_argv(
            str(payload["runner"]), str(payload["test_subject"]),
            str(payload["test_target"]), str(payload["named_test"]),
            f"fixture probe {payload['probe_id']}",
        )
        self._validate_observation_artifact(payload, f"fixture probe {payload['probe_id']}")
        return path, payload

    def _resolve_generate_token(self, source: Path, token: str) -> set[Path]:
        if token.startswith("@"):
            token = token[1:]
        if "=" in token and token.startswith("-"):
            token = token.split("=", 1)[1]
        elif token.startswith("-"):
            return set()
        if not token or "://" in token:
            return set()
        if any(character in token for character in "*?[]|;&<>") or "$" in token:
            raise LockdownError(
                f"go:generate token is not statically resolvable in {source}: {token!r}"
            )
        candidates: list[Path] = []
        for base in [source.parent, Path()]:
            combined = PurePosixPath(base.as_posix()) / PurePosixPath(token)
            normalized_parts: list[str] = []
            escaped = False
            for part in combined.parts:
                if part in {"", "."}:
                    continue
                if part == "..":
                    if not normalized_parts:
                        escaped = True
                        break
                    normalized_parts.pop()
                else:
                    normalized_parts.append(part)
            if not escaped and normalized_parts:
                candidates.append(Path(*normalized_parts))
        if not hasattr(self, "all_source_entry_oids"):
            self.all_source_entry_oids = self._source_tree_entries()
        for candidate in candidates:
            matching = {
                path for path in self.all_source_entry_oids
                if path == candidate or path.is_relative_to(candidate)
            }
            if matching:
                return matching
        path_like = token.startswith(".") or "/" in token or "\\" in token
        if path_like:
            raise LockdownError(
                f"go:generate repository path is not present at source_commit in {source}: {token!r}"
            )
        return set()

    def go_generate_dependencies(self, artifact_rows: list[list[str]]) -> dict[str, list[str]]:
        manifested = {Path(row[0]) for row in artifact_rows}
        dependencies: dict[str, list[str]] = {}
        shell_commands = {"sh", "bash", "zsh", "cmd", "powershell", "pwsh"}
        go_rows = [
            row for row in artifact_rows
            if row[1] in {"production-go", "test-go", "generated-production-go", "generated-test-go"}
        ]
        for row in go_rows:
            source = Path(row[0])
            text = (self.root / source).read_text(encoding="utf-8")
            package_name = go_package_name(text.encode("utf-8"), source)
            for line_number, line in enumerate(text.splitlines(), start=1):
                match = re.match(r"^[ \t]*//go:generate[ \t]+(.+?)\s*$", line)
                if match is None:
                    continue
                directive = match.group(1)
                try:
                    tokens = shlex.split(directive, posix=True)
                except ValueError as error:
                    raise LockdownError(
                        f"go:generate directive cannot be parsed at {source}:{line_number}: {error}"
                    ) from error
                if not tokens or tokens[0] == "-command" or Path(tokens[0]).name in shell_commands:
                    raise LockdownError(
                        f"go:generate directive is not statically resolvable at {source}:{line_number}"
                    )
                replacements = {
                    "$GOFILE": source.name,
                    "${GOFILE}": source.name,
                    "$GOLINE": str(line_number),
                    "${GOLINE}": str(line_number),
                    "$GOPACKAGE": package_name,
                    "${GOPACKAGE}": package_name,
                }
                expanded: list[str] = []
                for token in tokens:
                    for variable, value in replacements.items():
                        token = token.replace(variable, value)
                    if "$" in token:
                        raise LockdownError(
                            f"go:generate uses an unresolved environment variable at "
                            f"{source}:{line_number}: {token!r}"
                        )
                    expanded.append(token)
                resolved: set[Path] = {source}
                for token in expanded:
                    resolved.update(self._resolve_generate_token(source, token))
                missing = resolved - manifested
                if missing:
                    raise LockdownError(
                        f"go:generate inputs are not manifested at {source}:{line_number}: "
                        + ", ".join(path.as_posix() for path in sorted(missing))
                    )
                key = f"{source}:{line_number}:sha256:{hashlib.sha256(directive.encode()).hexdigest()}"
                dependencies[key] = sorted(path.as_posix() for path in resolved)
        return dependencies

    def raw_obligations(self, artifact_rows: list[list[str]]) -> dict[Path, list[list[str]]]:
        source_sha256 = {Path(row[0]): row[3] for row in artifact_rows}
        go_paths = {
            Path(row[0]) for row in artifact_rows
            if row[1] in {"production-go", "test-go", "generated-production-go", "generated-test-go"}
        }
        output = run(
            self.root,
            [
                "go", "run", f"./{GO_INVENTORY_TOOL}", "--root", ".",
                "--package", self.go_package.as_posix(),
            ],
        )
        lines = output.rstrip("\n").splitlines()
        if not lines or not lines[0].startswith("# obligation_id"):
            raise LockdownError("Go package inventory returned no valid header")
        grouped = {path: [] for path in go_paths}
        seen: set[str] = set()
        for line in lines[1:]:
            row = line.split("\t")
            if len(row) != 6:
                raise LockdownError(f"invalid Go inventory row: {row}")
            source = repository_path(row[2], "Go inventory source_path")
            if source not in go_paths:
                raise LockdownError(f"Go inventory emitted an unmanifested source: {source}")
            if row[0] in seen:
                raise LockdownError(f"duplicate Go obligation ID: {row[0]}")
            seen.add(row[0])
            grouped[source].append(row + [source_sha256[source]])
        for rows in grouped.values():
            rows.sort(key=lambda row: (row[3], row[1], row[0]))
        return grouped

    def _ledger_path(self, source_path: Path) -> Path:
        return self.ledgers_dir / f"{source_path.name}.tsv"

    def _existing_ledgers(self) -> tuple[dict[str, list[str]], set[Path]]:
        rows: dict[str, list[str]] = {}
        files: set[Path] = set()
        if not self.ledgers_dir.exists():
            return rows, files
        for path in sorted(self.ledgers_dir.glob("*.tsv")):
            files.add(path)
            for row in read_tsv(path, LEDGER_HEADER):
                values = [row[column] for column in LEDGER_HEADER]
                obligation_id = row["obligation_id"]
                if obligation_id in rows:
                    raise LockdownError(f"duplicate existing obligation ID: {obligation_id}")
                rows[obligation_id] = values
        return rows, files

    def generate(self) -> tuple[int, int]:
        self._initialize_receipt_artifact_census()
        self._validate_mapped_rust_change_census()
        artifact_rows = self.artifact_rows()
        self.fixture_accesses(artifact_rows)
        helper_calls = self.helper_call_rows()
        helper_raw = self._helper_contract_raw(helper_calls)
        existing_helpers = (
            read_tsv(self.helper_contracts_path, HELPER_CONTRACT_HEADER)
            if self.helper_contracts_path.is_file() else []
        )
        existing_helper_by_id = {row["helper_id"]: row for row in existing_helpers}
        if len(existing_helper_by_id) != len(existing_helpers):
            raise LockdownError("duplicate existing helper-call contract ID")
        removed_helpers = sorted(set(existing_helper_by_id) - {row[0] for row in helper_raw})
        if removed_helpers:
            raise LockdownError(
                "generation would discard helper-call contracts; review and explicitly remove "
                "retired rows first: " + ", ".join(removed_helpers)
            )
        self.go_generate_dependencies(artifact_rows)
        grouped = self.raw_obligations(artifact_rows)
        existing, existing_files = self._existing_ledgers()
        raw_by_id = {row[0]: row for rows in grouped.values() for row in rows}
        removed = sorted(set(existing) - set(raw_by_id))
        if removed:
            raise LockdownError(
                "generation would discard classified obligations; review and explicitly remove "
                "retired ledger rows first: " + ", ".join(removed)
            )
        expected_files = {self._ledger_path(source) for source in grouped}
        stale_empty = existing_files - expected_files
        if stale_empty:
            raise LockdownError(
                "generation would discard stale ledger files; remove them explicitly first: "
                + ", ".join(str(path.relative_to(self.root)) for path in sorted(stale_empty))
            )

        writes: list[tuple[Path, str]] = [
            (self.artifacts_path, tsv_text(ARTIFACT_SCHEMA, ARTIFACT_HEADER, artifact_rows)),
            (self.helper_calls_path, tsv_text("helper-calls-v1", HELPER_CALL_HEADER, helper_calls)),
        ]
        helper_contracts: list[list[str]] = []
        for raw in helper_raw:
            prior = existing_helper_by_id.get(raw[0])
            if prior is not None and [prior[column] for column in HELPER_CONTRACT_HEADER[:4]] == raw:
                helper_contracts.append(raw + [prior["status"], prior["evidence"]])
            else:
                helper_contracts.append(raw + ["UNCLASSIFIED", "-"])
        writes.append((
            self.helper_contracts_path,
            tsv_text("helper-contracts-v1", HELPER_CONTRACT_HEADER, helper_contracts),
        ))
        for source, raw_rows in sorted(grouped.items(), key=lambda item: item[0].as_posix()):
            classified: list[list[str]] = []
            for raw in raw_rows:
                prior = existing.get(raw[0])
                if prior is not None:
                    if prior[:6] != raw[:6]:
                        raise LockdownError(f"existing raw obligation fields drifted for {raw[0]}")
                    if prior[6] == raw[6]:
                        prior_verdict = prior[7:]
                        if prior_verdict[0] == "UNCLASSIFIED":
                            prior_verdict = ["UNCLASSIFIED", "-", "-", "-"]
                        classified.append(raw + prior_verdict)
                    else:
                        classified.append(raw + ["UNCLASSIFIED", "-", "-", "-"])
                else:
                    classified.append(raw + ["UNCLASSIFIED", "-", "-", "-"])
            writes.append(
                (self._ledger_path(source), tsv_text(LEDGER_SCHEMA, LEDGER_HEADER, classified))
            )
        for path, header in [
            (self.symbols_path, SYMBOL_HEADER),
            (self.rules_path, RULE_HEADER),
            (self.mutation_plan_path, MUTATION_PLAN_HEADER),
            (self.mutation_results_path, MUTATION_RESULT_HEADER),
            (self.probe_results_path, PROBE_RESULT_HEADER),
        ]:
            if not path.exists():
                writes.append((path, tsv_text(path.stem + "-v1", header, [])))
        for path, text in writes:
            atomic_write(path, text)
        self._initialize_receipt_artifact_census()
        self._validate_mapped_rust_change_census()
        return len(artifact_rows), len(raw_by_id)

    def _stored_ledger_rows(self, grouped: dict[Path, list[list[str]]]) -> list[dict[str, str]]:
        expected_files = {self._ledger_path(source) for source in grouped}
        actual_files = set(self.ledgers_dir.glob("*.tsv")) if self.ledgers_dir.is_dir() else set()
        if actual_files != expected_files:
            missing = sorted(str(path.relative_to(self.root)) for path in expected_files - actual_files)
            extra = sorted(str(path.relative_to(self.root)) for path in actual_files - expected_files)
            raise LockdownError(f"per-file ledger set drifted: missing={missing}, extra={extra}")
        stored: list[dict[str, str]] = []
        raw_by_id = {row[0]: row for rows in grouped.values() for row in rows}
        seen: set[str] = set()
        for source in sorted(grouped, key=lambda path: path.as_posix()):
            rows = read_tsv(self._ledger_path(source), LEDGER_HEADER)
            for row in rows:
                obligation_id = row["obligation_id"]
                if obligation_id in seen:
                    raise LockdownError(f"duplicate classified obligation ID: {obligation_id}")
                seen.add(obligation_id)
                raw = raw_by_id.get(obligation_id)
                if raw is None:
                    raise LockdownError(f"ledger contains an obsolete obligation: {obligation_id}")
                if [row[column] for column in LEDGER_HEADER[:7]] != raw:
                    raise LockdownError(f"raw Go obligation fields drifted: {obligation_id}")
                if Path(row["source_path"]) != source:
                    raise LockdownError(f"obligation is in the wrong per-file ledger: {obligation_id}")
                stored.append(row)
        if seen != set(raw_by_id):
            missing = sorted(set(raw_by_id) - seen)
            raise LockdownError(f"unclassified Go obligations are missing from ledgers: {missing}")
        return stored

    def _validate_evidence_artifact(
        self, identity: str, evidence: str, expected_quote: str, expected_kind: str
    ) -> tuple[Path, set[Path], set[str], dict[str, object]]:
        match = re.fullmatch(
            re.escape(expected_quote)
            + r";evidence-artifact:([^@]+)@sha256:([0-9a-f]{64})",
            evidence,
        )
        if match is None:
            raise LockdownError(
                f"{expected_kind} obligation {identity} lacks an exact content-addressed evidence artifact"
            )
        artifact_path = repository_path(match.group(1), f"{expected_kind} evidence path")
        if not (self.root / artifact_path).is_relative_to(self.receipt_dir):
            raise LockdownError(f"evidence artifact must live under the package receipt: {artifact_path}")
        full_path = self.root / artifact_path
        if full_path.is_symlink() or not full_path.is_file() or sha256(full_path) != match.group(2):
            raise LockdownError(f"evidence artifact disappeared or drifted: {artifact_path}")
        payload = read_strict_json(full_path, "verdict evidence artifact")
        common = {
            "schema", "kind", "obligation_ids", "conclusion", "source_commit",
            "boundary_cases",
        }
        if payload.get("schema") != EVIDENCE_SCHEMA or payload.get("kind") != expected_kind:
            raise LockdownError(f"evidence artifact has the wrong schema or kind: {artifact_path}")
        if payload.get("source_commit") != self.source_commit:
            raise LockdownError(f"evidence artifact binds the wrong Go source: {artifact_path}")
        obligation_ids = payload.get("obligation_ids")
        if not isinstance(obligation_ids, list) or not obligation_ids or not all(
            isinstance(item, str) and item for item in obligation_ids
        ) or len(obligation_ids) != len(set(obligation_ids)):
            raise LockdownError(f"evidence artifact has invalid obligation IDs: {artifact_path}")
        if identity not in obligation_ids:
            raise LockdownError(f"evidence artifact does not bind obligation {identity}: {artifact_path}")
        if not isinstance(payload.get("conclusion"), str) or not payload["conclusion"].strip():
            raise LockdownError(f"evidence artifact has no concrete conclusion: {artifact_path}")
        boundary_cases = payload.get("boundary_cases")
        if (
            not isinstance(boundary_cases, list)
            or len(boundary_cases) < 2
            or not all(isinstance(item, str) and item.strip() for item in boundary_cases)
            or len(boundary_cases) != len(set(boundary_cases))
        ):
            raise LockdownError(
                f"evidence artifact lacks two distinct boundary cases: {artifact_path}"
            )
        owned = {artifact_path}
        if expected_kind == "measured-probe":
            expected_keys = common | {
                "probe_id", "runner", "test_subject", "test_target", "named_test",
                "expected_exit_code", "observation_path", "observation_sha256",
            }
            if (
                set(payload) != expected_keys
                or not isinstance(payload.get("probe_id"), str)
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", payload["probe_id"])
                or payload.get("expected_exit_code") != 0
            ):
                raise LockdownError(f"measured probe artifact is incomplete: {artifact_path}")
            self._runner_argv(
                str(payload["runner"]),
                str(payload["test_subject"]),
                str(payload["test_target"]),
                str(payload["named_test"]),
                f"measured probe {payload['probe_id']}",
            )
            owned.add(self._validate_observation_artifact(
                payload, f"measured probe {payload['probe_id']}"
            ))
        else:
            expected_keys = common | {"entry_surface", "proof_steps"}
            steps = payload.get("proof_steps")
            if (
                set(payload) != expected_keys
                or not isinstance(payload.get("entry_surface"), str)
                or not payload["entry_surface"].strip()
                or not isinstance(steps, list)
                or len(steps) < 2
                or not all(isinstance(item, str) and item.strip() for item in steps)
                or len(steps) != len(set(steps))
            ):
                raise LockdownError(f"structural proof artifact is incomplete: {artifact_path}")
        return artifact_path, owned, set(obligation_ids), payload

    def _cargo_test_modules(
        self, crate: str, target: str
    ) -> dict[Path, tuple[str, ...]]:
        crate_root = Path("rust/crates") / crate
        manifest_path = crate_root / "Cargo.toml"
        full_manifest = self.root / manifest_path
        if not full_manifest.is_file() or full_manifest.is_symlink():
            raise LockdownError(f"mapped crate {crate} has no regular Cargo.toml")
        try:
            manifest = tomllib.loads(full_manifest.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise LockdownError(f"cannot read {manifest_path}: {error}") from error
        explicit = [
            item for item in manifest.get("test", [])
            if isinstance(item, dict) and item.get("name") == target
        ]
        if len(explicit) > 1:
            raise LockdownError(f"Cargo test target {crate}/{target} is duplicated")
        if explicit:
            raw_path = explicit[0].get("path", f"tests/{target}.rs")
            if not isinstance(raw_path, str):
                raise LockdownError(f"Cargo test target {crate}/{target} has no exact path")
            relative_target = repository_path(raw_path, f"Cargo test target {crate}/{target} path")
            target_path = crate_root / relative_target
        else:
            if manifest.get("package", {}).get("autotests", True) is False:
                raise LockdownError(
                    f"Cargo test target {crate}/{target} is not declared with autotests=false"
                )
            candidates = [
                crate_root / "tests" / f"{target}.rs",
                crate_root / "tests" / target / "main.rs",
            ]
            existing = [path for path in candidates if (self.root / path).is_file()]
            if len(existing) != 1:
                raise LockdownError(
                    f"Cargo test target {crate}/{target} has no unique default source"
                )
            target_path = existing[0]
        if (
            not (self.root / target_path).is_file()
            or target_path not in self._git_paths(
                ["ls-files", "-z", "--", target_path.as_posix()]
            )
        ):
            raise LockdownError(f"Cargo test target source is not tracked: {target_path}")

        modules: dict[Path, tuple[str, ...]] = {}

        def visit(path_value: Path, prefix: tuple[str, ...], target_root: bool = False) -> None:
            prior = modules.get(path_value)
            if prior is not None:
                if prior != prefix:
                    raise LockdownError(
                        f"Cargo test module {path_value} has ambiguous identities"
                    )
                return
            full_path = self.root / path_value
            if not full_path.is_file() or full_path.is_symlink():
                raise LockdownError(f"Cargo test module disappeared: {path_value}")
            modules[path_value] = prefix
            text = full_path.read_text(encoding="utf-8")
            masked = code_mask(text)
            for match in re.finditer(r"\bmod\s+([A-Za-z_][A-Za-z0-9_]*)\s*;", masked):
                name = match.group(1)
                if target_root or path_value.name in {"mod.rs", "main.rs", "lib.rs"}:
                    base = path_value.parent
                else:
                    base = path_value.parent / path_value.stem
                candidates = [base / f"{name}.rs", base / name / "mod.rs"]
                existing = [candidate for candidate in candidates if (self.root / candidate).is_file()]
                if len(existing) != 1:
                    raise LockdownError(
                        f"Cargo test module {path_value} does not resolve mod {name} exactly"
                    )
                visit(existing[0], (*prefix, name))

        visit(target_path, (), True)
        self.symbol_support_paths.update({manifest_path, *modules})
        return modules

    def _validate_verdicts(
        self, ledger_rows: list[dict[str, str]], symbols: list[dict[str, str]]
    ) -> tuple[
        dict[str, dict[str, str]], set[str], set[str], set[Path],
        dict[str, tuple[Path, dict[str, object]]],
    ]:
        symbol_by_id: dict[str, dict[str, str]] = {}
        rust_symbols: set[tuple[str, str]] = set()
        anchors: set[tuple[str, str]] = set()
        for row in symbols:
            symbol_id = row["symbol_id"]
            if not symbol_id or symbol_id in symbol_by_id:
                raise LockdownError(f"blank or duplicate symbol_id: {symbol_id!r}")
            if row["rust_crate"] not in self.mapped_rust_crates:
                raise LockdownError(f"symbol {symbol_id} uses an unowned Rust crate")
            if not row["rust_symbol"] or not row["anchor_name"]:
                raise LockdownError(f"symbol {symbol_id} has an incomplete compile anchor")
            rust_identity = (row["rust_crate"], row["rust_symbol"])
            if rust_identity in rust_symbols:
                raise LockdownError(f"duplicate registered Rust symbol: {rust_identity}")
            rust_symbols.add(rust_identity)
            definition_path = repository_path(
                row["definition_path"], f"symbol {symbol_id} definition_path"
            )
            production_root = Path("rust/crates") / row["rust_crate"] / "src"
            if (
                not definition_path.is_relative_to(production_root)
                or definition_path.suffix != ".rs"
                or definition_path not in self.owned_rust_files
                or definition_path not in self._git_paths(
                    ["ls-files", "-z", "--", definition_path.as_posix()]
                )
            ):
                raise LockdownError(
                    f"symbol {symbol_id} has no tracked owned production Rust definition"
                )
            definition = self.root / definition_path
            symbol_tokens = rust_tokens(row["rust_symbol"])
            crate_identity = row["rust_crate"].replace("-", "_")
            if (
                len(symbol_tokens) < 3
                or symbol_tokens[-2] != "::"
                or symbol_tokens[0] != crate_identity
            ):
                raise LockdownError(
                    f"symbol {symbol_id} must use its qualified mapped-crate Rust identity; "
                    "test-local crate:: paths are not production anchors"
                )
            relative_definition = definition_path.relative_to(production_root)
            module_parts = list(relative_definition.parts[:-1])
            if relative_definition.name not in {"lib.rs", "main.rs", "mod.rs"}:
                module_parts.append(relative_definition.stem)
            qualified_parts = tuple(
                token for token in symbol_tokens if token != "::"
            )
            canonical_identity = ("crate",) + qualified_parts[1:]
            if (
                not definition.is_file()
                or definition.is_symlink()
                or canonical_identity not in rust_declared_identities(
                    rust_tokens(definition.read_text(encoding="utf-8")),
                    ("crate", *module_parts),
                )
            ):
                raise LockdownError(f"symbol {symbol_id} disappeared from production definition")
            anchor_path = repository_path(row["anchor_path"], f"symbol {symbol_id} anchor_path")
            expected_crate_root = Path("rust/crates") / row["rust_crate"]
            if (
                not anchor_path.is_relative_to(expected_crate_root / "tests")
                or anchor_path.suffix != ".rs"
                or anchor_path == definition_path
                or anchor_path not in self.owned_rust_files
                or anchor_path not in self._git_paths(
                    ["ls-files", "-z", "--", anchor_path.as_posix()]
                )
            ):
                raise LockdownError(
                    f"symbol {symbol_id} anchor is not a tracked owned integration test"
                )
            anchor = self.root / anchor_path
            if not anchor.is_file():
                raise LockdownError(f"symbol {symbol_id} anchor file disappeared: {anchor_path}")
            if not re.fullmatch(r"[A-Za-z0-9_-]+", row["anchor_target"]):
                raise LockdownError(f"symbol {symbol_id} has an invalid Cargo test target")
            modules = self._cargo_test_modules(row["rust_crate"], row["anchor_target"])
            if anchor_path not in modules:
                raise LockdownError(
                    f"symbol {symbol_id} anchor is not reachable from Cargo test target "
                    f"{row['anchor_target']}"
                )
            anchor_identity = (
                anchor_path.as_posix(), row["anchor_target"], row["anchor_name"]
            )
            if anchor_identity in anchors:
                raise LockdownError(f"duplicate compile anchor: {anchor_identity}")
            anchors.add(anchor_identity)
            text = anchor.read_text(encoding="utf-8")
            tokens = rust_tokens(text)
            local_test_bodies = rust_test_function_bodies(tokens)
            prefix = modules[anchor_path]
            test_bodies = {
                "::".join((*prefix, *local_name.split("::"))): body
                for local_name, body in local_test_bodies.items()
            }
            if (
                not symbol_tokens
                or row["anchor_name"] not in test_bodies
                or not rust_executable_symbol_use(
                    test_bodies[row["anchor_name"]], symbol_tokens
                )
            ):
                raise LockdownError(f"symbol {symbol_id} disappeared from compile anchor {anchor_path}")
            symbol_by_id[symbol_id] = row

        used_symbols: set[str] = set()
        ported_ids: set[str] = set()
        evidence_paths: set[Path] = set()
        evidence_references: dict[Path, set[str]] = {}
        evidence_declared: dict[Path, set[str]] = {}
        probe_plans: dict[str, tuple[Path, dict[str, object]]] = {}
        for row in ledger_rows:
            identity = row["obligation_id"]
            status = row["status"]
            evidence = row["evidence"]
            if status not in ALLOWED_VERDICTS:
                raise LockdownError(f"obligation {identity} has no final verdict: {status!r}")
            if status == "PORTED":
                symbol = symbol_by_id.get(row["symbol_id"])
                if symbol is None:
                    raise LockdownError(f"PORTED obligation {identity} has no registered Rust symbol")
                if not row["rule_id"]:
                    raise LockdownError(f"PORTED obligation {identity} has no semantic rule")
                if evidence != f"boundary-test:{symbol['anchor_name']}":
                    raise LockdownError(f"PORTED obligation {identity} lacks boundary-test evidence")
                used_symbols.add(row["symbol_id"])
                ported_ids.add(identity)
            elif status == "DECLINED":
                if row["symbol_id"] != "-" or row["rule_id"] != "-":
                    raise LockdownError(f"DECLINED obligation {identity} claims a symbol or rule")
                go_quote = (
                    f"go-quote:{row['source_path']}#{row['ast_anchor']}"
                    f"@sha256:{row['node_sha256']}"
                )
                artifact, owned, declared, payload = self._validate_evidence_artifact(
                    identity, evidence, go_quote, "measured-probe"
                )
                probe_id = str(payload["probe_id"])
                prior_probe = probe_plans.get(probe_id)
                if prior_probe is not None and prior_probe[0] != artifact:
                    raise LockdownError(f"probe_id {probe_id} names multiple evidence plans")
                probe_plans[probe_id] = (artifact, payload)
                evidence_paths.update(owned)
                evidence_references.setdefault(artifact, set()).add(identity)
                evidence_declared[artifact] = declared
            else:
                if row["symbol_id"] != "-" or row["rule_id"] != "-":
                    raise LockdownError(f"UNREACHABLE obligation {identity} claims a symbol or rule")
                go_quote = (
                    f"go-quote:{row['source_path']}#{row['ast_anchor']}"
                    f"@sha256:{row['node_sha256']}"
                )
                artifact, owned, declared, _payload = self._validate_evidence_artifact(
                    identity, evidence, go_quote, "structural-proof"
                )
                evidence_paths.update(owned)
                evidence_references.setdefault(artifact, set()).add(identity)
                evidence_declared[artifact] = declared
        for artifact, references in evidence_references.items():
            if references != evidence_declared[artifact]:
                raise LockdownError(
                    f"evidence artifact obligation set is not exact: {artifact}; "
                    f"referenced={sorted(references)}, declared={sorted(evidence_declared[artifact])}"
                )
        if used_symbols != set(symbol_by_id):
            raise LockdownError(
                f"Rust symbol registry is not exact: used={sorted(used_symbols)}, "
                f"registered={sorted(symbol_by_id)}"
            )
        return symbol_by_id, ported_ids, used_symbols, evidence_paths, probe_plans

    def _validate_rules_and_mutations(
        self,
        ledger_rows: list[dict[str, str]],
        symbol_by_id: dict[str, dict[str, str]],
        ported_ids: set[str],
        rules: list[dict[str, str]],
        plans: list[dict[str, str]],
        results: list[dict[str, str]],
    ) -> tuple[
        dict[str, dict[str, str]],
        dict[str, dict[str, object]],
        dict[str, int],
        set[Path],
    ]:
        ledger_rule = {
            row["obligation_id"]: row["rule_id"] for row in ledger_rows if row["status"] == "PORTED"
        }
        ledger_symbol = {
            row["obligation_id"]: row["symbol_id"]
            for row in ledger_rows if row["status"] == "PORTED"
        }
        rule_by_id: dict[str, dict[str, str]] = {}
        obligation_rule: dict[str, str] = {}
        rule_mutations: dict[str, set[str]] = {}
        for row in rules:
            rule_id = row["rule_id"]
            if not rule_id or rule_id in rule_by_id:
                raise LockdownError(f"blank or duplicate rule_id: {rule_id!r}")
            if not row["cluster_id"] or not row["description"]:
                raise LockdownError(f"semantic rule {rule_id} has no cluster or description")
            obligations = split_list(row["obligation_ids"])
            boundaries = split_list(row["boundary_cases"])
            mutations = split_list(row["mutation_ids"])
            if (
                not obligations
                or len(boundaries) < 2
                or len(boundaries) != len(set(boundaries))
                or not mutations
                or len(mutations) != len(set(mutations))
            ):
                raise LockdownError(f"semantic rule {rule_id} lacks obligations, boundaries, or mutations")
            for identity in obligations:
                if identity in obligation_rule:
                    raise LockdownError(f"PORTED obligation {identity} appears in multiple semantic rules")
                obligation_rule[identity] = rule_id
            rule_mutations[rule_id] = set(mutations)
            rule_by_id[rule_id] = row
        if set(obligation_rule) != ported_ids:
            raise LockdownError(
                f"semantic rule obligation coverage is not exact: rules={sorted(obligation_rule)}, "
                f"ported={sorted(ported_ids)}"
            )
        for identity, rule_id in obligation_rule.items():
            if ledger_rule[identity] != rule_id:
                raise LockdownError(f"ledger/rules semantic rule mismatch for {identity}")

        plan_by_id: dict[str, dict[str, object]] = {}
        plan_rule_mutations = {rule_id: set() for rule_id in rule_by_id}
        expected_rule_bindings: dict[str, set[tuple[str, str, str, str]]] = {}
        observed_rule_bindings: dict[str, set[tuple[str, str, str, str]]] = {
            rule_id: set() for rule_id in rule_by_id
        }
        for rule_id, rule in rule_by_id.items():
            bindings: set[tuple[str, str, str, str]] = set()
            for obligation_id in split_list(rule["obligation_ids"]):
                symbol = symbol_by_id[ledger_symbol[obligation_id]]
                anchor_path = repository_path(
                    symbol["anchor_path"], f"rule {rule_id} anchor_path"
                )
                bindings.add((
                    symbol["definition_path"],
                    symbol["rust_crate"],
                    symbol["anchor_target"],
                    symbol["anchor_name"],
                ))
            expected_rule_bindings[rule_id] = bindings
        owned_paths: set[Path] = set()
        for row in plans:
            mutation_id = row["mutation_id"]
            if not mutation_id or mutation_id in plan_by_id:
                raise LockdownError(f"blank or duplicate mutation_id: {mutation_id!r}")
            rule_ids = split_list(row["rule_ids"])
            if len(rule_ids) != 1:
                raise LockdownError(f"mutation {mutation_id} must alter exactly one semantic rule")
            for rule_id in rule_ids:
                rule = rule_by_id.get(rule_id)
                if rule is None:
                    raise LockdownError(f"mutation {mutation_id} names unknown rule {rule_id}")
                if rule["cluster_id"] != row["cluster_id"]:
                    raise LockdownError(f"mutation {mutation_id} cluster differs from rule {rule_id}")
                plan_rule_mutations[rule_id].add(mutation_id)
            if not re.fullmatch(r"[0-9a-f]{40}", row["baseline_commit"]):
                raise LockdownError(f"mutation {mutation_id} has no full baseline commit")
            try:
                run(self.root, ["git", "cat-file", "-e", f"{row['baseline_commit']}^{{commit}}"])
                run(self.root, [
                    "git", "merge-base", "--is-ancestor", row["baseline_commit"], "HEAD",
                ])
            except LockdownError as error:
                raise LockdownError(
                    f"mutation {mutation_id} baseline commit is unavailable or not an ancestor"
                ) from error
            rust_path = repository_path(row["rust_path"], f"mutation {mutation_id} rust_path")
            if rust_path not in self.owned_rust_files or not any(
                rust_path.is_relative_to(Path("rust/crates") / crate / "src")
                for crate in self.mapped_rust_crates
            ) or rust_path.suffix != ".rs":
                raise LockdownError(
                    f"mutation {mutation_id} source is not an owned production Rust file"
                )
            full_path = self.root / rust_path
            if not full_path.is_file():
                raise LockdownError(f"mutation source disappeared: {rust_path}")
            current_hash = sha256(full_path)
            if row["source_sha256"] != current_hash:
                raise LockdownError(f"mutation source hash drifted: {mutation_id}")
            baseline_source = run_bytes(
                self.root,
                ["git", "cat-file", "blob", f"{row['baseline_commit']}:{rust_path.as_posix()}"],
            )
            if hashlib.sha256(baseline_source).hexdigest() != current_hash:
                raise LockdownError(
                    f"mutation {mutation_id} baseline does not contain the claimed Rust source"
                )
            command_argv = self._runner_argv(
                row["runner"], row["test_subject"], row["test_target"], row["named_test"],
                f"mutation {mutation_id}",
            )
            binding = (
                rust_path.as_posix(), row["test_subject"], row["test_target"], row["named_test"]
            )
            if binding not in expected_rule_bindings[rule_ids[0]]:
                raise LockdownError(
                    f"mutation {mutation_id} is not bound to the semantic rule's "
                    "registered production definition and boundary test"
                )
            observed_rule_bindings[rule_ids[0]].add(binding)
            operator_path, operator, mutated_source = self._mutation_operator(row, rust_path)
            mutated_hash = hashlib.sha256(mutated_source).hexdigest()
            if operator_path in owned_paths:
                raise LockdownError(f"mutation operator is reused by multiple plans: {operator_path}")
            owned_paths.add(operator_path)
            if not row["named_test"] or row["named_test"] not in command_argv:
                raise LockdownError(f"mutation {mutation_id} lacks an exact named-test command")
            plan_by_id[mutation_id] = {
                **row,
                "command_argv": command_argv,
                "operator": operator,
                "operator_path_value": operator_path,
                "mutated_source_sha256": mutated_hash,
            }
        if set(plan_by_id) != {item for values in rule_mutations.values() for item in values}:
            raise LockdownError("mutation plan contains missing or unreferenced mutation IDs")
        for rule_id in rule_by_id:
            if plan_rule_mutations[rule_id] != rule_mutations[rule_id]:
                raise LockdownError(f"rule/mutation plan mapping drifted for {rule_id}")
            if observed_rule_bindings[rule_id] != expected_rule_bindings[rule_id]:
                raise LockdownError(
                    f"semantic rule {rule_id} does not mutation-cover every registered "
                    "production definition and boundary test"
                )

        attempts_by_mutation: dict[str, list[dict[str, str]]] = {
            mutation_id: [] for mutation_id in plan_by_id
        }
        self._validate_mutation_history(results)
        attempt_ids: set[str] = set()
        result_artifacts: set[Path] = set()
        outcome_counts: dict[str, int] = {}
        for row in results:
            attempt_id = row["attempt_id"]
            if not attempt_id or attempt_id in attempt_ids:
                raise LockdownError(f"blank or duplicate mutation attempt_id: {attempt_id!r}")
            attempt_ids.add(attempt_id)
            current_plan = plan_by_id.get(row["mutation_id"])
            if current_plan is None:
                raise LockdownError(f"mutation result names unknown mutation {row['mutation_id']}")
            attempt_plan_path, attempt_plan, attempt_owned = self._load_attempt_plan(row)
            current_binding = self._mutation_plan_binding(
                current_plan,
                Path(str(current_plan["rust_path"])),
                str(current_plan["mutated_source_sha256"]),
                list(current_plan["command_argv"]),
            )
            attempt_binding = {
                key: attempt_plan[key] for key in current_binding
            }
            is_current_plan = attempt_binding == current_binding
            run_path, artifact, run_owned = self._execution_artifact(
                row["run_artifact_path"],
                row["run_artifact_sha256"],
                MUTATION_RUN_SCHEMA,
                f"mutation {attempt_id} run",
            )
            verification_path, verification, verification_owned = self._execution_artifact(
                row["verification_artifact_path"],
                row["verification_artifact_sha256"],
                MUTATION_RUN_SCHEMA,
                f"mutation {attempt_id} verification",
            )
            if run_path in result_artifacts or verification_path in result_artifacts:
                raise LockdownError(f"mutation execution artifact is referenced twice: {attempt_id}")
            result_artifacts.update({run_path, verification_path})
            expected_keys = {
                "schema", "producer", "phase", "prior_artifact_path",
                "prior_artifact_sha256", "attempt_id", "mutation_id", "baseline_commit",
                "attempt_plan_path", "attempt_plan_sha256",
                "rust_path", "original_source_sha256", "mutated_source_sha256",
                "operator_path", "operator_sha256", "command_argv", "named_test",
                "baseline_exit_code", "baseline_output_path", "baseline_output_sha256",
                "baseline_normalized_observation_sha256",
                "exit_code", "outcome", "output_path", "output_sha256",
                "normalized_observation_sha256", "restored_source_sha256",
                "restored_exit_code", "restored_output_path", "restored_output_sha256",
                "restored_normalized_observation_sha256",
            }
            expected_values = {
                "producer": CHECKER_SCHEMA,
                "attempt_id": attempt_id,
                "mutation_id": row["mutation_id"],
                "attempt_plan_path": attempt_plan_path.as_posix(),
                "attempt_plan_sha256": row["attempt_plan_sha256"],
                "baseline_commit": attempt_plan["baseline_commit"],
                "rust_path": attempt_plan["rust_path"],
                "original_source_sha256": attempt_plan["source_sha256"],
                "mutated_source_sha256": attempt_plan["mutated_source_sha256"],
                "operator_path": attempt_plan["operator_path"],
                "operator_sha256": attempt_plan["operator_sha256"],
                "command_argv": attempt_plan["command_argv"],
                "named_test": attempt_plan["named_test"],
                "restored_source_sha256": attempt_plan["source_sha256"],
            }
            self._validate_execution_payload(
                artifact, expected_keys, expected_values, "run", "-", "-", run_path
            )
            self._validate_execution_payload(
                verification,
                expected_keys,
                expected_values,
                "verify",
                run_path.as_posix(),
                row["run_artifact_sha256"],
                verification_path,
            )
            for payload in [artifact, verification]:
                owned_paths.add(self._validate_support_observation(
                    payload, "baseline", str(attempt_plan["runner"]),
                    str(attempt_plan["named_test"]), f"mutation {attempt_id}",
                ))
                owned_paths.add(self._validate_support_observation(
                    payload, "restored", str(attempt_plan["runner"]),
                    str(attempt_plan["named_test"]), f"mutation {attempt_id}",
                ))
                execution_output = self.root / repository_path(
                    str(payload["output_path"]), "mutation execution output_path"
                )
                output_bytes = execution_output.read_bytes()
                stdout, stderr = parse_command_output(
                    output_bytes, f"mutation {attempt_id} execution"
                )
                marker_hash = normalized_test_observation(
                    str(attempt_plan["runner"]), str(attempt_plan["named_test"]),
                    int(payload["exit_code"]),
                    stdout, stderr,
                )
                if payload["normalized_observation_sha256"] != marker_hash:
                    raise LockdownError(
                        f"mutation {attempt_id} normalized observation is not derived from its log"
                    )
            outcome = str(artifact["outcome"])
            if (
                verification["exit_code"] != artifact["exit_code"]
                or verification["outcome"] != outcome
                or verification["normalized_observation_sha256"]
                != artifact["normalized_observation_sha256"]
                or verification["baseline_normalized_observation_sha256"]
                != artifact["baseline_normalized_observation_sha256"]
                or verification["restored_normalized_observation_sha256"]
                != artifact["restored_normalized_observation_sha256"]
            ):
                raise LockdownError(f"mutation {attempt_id} verification did not reproduce its run")
            output_path = repository_path(str(artifact["output_path"]), "mutation output_path")
            output_file = self.root / output_path
            if outcome == "KILLED" and str(attempt_plan["named_test"]) not in output_file.read_text(
                encoding="utf-8", errors="replace"
            ):
                raise LockdownError(f"mutation {attempt_id} output lacks its exact named failure")
            attempts_by_mutation[row["mutation_id"]].append({
                **row, "outcome": str(outcome),
                "is_current_plan": "yes" if is_current_plan else "no",
            })
            outcome_counts[str(outcome)] = outcome_counts.get(str(outcome), 0) + 1
            owned_paths.update(run_owned | verification_owned | attempt_owned)
        for mutation_id, attempts in attempts_by_mutation.items():
            current_attempts = [row for row in attempts if row["is_current_plan"] == "yes"]
            if not current_attempts:
                raise LockdownError(f"mutation {mutation_id} has no current-source recorded attempt")
            if current_attempts[-1]["outcome"] != "KILLED":
                raise LockdownError(f"mutation {mutation_id} does not end with a killed attempt")
        return rule_by_id, plan_by_id, outcome_counts, owned_paths

    def _runner_argv(
        self,
        runner: str,
        test_subject: str,
        test_target: str,
        named_test: str,
        context: str,
    ) -> list[str]:
        if runner in {"cargo-test", "cargo-test-pretty"}:
            if not re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)*", named_test
            ):
                raise LockdownError(f"{context} has an invalid exact Rust test name")
            if test_subject not in self.mapped_rust_crates:
                raise LockdownError(f"{context} uses an unmapped Rust crate")
            if not re.fullmatch(r"[A-Za-z0-9_-]+", test_target):
                raise LockdownError(f"{context} has an invalid Cargo test target")
            argv = [
                "cargo", "test", "--offline", "--locked", "-j12", "--quiet",
                "-p", test_subject, "--test", test_target, named_test, "--",
                "--exact", "--nocapture",
            ]
            if runner == "cargo-test-pretty":
                argv.append("--format=pretty")
            return argv
        if runner == "go-test":
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", named_test):
                raise LockdownError(f"{context} has an invalid exact Go test name")
            if test_subject != self.go_package.as_posix() or test_target != "-":
                raise LockdownError(f"{context} must use the exact pinned Go package")
            if self._go_test_uses_failpoint_wrapper():
                return [
                    "./tools/check/failpoint-go-test.sh", test_subject,
                    "-run", f"^{named_test}$", "-count=1", "-v",
                ]
            return [
                "go", "test", f"./{test_subject}", "-run", f"^{named_test}$", "-count=1",
                "-v",
            ]
        raise LockdownError(f"{context} has unknown runner enum {runner!r}")

    def _go_test_uses_failpoint_wrapper(self) -> bool:
        cached = self._go_test_uses_failpoint_wrapper_cache
        if cached is not None:
            return cached
        command = [
            "git", "grep", "-q",
            "-e", r"failpoint\.",
            "-e", r"testfailpoint\.",
            "-e", "@com_github_pingcap_failpoint//:failpoint",
            self.accepted_source_commit, "--", self.go_package.as_posix(),
        ]
        completed = subprocess.run(
            command,
            cwd=self.root,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if completed.returncode not in {0, 1}:
            detail = completed.stderr.decode("utf-8", errors="replace").strip()
            raise LockdownError(
                f"cannot decide failpoint lifecycle for {self.go_package}: "
                f"{detail or f'exit {completed.returncode}'}"
            )
        self._go_test_uses_failpoint_wrapper_cache = completed.returncode == 0
        return self._go_test_uses_failpoint_wrapper_cache

    def _run_fixed_test(
        self,
        runner: str,
        test_subject: str,
        test_target: str,
        named_test: str,
        context: str,
    ) -> tuple[list[str], subprocess.CompletedProcess[bytes]]:
        argv = self._runner_argv(runner, test_subject, test_target, named_test, context)
        working_directory = (
            self.root / "rust"
            if runner in {"cargo-test", "cargo-test-pretty"}
            else self.root
        )
        try:
            completed = subprocess.run(
                argv,
                cwd=working_directory,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError as error:
            raise LockdownError(f"{context} fixed test runner could not execute: {error}") from error
        return argv, completed

    def _execution_artifact(
        self, raw_path: str, digest: str, schema: str, context: str
    ) -> tuple[Path, dict[str, object], set[Path]]:
        path = repository_path(raw_path, f"{context} artifact path")
        if not (self.root / path).is_relative_to(self.receipt_dir):
            raise LockdownError(f"{context} artifact is outside the package receipt: {path}")
        full_path = self.root / path
        if not full_path.is_file() or full_path.is_symlink() or sha256(full_path) != digest:
            raise LockdownError(f"{context} artifact disappeared or drifted: {path}")
        payload = read_strict_json(full_path, f"{context} artifact")
        if not isinstance(payload, dict) or payload.get("schema") != schema:
            raise LockdownError(f"{context} artifact has the wrong schema: {path}")
        output_path = repository_path(str(payload.get("output_path", "")), f"{context} output_path")
        if not (self.root / output_path).is_relative_to(self.receipt_dir):
            raise LockdownError(f"{context} output is outside the package receipt: {output_path}")
        output = self.root / output_path
        if output.is_symlink() or not output.is_file() or sha256(output) != payload.get("output_sha256"):
            raise LockdownError(f"{context} output disappeared or drifted: {output_path}")
        return path, payload, {path, output_path}

    def _validate_support_observation(
        self,
        payload: dict[str, object],
        prefix: str,
        runner: str,
        named_test: str,
        context: str,
    ) -> Path:
        output_path = repository_path(
            str(payload.get(f"{prefix}_output_path", "")), f"{context} {prefix}_output_path"
        )
        if not (self.root / output_path).is_relative_to(self.receipt_dir):
            raise LockdownError(f"{context} {prefix} output is outside the package receipt")
        output = self.root / output_path
        if output.is_symlink() or not output.is_file() or sha256(output) != payload.get(
            f"{prefix}_output_sha256"
        ):
            raise LockdownError(f"{context} {prefix} output disappeared or drifted")
        exit_code = payload.get(f"{prefix}_exit_code")
        if exit_code != 0:
            raise LockdownError(f"{context} {prefix} named test did not pass")
        stdout, stderr = parse_command_output(output.read_bytes(), f"{context} {prefix}")
        normalized = normalized_test_observation(
            runner, named_test, int(exit_code), stdout, stderr
        )
        if payload.get(f"{prefix}_normalized_observation_sha256") != normalized:
            raise LockdownError(f"{context} {prefix} normalized observation drifted")
        return output_path

    def _validate_execution_payload(
        self,
        payload: dict[str, object],
        expected_keys: set[str],
        expected_values: dict[str, object],
        phase: str,
        prior_path: str,
        prior_sha256: str,
        path: Path,
    ) -> None:
        if set(payload) != expected_keys:
            raise LockdownError(f"execution artifact has unexpected fields: {path}")
        phase_values = {
            **expected_values,
            "phase": phase,
            "prior_artifact_path": prior_path,
            "prior_artifact_sha256": prior_sha256,
        }
        for field, expected in phase_values.items():
            if payload.get(field) != expected:
                raise LockdownError(f"execution artifact {path} does not bind {field}")
        if not isinstance(payload.get("exit_code"), int):
            raise LockdownError(f"execution artifact has no integer exit code: {path}")
        outcome = payload.get("outcome")
        if outcome not in {"KILLED", "SURVIVED"} or (
            (outcome == "KILLED") != (payload["exit_code"] != 0)
        ):
            raise LockdownError(f"execution artifact outcome is inconsistent: {path}")

    def _validate_probe_results(
        self,
        probe_plans: dict[str, tuple[Path, dict[str, object]]],
        results: list[dict[str, str]],
    ) -> tuple[int, set[Path]]:
        result_by_id: dict[str, dict[str, str]] = {}
        owned: set[Path] = set()
        for row in results:
            probe_id = row["probe_id"]
            if not probe_id or probe_id in result_by_id:
                raise LockdownError(f"blank or duplicate probe result: {probe_id!r}")
            if probe_id not in probe_plans:
                raise LockdownError(f"probe result names unknown probe plan: {probe_id}")
            result_by_id[probe_id] = row
        if set(result_by_id) != set(probe_plans):
            raise LockdownError(
                f"probe execution coverage is not exact: plans={sorted(probe_plans)}, "
                f"results={sorted(result_by_id)}"
            )
        artifact_paths: set[Path] = set()
        for probe_id, (plan_path, plan) in probe_plans.items():
            row = result_by_id[probe_id]
            run_path, run_payload, run_owned = self._execution_artifact(
                row["run_artifact_path"], row["run_artifact_sha256"], EVIDENCE_RUN_SCHEMA,
                f"probe {probe_id} run",
            )
            verify_path, verify_payload, verify_owned = self._execution_artifact(
                row["verification_artifact_path"], row["verification_artifact_sha256"],
                EVIDENCE_RUN_SCHEMA, f"probe {probe_id} verification",
            )
            if run_path in artifact_paths or verify_path in artifact_paths:
                raise LockdownError(f"probe execution artifact is referenced twice: {probe_id}")
            artifact_paths.update({run_path, verify_path})
            expected_keys = {
                "schema", "producer", "phase", "prior_artifact_path",
                "prior_artifact_sha256", "probe_id", "source_commit",
                "evidence_plan_path", "evidence_plan_sha256", "runner", "test_subject",
                "test_target", "named_test", "observation_path", "observation_sha256",
                "conclusion_sha256", "command_argv", "expected_exit_code",
                "exit_code", "normalized_observation_sha256", "output_path", "output_sha256",
            }
            expected = {
                "producer": CHECKER_SCHEMA,
                "probe_id": probe_id,
                "source_commit": self.source_commit,
                "evidence_plan_path": plan_path.as_posix(),
                "evidence_plan_sha256": sha256(self.root / plan_path),
                "runner": plan["runner"],
                "test_subject": plan["test_subject"],
                "test_target": plan["test_target"],
                "named_test": plan["named_test"],
                "observation_path": plan["observation_path"],
                "observation_sha256": plan["observation_sha256"],
                "conclusion_sha256": hashlib.sha256(
                    str(plan["conclusion"]).encode("utf-8")
                ).hexdigest(),
                "command_argv": self._runner_argv(
                    str(plan["runner"]), str(plan["test_subject"]),
                    str(plan["test_target"]), str(plan["named_test"]),
                    f"probe {probe_id}",
                ),
                "expected_exit_code": plan["expected_exit_code"],
            }
            for payload, phase, prior_path, prior_hash, path in [
                (run_payload, "run", "-", "-", run_path),
                (
                    verify_payload, "verify", run_path.as_posix(),
                    row["run_artifact_sha256"], verify_path,
                ),
            ]:
                if set(payload) != expected_keys:
                    raise LockdownError(f"probe execution artifact has unexpected fields: {path}")
                for field, value in {
                    **expected,
                    "phase": phase,
                    "prior_artifact_path": prior_path,
                    "prior_artifact_sha256": prior_hash,
                }.items():
                    if payload.get(field) != value:
                        raise LockdownError(f"probe execution artifact {path} does not bind {field}")
                if payload.get("exit_code") != plan["expected_exit_code"]:
                    raise LockdownError(f"probe {probe_id} observed the wrong exit code")
                execution_output = self.root / repository_path(
                    str(payload["output_path"]), "probe execution output_path"
                )
                output_bytes = execution_output.read_bytes()
                stdout, stderr = parse_command_output(output_bytes, f"probe {probe_id} execution")
                marker_hash = normalized_test_observation(
                    str(plan["runner"]), str(plan["named_test"]), int(payload["exit_code"]),
                    stdout, stderr,
                )
                if payload["normalized_observation_sha256"] != marker_hash:
                    raise LockdownError(
                        f"probe {probe_id} normalized observation is not derived from its log"
                    )
                self._validate_observation_output(
                    plan, stdout, stderr, f"probe {probe_id} {phase}"
                )
            if (
                verify_payload["normalized_observation_sha256"]
                != run_payload["normalized_observation_sha256"]
            ):
                raise LockdownError(f"probe {probe_id} verification did not reproduce its run")
            owned.update(run_owned | verify_owned)
        return len(results), owned

    def _load_mutation_operator(
        self, row: dict[str, str], rust_path: Path
    ) -> tuple[Path, dict[str, object]]:
        mutation_id = row["mutation_id"]
        operator_path = repository_path(
            row["operator_path"], f"mutation {mutation_id} operator_path"
        )
        if not (self.root / operator_path).is_relative_to(self.receipt_dir):
            raise LockdownError(f"mutation operator is outside the package receipt: {operator_path}")
        operator_file = self.root / operator_path
        if operator_file.is_symlink() or not operator_file.is_file() or sha256(
            operator_file
        ) != row["operator_sha256"]:
            raise LockdownError(f"mutation operator disappeared or drifted: {operator_path}")
        operator = read_strict_json(operator_file, "mutation operator")
        expected_keys = {"schema", "mutation_id", "rust_path", "source_sha256", "replacements"}
        replacements = operator.get("replacements") if isinstance(operator, dict) else None
        if (
            not isinstance(operator, dict)
            or set(operator) != expected_keys
            or operator.get("schema") != MUTATION_OPERATOR_SCHEMA
            or operator.get("mutation_id") != mutation_id
            or operator.get("rust_path") != rust_path.as_posix()
            or operator.get("source_sha256") != row["source_sha256"]
            or not isinstance(replacements, list)
            or not replacements
        ):
            raise LockdownError(f"mutation operator has the wrong schema or binding: {operator_path}")
        return operator_path, operator

    def _mutation_operator(
        self, row: dict[str, str], rust_path: Path
    ) -> tuple[Path, dict[str, object], bytes]:
        operator_path, operator = self._load_mutation_operator(row, rust_path)
        source = (self.root / rust_path).read_bytes()
        mutated = self._apply_mutation_operator(operator, source, operator_path)
        return operator_path, operator, mutated

    def _apply_mutation_operator(
        self, operator: dict[str, object], source: bytes, operator_path: Path
    ) -> bytes:
        replacements = operator["replacements"]
        if not isinstance(replacements, list):
            raise LockdownError(f"mutation operator replacements are invalid: {operator_path}")
        mutated = source
        for index, replacement in enumerate(replacements):
            if not isinstance(replacement, dict) or set(replacement) != {
                "old", "new", "expected_count",
            }:
                raise LockdownError(
                    f"mutation operator replacement {index} is incomplete: {operator_path}"
                )
            old = replacement.get("old")
            new = replacement.get("new")
            expected_count = replacement.get("expected_count")
            if (
                not isinstance(old, str)
                or not old
                or not isinstance(new, str)
                or old == new
                or not isinstance(expected_count, int)
                or expected_count < 1
            ):
                raise LockdownError(
                    f"mutation operator replacement {index} is invalid: {operator_path}"
                )
            old_bytes = old.encode("utf-8")
            if mutated.count(old_bytes) != expected_count:
                raise LockdownError(
                    f"mutation operator replacement {index} match count drifted: {operator_path}"
                )
            mutated = mutated.replace(old_bytes, new.encode("utf-8"))
        if mutated == source:
            raise LockdownError(f"mutation operator makes no source change: {operator_path}")
        return mutated

    def _mutation_plan_binding(
        self,
        row: dict[str, object],
        rust_path: Path,
        mutated_source_sha256: str,
        command_argv: list[str],
    ) -> dict[str, object]:
        return {
            "mutation_id": row["mutation_id"],
            "cluster_id": row["cluster_id"],
            "rule_ids": row["rule_ids"],
            "baseline_commit": row["baseline_commit"],
            "rust_path": rust_path.as_posix(),
            "source_sha256": row["source_sha256"],
            "runner": row["runner"],
            "test_subject": row["test_subject"],
            "test_target": row["test_target"],
            "named_test": row["named_test"],
            "command_argv": command_argv,
            "operator_path": row["operator_path"],
            "operator_sha256": row["operator_sha256"],
            "mutated_source_sha256": mutated_source_sha256,
        }

    def _load_attempt_plan(
        self, result: dict[str, str]
    ) -> tuple[Path, dict[str, object], set[Path]]:
        attempt_id = result["attempt_id"]
        path = repository_path(
            result["attempt_plan_path"], f"mutation {attempt_id} attempt_plan_path"
        )
        if not (self.root / path).is_relative_to(self.receipt_dir):
            raise LockdownError(f"mutation attempt plan is outside the package receipt: {path}")
        full_path = self.root / path
        if not full_path.is_file() or full_path.is_symlink() or sha256(full_path) != result[
            "attempt_plan_sha256"
        ]:
            raise LockdownError(f"mutation attempt plan disappeared or drifted: {path}")
        payload = read_strict_json(full_path, "mutation attempt plan")
        binding_keys = {
            "mutation_id", "cluster_id", "rule_ids", "baseline_commit", "rust_path",
            "source_sha256", "runner", "test_subject", "test_target", "named_test",
            "command_argv", "operator_path", "operator_sha256", "mutated_source_sha256",
        }
        if (
            not isinstance(payload, dict)
            or set(payload) != binding_keys | {"schema", "producer", "attempt_id"}
            or payload.get("schema") != MUTATION_ATTEMPT_PLAN_SCHEMA
            or payload.get("producer") != CHECKER_SCHEMA
            or payload.get("attempt_id") != attempt_id
            or payload.get("mutation_id") != result["mutation_id"]
        ):
            raise LockdownError(f"mutation attempt plan has the wrong schema or identity: {path}")
        rust_path = repository_path(str(payload["rust_path"]), f"mutation {attempt_id} rust_path")
        if rust_path not in self.owned_rust_files or not any(
            rust_path.is_relative_to(Path("rust/crates") / crate / "src")
            for crate in self.mapped_rust_crates
        ) or rust_path.suffix != ".rs":
            raise LockdownError(f"historical mutation source is not owned production Rust: {path}")
        baseline = str(payload["baseline_commit"])
        if not re.fullmatch(r"[0-9a-f]{40}", baseline):
            raise LockdownError(f"historical mutation has no full baseline commit: {path}")
        try:
            run(self.root, ["git", "merge-base", "--is-ancestor", baseline, "HEAD"])
        except LockdownError as error:
            raise LockdownError(
                f"historical mutation baseline is not an ancestor of HEAD: {path}"
            ) from error
        source = run_bytes(
            self.root, ["git", "cat-file", "blob", f"{baseline}:{rust_path.as_posix()}"],
        )
        if hashlib.sha256(source).hexdigest() != payload["source_sha256"]:
            raise LockdownError(f"historical mutation source differs from baseline: {path}")
        command = self._runner_argv(
            str(payload["runner"]), str(payload["test_subject"]),
            str(payload["test_target"]), str(payload["named_test"]),
            f"historical mutation {attempt_id}",
        )
        if payload["command_argv"] != command:
            raise LockdownError(f"historical mutation command binding drifted: {path}")
        operator_path, operator = self._load_mutation_operator(
            {key: str(payload[key]) for key in [
                "mutation_id", "operator_path", "operator_sha256", "source_sha256",
            ]},
            rust_path,
        )
        mutated = self._apply_mutation_operator(operator, source, operator_path)
        if hashlib.sha256(mutated).hexdigest() != payload["mutated_source_sha256"]:
            raise LockdownError(f"historical mutation operator binding drifted: {path}")
        return path, payload, {path, operator_path}

    def _mutation_plan_for_execution(
        self, mutation_id: str
    ) -> tuple[dict[str, str], Path, bytes, list[str]]:
        plans = read_tsv(self.mutation_plan_path, MUTATION_PLAN_HEADER)
        matching = [row for row in plans if row["mutation_id"] == mutation_id]
        if len(matching) != 1:
            raise LockdownError(f"mutation plan must contain exactly one {mutation_id!r} row")
        row = matching[0]
        if not re.fullmatch(r"[0-9a-f]{40}", row["baseline_commit"]):
            raise LockdownError(f"mutation {mutation_id} has no full baseline commit")
        try:
            run(self.root, [
                "git", "merge-base", "--is-ancestor", row["baseline_commit"], "HEAD",
            ])
        except LockdownError as error:
            raise LockdownError(
                f"mutation {mutation_id} baseline is not an ancestor of HEAD"
            ) from error
        rust_path = repository_path(row["rust_path"], f"mutation {mutation_id} rust_path")
        if rust_path not in self.owned_rust_files or not any(
            rust_path.is_relative_to(Path("rust/crates") / crate / "src")
            for crate in self.mapped_rust_crates
        ) or rust_path.suffix != ".rs":
            raise LockdownError(
                f"mutation {mutation_id} source is not an owned production Rust file"
            )
        full_path = self.root / rust_path
        if not full_path.is_file() or full_path.is_symlink():
            raise LockdownError(f"mutation source is not a regular file: {rust_path}")
        original = full_path.read_bytes()
        if hashlib.sha256(original).hexdigest() != row["source_sha256"]:
            raise LockdownError(f"mutation source hash drifted: {mutation_id}")
        baseline = run_bytes(
            self.root,
            ["git", "cat-file", "blob", f"{row['baseline_commit']}:{rust_path.as_posix()}"],
        )
        if baseline != original:
            raise LockdownError(
                f"mutation {mutation_id} baseline does not contain the claimed Rust source"
            )
        _operator_path, _operator, mutated = self._mutation_operator(row, rust_path)
        argv = self._runner_argv(
            row["runner"], row["test_subject"], row["test_target"], row["named_test"],
            f"mutation {mutation_id}",
        )
        return row, rust_path, mutated, argv

    def _probe_plans_for_execution(self) -> dict[str, tuple[Path, dict[str, object]]]:
        artifact_rows = self.artifact_rows()
        fixture_rows = self.fixture_accesses(artifact_rows)
        helper_calls = self.helper_call_rows()
        expected_helper_calls = tsv_text("helper-calls-v1", HELPER_CALL_HEADER, helper_calls)
        if (
            not self.helper_calls_path.is_file()
            or self.helper_calls_path.read_text(encoding="utf-8") != expected_helper_calls
        ):
            raise LockdownError("Go test helper-call inventory drifted; run generate and classify it")
        self.validate_helper_contracts(
            helper_calls, fixture_rows, {Path(row[0]) for row in artifact_rows}
        )
        self.go_generate_dependencies(artifact_rows)
        grouped = self.raw_obligations(artifact_rows)
        ledger_rows = self._stored_ledger_rows(grouped)
        symbols = read_tsv(self.symbols_path, SYMBOL_HEADER)
        verdict_plans = self._validate_verdicts(ledger_rows, symbols)[4]
        overlap = set(verdict_plans) & set(self.fixture_probe_plans)
        if overlap:
            raise LockdownError(f"fixture and verdict probe IDs overlap: {sorted(overlap)}")
        return {**verdict_plans, **self.fixture_probe_plans}

    def _probe_plan_for_execution(
        self, evidence_id: str
    ) -> tuple[Path, dict[str, object]]:
        """Validate one verdict probe without requiring unrelated final verdicts.

        Package completion remains atomic in ``check`` and ``write-receipt``. Evidence
        collection, however, is intentionally incremental: a measured probe may be run
        as soon as every obligation it declares has a complete DECLINED verdict. This
        keeps the executable evidence bound to the exact Go inventory while avoiding a
        circular dependency on finishing the rest of a large package first.
        """
        artifact_rows = self.artifact_rows()
        grouped = self.raw_obligations(artifact_rows)
        ledger_rows = self._stored_ledger_rows(grouped)
        selected: tuple[Path, dict[str, object]] | None = None
        referenced_ids: set[str] = set()
        declared_ids: set[str] | None = None
        for row in ledger_rows:
            if row["status"] != "DECLINED":
                continue
            identity = row["obligation_id"]
            go_quote = (
                f"go-quote:{row['source_path']}#{row['ast_anchor']}"
                f"@sha256:{row['node_sha256']}"
            )
            artifact, _owned, declared, payload = self._validate_evidence_artifact(
                identity, row["evidence"], go_quote, "measured-probe"
            )
            if payload["probe_id"] != evidence_id:
                continue
            current = (artifact, payload)
            if selected is not None and selected[0] != artifact:
                raise LockdownError(f"probe_id {evidence_id} names multiple evidence plans")
            if declared_ids is not None and declared_ids != declared:
                raise LockdownError(f"probe_id {evidence_id} has inconsistent obligation sets")
            selected = current
            declared_ids = declared
            referenced_ids.add(identity)
        if selected is not None:
            if declared_ids != referenced_ids:
                raise LockdownError(
                    f"probe {evidence_id} obligation coverage is not exact: "
                    f"declared={sorted(declared_ids or set())}, "
                    f"classified={sorted(referenced_ids)}"
                )
            return selected

        plans = self._probe_plans_for_execution()
        if evidence_id not in plans:
            raise LockdownError(f"unknown measured probe: {evidence_id}")
        return plans[evidence_id]

    def _write_execution_artifact(
        self,
        kind: str,
        identity: str,
        phase: str,
        schema: str,
        payload: dict[str, object],
        stdout: bytes,
        stderr: bytes,
    ) -> tuple[Path, str]:
        run_dir = self.receipt_dir / "execution" / kind
        output_path = run_dir / f"{identity}.{phase}.log"
        artifact_path = run_dir / f"{identity}.{phase}.json"
        if output_path.exists() or artifact_path.exists():
            raise LockdownError(f"execution artifact already exists: {kind}/{identity}.{phase}")
        atomic_write_bytes(output_path, command_output(stdout, stderr))
        complete_payload = {
            **payload,
            "schema": schema,
            "producer": CHECKER_SCHEMA,
            "phase": phase,
            "output_path": output_path.relative_to(self.root).as_posix(),
            "output_sha256": sha256(output_path),
        }
        atomic_write(artifact_path, canonical_json_text(complete_payload))
        return artifact_path.relative_to(self.root), sha256(artifact_path)

    def _write_support_log(
        self,
        kind: str,
        identity: str,
        phase: str,
        label: str,
        completed: subprocess.CompletedProcess[bytes],
    ) -> tuple[Path, str]:
        path = self.receipt_dir / "execution" / kind / f"{identity}.{phase}.{label}.log"
        if path.exists():
            raise LockdownError(f"execution support log already exists: {path}")
        atomic_write_bytes(path, command_output(completed.stdout, completed.stderr))
        return path.relative_to(self.root), sha256(path)

    def _write_attempt_plan(
        self,
        attempt_id: str,
        row: dict[str, str],
        rust_path: Path,
        mutated: bytes,
        argv: list[str],
    ) -> tuple[Path, str]:
        path = self.receipt_dir / "execution" / "mutation" / f"{attempt_id}.plan.json"
        if path.exists():
            raise LockdownError(f"mutation attempt plan already exists: {attempt_id}")
        payload = {
            "schema": MUTATION_ATTEMPT_PLAN_SCHEMA,
            "producer": CHECKER_SCHEMA,
            "attempt_id": attempt_id,
            **self._mutation_plan_binding(
                row, rust_path, hashlib.sha256(mutated).hexdigest(), argv
            ),
        }
        atomic_write(path, canonical_json_text(payload))
        return path.relative_to(self.root), sha256(path)

    def _execute_mutated_source(
        self,
        row: dict[str, str],
        rust_path: Path,
        mutated: bytes,
    ) -> tuple[
        list[str], subprocess.CompletedProcess[bytes], subprocess.CompletedProcess[bytes],
        subprocess.CompletedProcess[bytes], bytes,
    ]:
        source = self.root / rust_path
        original = source.read_bytes()
        argv, baseline = self._run_fixed_test(
            row["runner"], row["test_subject"], row["test_target"], row["named_test"],
            f"mutation {row['mutation_id']} baseline",
        )
        if baseline.returncode != 0:
            raise LockdownError(f"mutation {row['mutation_id']} baseline test did not pass")
        normalized_test_observation(
            row["runner"], row["named_test"], baseline.returncode,
            baseline.stdout, baseline.stderr,
        )
        mutated_result: subprocess.CompletedProcess[bytes] | None = None
        try:
            source.write_bytes(mutated)
            if source.read_bytes() != mutated:
                raise LockdownError(f"mutation {row['mutation_id']} did not install exact bytes")
            mutated_argv, mutated_result = self._run_fixed_test(
                row["runner"], row["test_subject"], row["test_target"], row["named_test"],
                f"mutation {row['mutation_id']} mutated",
            )
            if mutated_argv != argv:
                raise LockdownError(f"mutation {row['mutation_id']} runner argv drifted")
        finally:
            source.write_bytes(original)
        if source.read_bytes() != original:
            raise LockdownError(f"mutation {row['mutation_id']} did not restore exact source bytes")
        if mutated_result is None:
            raise LockdownError(f"mutation {row['mutation_id']} produced no test result")
        restored_argv, restored = self._run_fixed_test(
            row["runner"], row["test_subject"], row["test_target"], row["named_test"],
            f"mutation {row['mutation_id']} restored",
        )
        if restored_argv != argv or restored.returncode != 0:
            raise LockdownError(f"mutation {row['mutation_id']} restored test did not pass")
        normalized_test_observation(
            row["runner"], row["named_test"], restored.returncode,
            restored.stdout, restored.stderr,
        )
        return argv, baseline, mutated_result, restored, original

    def run_evidence(self, kind: str, evidence_id: str, attempt_id: str | None) -> str:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", evidence_id):
            raise LockdownError(f"unsafe evidence id: {evidence_id!r}")
        if kind == "mutation":
            if attempt_id is None or not re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9_.-]*", attempt_id
            ):
                raise LockdownError("mutation run-evidence requires a safe --attempt")
            row, rust_path, mutated, _argv = self._mutation_plan_for_execution(evidence_id)
            results = read_tsv(self.mutation_results_path, MUTATION_RESULT_HEADER)
            history_head, _previous = self._validate_mutation_history(results)
            if any(result["attempt_id"] == attempt_id for result in results):
                raise LockdownError(f"mutation attempt already exists: {attempt_id}")
            accepted = self._accepted_history_checkpoint()
            prior_checkpoint = accepted[0] if accepted is not None else "-"
            argv, baseline, completed, restored, original = self._execute_mutated_source(
                row, rust_path, mutated
            )
            outcome = "KILLED" if completed.returncode != 0 else "SURVIVED"
            observation_hash = normalized_test_observation(
                row["runner"], row["named_test"], completed.returncode,
                completed.stdout, completed.stderr,
            )
            baseline_observation = normalized_test_observation(
                row["runner"], row["named_test"], baseline.returncode,
                baseline.stdout, baseline.stderr,
            )
            restored_observation = normalized_test_observation(
                row["runner"], row["named_test"], restored.returncode,
                restored.stdout, restored.stderr,
            )
            attempt_plan_path, attempt_plan_hash = self._write_attempt_plan(
                attempt_id, row, rust_path, mutated, argv
            )
            baseline_log, baseline_log_hash = self._write_support_log(
                "mutation", attempt_id, "run", "baseline", baseline
            )
            restored_log, restored_log_hash = self._write_support_log(
                "mutation", attempt_id, "run", "restored", restored
            )
            artifact_path, artifact_hash = self._write_execution_artifact(
                "mutation", attempt_id, "run", MUTATION_RUN_SCHEMA,
                {
                    "prior_artifact_path": "-",
                    "prior_artifact_sha256": "-",
                    "attempt_id": attempt_id,
                    "mutation_id": evidence_id,
                    "attempt_plan_path": attempt_plan_path.as_posix(),
                    "attempt_plan_sha256": attempt_plan_hash,
                    "baseline_commit": row["baseline_commit"],
                    "rust_path": rust_path.as_posix(),
                    "original_source_sha256": hashlib.sha256(original).hexdigest(),
                    "mutated_source_sha256": hashlib.sha256(mutated).hexdigest(),
                    "operator_path": row["operator_path"],
                    "operator_sha256": row["operator_sha256"],
                    "command_argv": argv,
                    "named_test": row["named_test"],
                    "baseline_exit_code": baseline.returncode,
                    "baseline_output_path": baseline_log.as_posix(),
                    "baseline_output_sha256": baseline_log_hash,
                    "baseline_normalized_observation_sha256": baseline_observation,
                    "exit_code": completed.returncode,
                    "outcome": outcome,
                    "normalized_observation_sha256": observation_hash,
                    "restored_exit_code": restored.returncode,
                    "restored_output_path": restored_log.as_posix(),
                    "restored_output_sha256": restored_log_hash,
                    "restored_normalized_observation_sha256": restored_observation,
                    "restored_source_sha256": hashlib.sha256(original).hexdigest(),
                },
                completed.stdout,
                completed.stderr,
            )
            results.append({
                "sequence": str(len(results) + 1),
                "attempt_id": attempt_id,
                "mutation_id": evidence_id,
                "prior_history_sha256": history_head,
                "prior_checkpoint_sha256": prior_checkpoint,
                "attempt_plan_path": attempt_plan_path.as_posix(),
                "attempt_plan_sha256": attempt_plan_hash,
                "run_artifact_path": artifact_path.as_posix(),
                "run_artifact_sha256": artifact_hash,
                "verification_artifact_path": "-",
                "verification_artifact_sha256": "-",
                "history_sha256": "-",
            })
            atomic_write(
                self.mutation_results_path,
                tsv_text("mutation-results-v3", MUTATION_RESULT_HEADER, (
                    [result[column] for column in MUTATION_RESULT_HEADER] for result in results
                )),
            )
            return outcome

        if kind == "probe":
            if attempt_id is not None:
                raise LockdownError("probe run-evidence does not accept --attempt")
            plan_path, plan = self._probe_plan_for_execution(evidence_id)
            results = read_tsv(self.probe_results_path, PROBE_RESULT_HEADER)
            if any(result["probe_id"] == evidence_id for result in results):
                raise LockdownError(f"probe result already exists: {evidence_id}")
            argv, completed = self._run_fixed_test(
                str(plan["runner"]), str(plan["test_subject"]),
                str(plan["test_target"]), str(plan["named_test"]),
                f"probe {evidence_id}",
            )
            if completed.returncode != plan["expected_exit_code"]:
                raise LockdownError(
                    f"probe {evidence_id} exit {completed.returncode} differs from expected "
                    f"{plan['expected_exit_code']}"
                )
            observation_hash = normalized_test_observation(
                str(plan["runner"]), str(plan["named_test"]), completed.returncode,
                completed.stdout, completed.stderr,
            )
            self._validate_observation_output(
                plan, completed.stdout, completed.stderr, f"probe {evidence_id} run"
            )
            artifact_path, artifact_hash = self._write_execution_artifact(
                "probe", evidence_id, "run", EVIDENCE_RUN_SCHEMA,
                {
                    "prior_artifact_path": "-",
                    "prior_artifact_sha256": "-",
                    "probe_id": evidence_id,
                    "source_commit": self.source_commit,
                    "evidence_plan_path": plan_path.as_posix(),
                    "evidence_plan_sha256": sha256(self.root / plan_path),
                    "runner": plan["runner"],
                    "test_subject": plan["test_subject"],
                    "test_target": plan["test_target"],
                    "named_test": plan["named_test"],
                    "observation_path": plan["observation_path"],
                    "observation_sha256": plan["observation_sha256"],
                    "conclusion_sha256": hashlib.sha256(
                        str(plan["conclusion"]).encode("utf-8")
                    ).hexdigest(),
                    "command_argv": argv,
                    "expected_exit_code": plan["expected_exit_code"],
                    "exit_code": completed.returncode,
                    "normalized_observation_sha256": observation_hash,
                },
                completed.stdout,
                completed.stderr,
            )
            results.append({
                "probe_id": evidence_id,
                "run_artifact_path": artifact_path.as_posix(),
                "run_artifact_sha256": artifact_hash,
                "verification_artifact_path": "-",
                "verification_artifact_sha256": "-",
            })
            atomic_write(
                self.probe_results_path,
                tsv_text("probe-results-v1", PROBE_RESULT_HEADER, (
                    [result[column] for column in PROBE_RESULT_HEADER] for result in results
                )),
            )
            return "OBSERVED"
        raise LockdownError(f"unknown evidence kind: {kind!r}")

    def verify_evidence(self, kind: str, evidence_id: str, attempt_id: str | None) -> str:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", evidence_id):
            raise LockdownError(f"unsafe evidence id: {evidence_id!r}")
        if kind == "mutation":
            if attempt_id is None:
                raise LockdownError("mutation verify-evidence requires --attempt")
            plan, rust_path, mutated, _argv = self._mutation_plan_for_execution(evidence_id)
            results = read_tsv(self.mutation_results_path, MUTATION_RESULT_HEADER)
            self._validate_mutation_history(results, allow_unverified_tail=True)
            matching = [
                row for row in results
                if row["attempt_id"] == attempt_id and row["mutation_id"] == evidence_id
            ]
            if len(matching) != 1:
                raise LockdownError(f"unknown mutation attempt: {attempt_id}")
            result = matching[0]
            if result["verification_artifact_path"] != "-" or result[
                "verification_artifact_sha256"
            ] != "-":
                raise LockdownError(f"mutation attempt is already verified: {attempt_id}")
            run_path, run_payload, _run_owned = self._execution_artifact(
                result["run_artifact_path"], result["run_artifact_sha256"],
                MUTATION_RUN_SCHEMA, f"mutation {attempt_id} run",
            )
            argv, baseline, completed, restored, original = self._execute_mutated_source(
                plan, rust_path, mutated
            )
            outcome = "KILLED" if completed.returncode != 0 else "SURVIVED"
            observation_hash = normalized_test_observation(
                plan["runner"], plan["named_test"], completed.returncode,
                completed.stdout, completed.stderr,
            )
            baseline_observation = normalized_test_observation(
                plan["runner"], plan["named_test"], baseline.returncode,
                baseline.stdout, baseline.stderr,
            )
            restored_observation = normalized_test_observation(
                plan["runner"], plan["named_test"], restored.returncode,
                restored.stdout, restored.stderr,
            )
            if (
                completed.returncode != run_payload.get("exit_code")
                or outcome != run_payload.get("outcome")
                or observation_hash != run_payload.get("normalized_observation_sha256")
                or baseline_observation
                != run_payload.get("baseline_normalized_observation_sha256")
                or restored_observation
                != run_payload.get("restored_normalized_observation_sha256")
            ):
                raise LockdownError(f"mutation {attempt_id} verification did not reproduce its run")
            baseline_log, baseline_log_hash = self._write_support_log(
                "mutation", attempt_id, "verify", "baseline", baseline
            )
            restored_log, restored_log_hash = self._write_support_log(
                "mutation", attempt_id, "verify", "restored", restored
            )
            artifact_path, artifact_hash = self._write_execution_artifact(
                "mutation", attempt_id, "verify", MUTATION_RUN_SCHEMA,
                {
                    "prior_artifact_path": run_path.as_posix(),
                    "prior_artifact_sha256": result["run_artifact_sha256"],
                    "attempt_id": attempt_id,
                    "mutation_id": evidence_id,
                    "attempt_plan_path": result["attempt_plan_path"],
                    "attempt_plan_sha256": result["attempt_plan_sha256"],
                    "baseline_commit": plan["baseline_commit"],
                    "rust_path": rust_path.as_posix(),
                    "original_source_sha256": hashlib.sha256(original).hexdigest(),
                    "mutated_source_sha256": hashlib.sha256(mutated).hexdigest(),
                    "operator_path": plan["operator_path"],
                    "operator_sha256": plan["operator_sha256"],
                    "command_argv": argv,
                    "named_test": plan["named_test"],
                    "baseline_exit_code": baseline.returncode,
                    "baseline_output_path": baseline_log.as_posix(),
                    "baseline_output_sha256": baseline_log_hash,
                    "baseline_normalized_observation_sha256": baseline_observation,
                    "exit_code": completed.returncode,
                    "outcome": outcome,
                    "normalized_observation_sha256": observation_hash,
                    "restored_exit_code": restored.returncode,
                    "restored_output_path": restored_log.as_posix(),
                    "restored_output_sha256": restored_log_hash,
                    "restored_normalized_observation_sha256": restored_observation,
                    "restored_source_sha256": hashlib.sha256(original).hexdigest(),
                },
                completed.stdout,
                completed.stderr,
            )
            result["verification_artifact_path"] = artifact_path.as_posix()
            result["verification_artifact_sha256"] = artifact_hash
            result["history_sha256"] = self._mutation_history_hash(result)
            atomic_write(
                self.mutation_results_path,
                tsv_text("mutation-results-v3", MUTATION_RESULT_HEADER, (
                    [row[column] for column in MUTATION_RESULT_HEADER] for row in results
                )),
            )
            return outcome

        if kind == "probe":
            if attempt_id is not None:
                raise LockdownError("probe verify-evidence does not accept --attempt")
            _plan_path, plan = self._probe_plan_for_execution(evidence_id)
            results = read_tsv(self.probe_results_path, PROBE_RESULT_HEADER)
            matching = [row for row in results if row["probe_id"] == evidence_id]
            if len(matching) != 1:
                raise LockdownError(f"unknown probe result: {evidence_id}")
            result = matching[0]
            if result["verification_artifact_path"] != "-" or result[
                "verification_artifact_sha256"
            ] != "-":
                raise LockdownError(f"probe is already verified: {evidence_id}")
            run_path, run_payload, _run_owned = self._execution_artifact(
                result["run_artifact_path"], result["run_artifact_sha256"],
                EVIDENCE_RUN_SCHEMA, f"probe {evidence_id} run",
            )
            argv, completed = self._run_fixed_test(
                str(plan["runner"]), str(plan["test_subject"]),
                str(plan["test_target"]), str(plan["named_test"]),
                f"probe {evidence_id}",
            )
            observation_hash = normalized_test_observation(
                str(plan["runner"]), str(plan["named_test"]), completed.returncode,
                completed.stdout, completed.stderr,
            )
            self._validate_observation_output(
                plan, completed.stdout, completed.stderr, f"probe {evidence_id} verification"
            )
            if (
                completed.returncode != run_payload.get("exit_code")
                or observation_hash != run_payload.get("normalized_observation_sha256")
            ):
                raise LockdownError(f"probe {evidence_id} verification did not reproduce its run")
            artifact_path, artifact_hash = self._write_execution_artifact(
                "probe", evidence_id, "verify", EVIDENCE_RUN_SCHEMA,
                {
                    "prior_artifact_path": run_path.as_posix(),
                    "prior_artifact_sha256": result["run_artifact_sha256"],
                    "probe_id": evidence_id,
                    "source_commit": self.source_commit,
                    "evidence_plan_path": str(run_payload["evidence_plan_path"]),
                    "evidence_plan_sha256": str(run_payload["evidence_plan_sha256"]),
                    "runner": plan["runner"],
                    "test_subject": plan["test_subject"],
                    "test_target": plan["test_target"],
                    "named_test": plan["named_test"],
                    "observation_path": plan["observation_path"],
                    "observation_sha256": plan["observation_sha256"],
                    "conclusion_sha256": hashlib.sha256(
                        str(plan["conclusion"]).encode("utf-8")
                    ).hexdigest(),
                    "command_argv": argv,
                    "expected_exit_code": plan["expected_exit_code"],
                    "exit_code": completed.returncode,
                    "normalized_observation_sha256": observation_hash,
                },
                completed.stdout,
                completed.stderr,
            )
            result["verification_artifact_path"] = artifact_path.as_posix()
            result["verification_artifact_sha256"] = artifact_hash
            atomic_write(
                self.probe_results_path,
                tsv_text("probe-results-v1", PROBE_RESULT_HEADER, (
                    [row[column] for column in PROBE_RESULT_HEADER] for row in results
                )),
            )
            return "VERIFIED"
        raise LockdownError(f"unknown evidence kind: {kind!r}")

    def _owned_hashes(
        self,
        grouped: dict[Path, list[list[str]]],
        symbols: list[dict[str, str]],
        evidence_paths: set[Path],
    ) -> dict[str, str]:
        paths = {
            self.spec_relative,
            self.artifacts_path.relative_to(self.root),
            self.symbols_path.relative_to(self.root),
            self.rules_path.relative_to(self.root),
            self.mutation_plan_path.relative_to(self.root),
            self.mutation_results_path.relative_to(self.root),
            self.probe_results_path.relative_to(self.root),
            self.helper_calls_path.relative_to(self.root),
            self.helper_contracts_path.relative_to(self.root),
            *[self._ledger_path(source).relative_to(self.root) for source in grouped],
            *self.owned_rust_files,
            *[repository_path(row["anchor_path"], "symbol anchor_path") for row in symbols],
            *self.symbol_support_paths,
            *evidence_paths,
        }
        receipt_relative = self.receipt_dir.relative_to(self.root)
        receipt_paths = {
            path for path in paths
            if path == receipt_relative or path.is_relative_to(receipt_relative)
        }
        for path in receipt_paths:
            if path.suffix in {".rs", ".go"}:
                raise LockdownError(
                    f"receipt proof directory cannot own production/test source: {path}"
                )
        actual_receipt_paths = set(self._git_paths([
            "ls-files", "-z", "--", receipt_relative.as_posix(),
        ]))
        actual_receipt_paths.update(self._git_paths([
            "ls-files", "-z", "--others", "--exclude-standard", "--",
            receipt_relative.as_posix(),
        ]))
        receipt_path = self.receipt_path.relative_to(self.root)
        extra = actual_receipt_paths - receipt_paths - {receipt_path}
        if extra:
            raise LockdownError(
                "receipt proof directory contains an unowned artifact: "
                + ", ".join(path.as_posix() for path in sorted(
                    extra, key=lambda item: item.as_posix()
                ))
            )
        missing = receipt_paths - actual_receipt_paths
        if missing:
            raise LockdownError(
                "receipt-owned artifact is absent from the exact receipt directory census: "
                + ", ".join(path.as_posix() for path in sorted(
                    missing, key=lambda item: item.as_posix()
                ))
            )
        self.receipt_owned_paths = receipt_paths
        hashes: dict[str, str] = {}
        for path in sorted(paths, key=lambda item: item.as_posix()):
            full_path = self.root / path
            if not full_path.is_file():
                raise LockdownError(f"receipt-owned file disappeared: {path}")
            hashes[path.as_posix()] = sha256(full_path)
        return hashes

    def _validate(self) -> tuple[dict[str, object], int, int]:
        artifact_rows = self.artifact_rows()
        fixture_accesses = self.fixture_accesses(artifact_rows)
        helper_calls = self.helper_call_rows()
        expected_helper_calls = tsv_text("helper-calls-v1", HELPER_CALL_HEADER, helper_calls)
        if (
            not self.helper_calls_path.is_file()
            or self.helper_calls_path.read_text(encoding="utf-8") != expected_helper_calls
        ):
            raise LockdownError("Go test helper-call inventory drifted; run generate and classify it")
        helper_contracts, helper_evidence_paths = self.validate_helper_contracts(
            helper_calls, fixture_accesses, {Path(row[0]) for row in artifact_rows}
        )
        generate_dependencies = self.go_generate_dependencies(artifact_rows)
        expected_artifacts = tsv_text(ARTIFACT_SCHEMA, ARTIFACT_HEADER, artifact_rows)
        if not self.artifacts_path.is_file() or self.artifacts_path.read_text(encoding="utf-8") != expected_artifacts:
            raise LockdownError("package artifact manifest drifted; run generate only after reviewing the change")
        grouped = self.raw_obligations(artifact_rows)
        ledger_rows = self._stored_ledger_rows(grouped)
        symbols = read_tsv(self.symbols_path, SYMBOL_HEADER)
        rules = read_tsv(self.rules_path, RULE_HEADER)
        plans = read_tsv(self.mutation_plan_path, MUTATION_PLAN_HEADER)
        results = read_tsv(self.mutation_results_path, MUTATION_RESULT_HEADER)
        probe_results = read_tsv(self.probe_results_path, PROBE_RESULT_HEADER)
        (
            symbol_by_id, ported_ids, _used_symbols, verdict_evidence_paths, probe_plans,
        ) = self._validate_verdicts(ledger_rows, symbols)
        overlap = set(probe_plans) & set(self.fixture_probe_plans)
        if overlap:
            raise LockdownError(f"fixture and verdict probe IDs overlap: {sorted(overlap)}")
        probe_plans = {**probe_plans, **self.fixture_probe_plans}
        rule_by_id, plan_by_id, outcome_counts, mutation_evidence_paths = (
            self._validate_rules_and_mutations(
                ledger_rows, symbol_by_id, ported_ids, rules, plans, results
            )
        )
        probe_run_count, probe_evidence_paths = self._validate_probe_results(
            probe_plans, probe_results
        )
        mutation_history_head = (
            results[-1]["history_sha256"] if results else self._mutation_history_genesis()
        )

        role_counts: dict[str, int] = {role: 0 for role in ALLOWED_ARTIFACT_ROLES}
        trait_counts: dict[str, int] = {
            trait: 0 for trait in [
                "build-tag", "platform-variant", "generated", "go-generate", "go-embed",
                "go-embed-input", "testdata",
            ]
        }
        for row in artifact_rows:
            role_counts[row[1]] = role_counts.get(row[1], 0) + 1
            for trait in split_list(row[2].replace(",", ";")) if row[2] != "-" else []:
                trait_counts[trait] = trait_counts.get(trait, 0) + 1
        category_counts: dict[str, int] = {}
        status_counts: dict[str, int] = {}
        for row in ledger_rows:
            category_counts[row["category"]] = category_counts.get(row["category"], 0) + 1
            status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1
        receipt: dict[str, object] = {
            "schema": RECEIPT_SCHEMA,
            "checker_schema": CHECKER_SCHEMA,
            "claim": "whole-go-package",
            "completion_kind": (
                "falsification" if not ported_ids
                else "classified-gaps" if status_counts.get("DECLINED", 0)
                else "lockdown"
            ),
            "inventory_complete": True,
            "implementation_complete": bool(ported_ids) and not status_counts.get("DECLINED", 0),
            "go_package": self.go_package.as_posix(),
            "source_commit": self.source_commit,
            "primary_rust_crate": self.primary_rust_crate,
            "mapped_rust_crates": self.mapped_rust_crates,
            "excluded_subpackages": {
                path.as_posix(): proof for path, proof in sorted(self.excluded_subpackages.items())
            },
            "artifact_count": len(artifact_rows),
            "artifact_role_counts": dict(sorted(role_counts.items())),
            "artifact_trait_counts": dict(sorted(trait_counts.items())),
            "fixture_access_count": len(fixture_accesses),
            "helper_call_count": len(helper_calls),
            "helper_contract_count": len(helper_contracts),
            "helper_contract_status_counts": {
                status: sum(row["status"] == status for row in helper_contracts)
                for status in sorted(ALLOWED_HELPER_STATUSES)
            },
            "unresolved_fixture_count": len(self.unresolved_fixture_evidence),
            "go_generate_directive_count": len(generate_dependencies),
            "go_generate_dependencies": generate_dependencies,
            "go_embed_match_count": len(self.go_embed_rows),
            "go_embed_directive_count": len(self.go_embed_rows),
            "go_embed_dependency_count": len(self.embed_dependency_paths),
            "go_embed_inventory": self.go_embed_rows,
            "go_file_count": len(grouped),
            "obligation_count": len(ledger_rows),
            "category_counts": dict(sorted(category_counts.items())),
            "status_counts": dict(sorted(status_counts.items())),
            "symbol_count": len(symbols),
            "semantic_rule_count": len(rule_by_id),
            "mutation_count": len(plan_by_id),
            "mutation_attempt_count": len(results),
            "mutation_outcome_counts": dict(sorted(outcome_counts.items())),
            "mutation_history_schema": MUTATION_HISTORY_SCHEMA,
            "mutation_history_genesis_sha256": self._mutation_history_genesis(),
            "mutation_history_head_sha256": mutation_history_head,
            "mutation_history": [
                {column: row[column] for column in MUTATION_RESULT_HEADER}
                for row in results
            ],
            "measured_probe_count": len(probe_plans),
            "measured_probe_run_count": probe_run_count,
            "owned_file_sha256": self._owned_hashes(
                grouped,
                symbols,
                verdict_evidence_paths
                | self.fixture_evidence_paths
                | helper_evidence_paths
                | mutation_evidence_paths
                | probe_evidence_paths,
            ),
        }
        self._validate_mapped_rust_change_census()
        return receipt, len(artifact_rows), len(ledger_rows)

    def write_receipt(self) -> tuple[int, int]:
        receipt, artifacts, obligations = self._validate()
        atomic_write(self.receipt_path, canonical_json_text(receipt))
        return artifacts, obligations

    def check(self) -> tuple[int, int]:
        receipt, artifacts, obligations = self._validate()
        if not self.receipt_path.is_file():
            raise LockdownError("content-addressed receipt is missing; run write-receipt")
        try:
            raw = self.receipt_path.read_text(encoding="utf-8")
        except OSError as error:
            raise LockdownError(f"cannot read receipt {self.receipt_path}: {error}") from error
        strict_json_loads(raw, f"package receipt {self.receipt_path}")
        if raw != canonical_json_text(receipt):
            raise LockdownError(
                "content-addressed package receipt is not exact canonical JSON; "
                "run write-receipt after review"
            )
        return artifacts, obligations


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ["generate", "check", "write-receipt"]:
        child = subparsers.add_parser(command)
        child.add_argument("--spec", required=True, type=Path)
        child.add_argument("--accepted-source-commit", required=True)
    for command in ["run-evidence", "verify-evidence"]:
        child = subparsers.add_parser(command)
        child.add_argument("--spec", required=True, type=Path)
        child.add_argument("--accepted-source-commit", required=True)
        child.add_argument("--kind", required=True, choices=["mutation", "probe"])
        child.add_argument("--id", required=True)
        child.add_argument("--attempt")
    return parser.parse_args()


def main() -> int:
    args = parse_arguments()
    try:
        lockdown = PackageLockdown(args.root, args.spec, args.accepted_source_commit)
        if args.command == "generate":
            artifacts, obligations = lockdown.generate()
            action = "generated"
        elif args.command == "write-receipt":
            artifacts, obligations = lockdown.write_receipt()
            action = "wrote receipt for"
        elif args.command == "run-evidence":
            outcome = lockdown.run_evidence(args.kind, args.id, args.attempt)
            print(f"ran {args.kind} evidence {args.id}: {outcome}")
            return 0
        elif args.command == "verify-evidence":
            outcome = lockdown.verify_evidence(args.kind, args.id, args.attempt)
            print(f"verified {args.kind} evidence {args.id}: {outcome}")
            return 0
        else:
            artifacts, obligations = lockdown.check()
            action = "checked"
        print(
            f"{action} {lockdown.go_package}: {artifacts} artifacts, "
            f"{obligations} AST obligations"
        )
    except (LockdownError, OSError) as error:
        print(f"go-package-lockdown failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
