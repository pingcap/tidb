#!/usr/bin/env python3
"""Check checked-in TiPB projections against the pinned Go TiPB schemas."""

from __future__ import annotations

import ast
import pathlib
import re
import subprocess
import sys
import tempfile


ROOT = pathlib.Path(__file__).resolve().parents[2]
RUST_PROTO_DIR = ROOT / "rust/crates/tidb-proto/proto"
GO_MODULE = "github.com/pingcap/tipb"
PARTIAL_ENUMS = {
    # The Rust planner intentionally projects only executor/expression kinds
    # that its TiKV lowering currently constructs. New upstream values must be
    # reviewed before those projections are expanded.
    ".tipb.ExecType",
    ".tipb.ExprType",
}
def run(command: list[str], *, input_bytes: bytes | None = None) -> bytes:
    result = subprocess.run(
        command,
        cwd=ROOT,
        input=input_bytes,
        check=True,
        capture_output=True,
    )
    return result.stdout


def descriptor_files(descriptor_set: bytes) -> list[dict[str, list[object]]]:
    """Decode protoc's descriptor text without adding a Python protobuf dependency."""
    output = run(
        [
            "protoc",
            "--decode=google.protobuf.FileDescriptorSet",
            "google/protobuf/descriptor.proto",
        ],
        input_bytes=descriptor_set,
    ).decode()
    root: dict[str, list[object]] = {"file": []}
    stack = [root]
    for line in output.splitlines():
        item = line.strip()
        if item == "}":
            stack.pop()
            continue
        section = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\s*\{", item)
        if section:
            child: dict[str, list[object]] = {}
            stack[-1].setdefault(section.group(1), []).append(child)
            stack.append(child)
            continue
        if ":" not in item:
            raise ValueError(f"unrecognized protoc descriptor output: {line}")
        name, raw_value = item.split(":", 1)
        raw_value = raw_value.strip()
        if raw_value.startswith('"'):
            value: object = ast.literal_eval(raw_value)
        elif raw_value in ("true", "false"):
            value = raw_value == "true"
        else:
            try:
                value = int(raw_value)
            except ValueError:
                value = raw_value
        stack[-1].setdefault(name, []).append(value)
    if stack != [root]:
        raise ValueError("unterminated protoc descriptor section")
    return root["file"]  # type: ignore[return-value]


def compile_descriptors(
    sources: list[pathlib.Path], include_dirs: list[pathlib.Path]
) -> list[dict[str, list[object]]]:
    with tempfile.TemporaryDirectory(prefix="tipb-proto-check-") as directory:
        descriptor_path = pathlib.Path(directory) / "descriptor.pb"
        command = ["protoc"]
        for include_dir in include_dirs:
            command.extend(["--proto_path", str(include_dir)])
        command.extend(
            [
                "--include_imports",
                f"--descriptor_set_out={descriptor_path}",
                *(str(source) for source in sources),
            ]
        )
        run(command)
        return descriptor_files(descriptor_path.read_bytes())


def value(descriptor: dict[str, list[object]], name: str) -> object | None:
    values = descriptor.get(name, [])
    return values[0] if values else None


def collect_symbols(
    files: list[dict[str, list[object]]],
) -> tuple[dict[str, dict[str, list[object]]], dict[str, dict[str, list[object]]]]:
    messages: dict[str, dict[str, list[object]]] = {}
    enums: dict[str, dict[str, list[object]]] = {}

    def collect_message(message: dict[str, list[object]], parent: str) -> None:
        full_name = f"{parent}.{value(message, 'name')}"
        messages[full_name] = message
        for enum in message.get("enum_type", []):
            enums[f"{full_name}.{value(enum, 'name')}"] = enum
        for nested in message.get("nested_type", []):
            collect_message(nested, full_name)

    for file in files:
        package = str(value(file, "package") or "")
        parent = f".{package}" if package else ""
        for message in file.get("message_type", []):
            collect_message(message, parent)
        for enum in file.get("enum_type", []):
            enums[f"{parent}.{value(enum, 'name')}"] = enum
    return messages, enums


def enum_values(enum: dict[str, list[object]]) -> dict[str, object]:
    return {
        str(value(item, "name")): value(item, "number")
        for item in enum.get("value", [])
    }


def oneof_name(
    message: dict[str, list[object]], field: dict[str, list[object]]
) -> object | None:
    index = value(field, "oneof_index")
    if index is None:
        return None
    return value(message.get("oneof_decl", [])[int(index)], "name")


def compare_projection(
    local_files: list[dict[str, list[object]]],
    upstream_files: list[dict[str, list[object]]],
) -> list[str]:
    local_messages, local_enums = collect_symbols(local_files)
    upstream_messages, upstream_enums = collect_symbols(upstream_files)
    errors: list[str] = []

    for name, local in local_messages.items():
        upstream = upstream_messages.get(name)
        if upstream is None:
            errors.append(f"{name}: no message with this name in pinned TiPB")
            continue
        upstream_fields = {
            value(field, "name"): field for field in upstream.get("field", [])
        }
        for field in local.get("field", []):
            field_name = value(field, "name")
            reference = upstream_fields.get(field_name)
            if reference is None:
                errors.append(f"{name}.{field_name}: field is absent from pinned TiPB")
                continue
            for attribute in ("number", "type", "type_name", "label"):
                if value(field, attribute) != value(reference, attribute):
                    errors.append(
                        f"{name}.{field_name}: {attribute} differs from pinned TiPB"
                    )
            if oneof_name(local, field) != oneof_name(upstream, reference):
                errors.append(f"{name}.{field_name}: oneof membership differs from pinned TiPB")

    for name, local in local_enums.items():
        upstream = upstream_enums.get(name)
        if upstream is None:
            errors.append(f"{name}: no enum with this name in pinned TiPB")
            continue
        local_values = enum_values(local)
        upstream_values = enum_values(upstream)
        for value_name, number in local_values.items():
            if upstream_values.get(value_name) != number:
                errors.append(f"{name}.{value_name}: value differs from pinned TiPB")
        if name not in PARTIAL_ENUMS and local_values != upstream_values:
            errors.append(
                f"{name}: enum is stale ({len(local_values)} local values, "
                f"{len(upstream_values)} pinned values)"
            )
    return errors


def main() -> int:
    try:
        module_dir, version = run(
            ["go", "list", "-m", "-f", "{{.Dir}} {{.Version}}", GO_MODULE]
        ).decode().strip().rsplit(" ", 1)
        module_dir_path = pathlib.Path(module_dir)
        proto_dir = module_dir_path / "proto"
        upstream = compile_descriptors(
            [
                proto_dir / "select.proto",
                proto_dir / "expression.proto",
                proto_dir / "analyze.proto",
                proto_dir / "resourcetag.proto",
            ],
            [proto_dir, module_dir_path / "include"],
        )
        local = compile_descriptors(
            [
                RUST_PROTO_DIR / "select.proto",
                RUST_PROTO_DIR / "analyze.proto",
                RUST_PROTO_DIR / "resourcetag.proto",
            ],
            [RUST_PROTO_DIR],
        )
        errors = compare_projection(local, upstream)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"TiPB projection check failed: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            print(error.stderr.decode(), file=sys.stderr, end="")
        return 2

    if errors:
        print(
            f"TiPB projection differs from github.com/pingcap/tipb@{version}:",
            file=sys.stderr,
        )
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        return 1
    local_messages, local_enums = collect_symbols(local)
    print(
        f"TiPB projections match pinned wire declarations "
        f"({len(local_messages)} messages, {len(local_enums)} enums; {version})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
