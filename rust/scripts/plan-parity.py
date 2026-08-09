#!/usr/bin/env python3
"""Compare Go and Rust TiDB physical plans for pinned workload SQL.

Prepared cases prepare ``EXPLAIN FORMAT='brief' <workload SQL>`` through the
MySQL binary protocol, bind the workload's exact parameter types, and execute
the statement. Direct cases issue a text-protocol EXPLAIN. The gate deliberately
normalizes only generated operator IDs and internal ``Column#N`` ordinals.
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import importlib.util
import json
import re
import struct
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Callable


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_MANIFEST = SCRIPT_DIR / "tpcc-sysbench-plan-manifest.json"
PROTOCOL_CLIENT = SCRIPT_DIR / "mysql-prepared-client.py"

COM_QUERY = 0x03
COM_STMT_EXECUTE = 0x17

MYSQL_TYPE_LONG = 0x03
MYSQL_TYPE_DOUBLE = 0x05
MYSQL_TYPE_NULL = 0x06
MYSQL_TYPE_LONGLONG = 0x08
MYSQL_TYPE_BLOB = 0xFC
MYSQL_TYPE_VAR_STRING = 0xFD
MYSQL_TYPE_STRING = 0xFE
UNSIGNED_FLAG = 0x80

STRING_RESULT_TYPES = {
    MYSQL_TYPE_BLOB,
    MYSQL_TYPE_VAR_STRING,
    MYSQL_TYPE_STRING,
}

DATABASE_NAME = re.compile(r"^[A-Za-z0-9_$]+$")
OPERATOR_ID = re.compile(r"(?<=[A-Za-z)])_\d+(?=(?:\([^)]*\))?$)")
COLUMN_ORDINAL = re.compile(r"Column#\d+")
MULTI_ROW_INSERT_ROW = re.compile(r"^\(.*\)(?:/\*.*\*/)?$", re.DOTALL)


class GateError(RuntimeError):
    """The parity gate cannot produce trustworthy evidence."""


@dataclass(frozen=True)
class Endpoint:
    name: str
    host: str
    port: int
    user: str
    password: str


@dataclass(frozen=True)
class Parameter:
    type_name: str
    value: Any


def load_protocol_client() -> ModuleType:
    """Load the existing bounded MySQL protocol implementation by path."""
    spec = importlib.util.spec_from_file_location("tidb_mysql_protocol", PROTOCOL_CLIENT)
    if spec is None or spec.loader is None:
        raise GateError(f"cannot load protocol client: {PROTOCOL_CLIENT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def encode_lenenc(value: int) -> bytes:
    """Encode a non-negative MySQL length-encoded integer."""
    if value < 0:
        raise GateError("length-encoded integers cannot be negative")
    if value < 0xFB:
        return bytes((value,))
    if value <= 0xFFFF:
        return b"\xfc" + value.to_bytes(2, "little")
    if value <= 0xFFFFFF:
        return b"\xfd" + value.to_bytes(3, "little")
    if value <= 0xFFFFFFFFFFFFFFFF:
        return b"\xfe" + value.to_bytes(8, "little")
    raise GateError("length-encoded integer exceeds uint64")


def encode_lenenc_bytes(value: bytes) -> bytes:
    return encode_lenenc(len(value)) + value


def encode_signed(value: Any, width: int) -> bytes:
    if isinstance(value, bool) or not isinstance(value, int):
        raise GateError(f"expected signed integer, got {value!r}")
    try:
        return value.to_bytes(width, "little", signed=True)
    except OverflowError as error:
        raise GateError(f"signed {width * 8}-bit value out of range: {value}") from error


def encode_unsigned(value: Any, width: int) -> bytes:
    if isinstance(value, bool) or not isinstance(value, int):
        raise GateError(f"expected unsigned integer, got {value!r}")
    try:
        return value.to_bytes(width, "little", signed=False)
    except OverflowError as error:
        raise GateError(f"unsigned {width * 8}-bit value out of range: {value}") from error


def encode_double(value: Any) -> bytes:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GateError(f"expected double, got {value!r}")
    return struct.pack("<d", float(value))


def encode_string(value: Any) -> bytes:
    if not isinstance(value, str):
        raise GateError(f"expected string, got {value!r}")
    return encode_lenenc_bytes(value.encode("utf-8"))


def encode_bytes(value: Any) -> bytes:
    if not isinstance(value, (bytes, bytearray)):
        raise GateError(f"expected bytes, got {value!r}")
    return encode_lenenc_bytes(bytes(value))


PARAMETER_ENCODERS: dict[str, tuple[int, int, Callable[[Any], bytes]]] = {
    "i32": (MYSQL_TYPE_LONG, 0, lambda value: encode_signed(value, 4)),
    "u32": (MYSQL_TYPE_LONG, UNSIGNED_FLAG, lambda value: encode_unsigned(value, 4)),
    "i64": (MYSQL_TYPE_LONGLONG, 0, lambda value: encode_signed(value, 8)),
    "u64": (
        MYSQL_TYPE_LONGLONG,
        UNSIGNED_FLAG,
        lambda value: encode_unsigned(value, 8),
    ),
    "f64": (MYSQL_TYPE_DOUBLE, 0, encode_double),
    "string": (MYSQL_TYPE_STRING, 0, encode_string),
    "bytes": (MYSQL_TYPE_BLOB, 0, encode_bytes),
    "null": (MYSQL_TYPE_NULL, 0, lambda _value: b""),
}


def parse_parameters(raw: Any) -> list[Parameter]:
    if not isinstance(raw, list):
        raise GateError("case params must be a list")
    parameters: list[Parameter] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict) or set(item) != {"type", "value"}:
            raise GateError(f"parameter {index} must contain exactly type and value")
        type_name = item["type"]
        if type_name not in PARAMETER_ENCODERS:
            raise GateError(f"parameter {index} has unsupported type {type_name!r}")
        if type_name == "null" and item["value"] is not None:
            raise GateError(f"parameter {index} null type must have a null value")
        parameters.append(Parameter(type_name, item["value"]))
    return parameters


def build_execute_payload(statement_id: int, parameters: list[Parameter]) -> bytes:
    """Build a COM_STMT_EXECUTE body with explicit per-parameter MySQL types."""
    payload = bytearray(statement_id.to_bytes(4, "little"))
    payload.append(0)  # no cursor
    payload.extend((1).to_bytes(4, "little"))

    null_bitmap = bytearray((len(parameters) + 7) // 8)
    for index, parameter in enumerate(parameters):
        if parameter.type_name == "null":
            null_bitmap[index // 8] |= 1 << (index % 8)
    payload.extend(null_bitmap)
    payload.append(1)  # new parameter types are present

    for parameter in parameters:
        type_code, flags, _ = PARAMETER_ENCODERS[parameter.type_name]
        payload.extend((type_code, flags))
    for parameter in parameters:
        _, _, encoder = PARAMETER_ENCODERS[parameter.type_name]
        payload.extend(encoder(parameter.value))
    return bytes(payload)


def is_result_terminator(payload: bytes) -> bool:
    return bool(payload) and payload[0] == 0xFE and len(payload) < 9


def read_columns(connection: Any, first: bytes, protocol: ModuleType) -> tuple[list[Any], int]:
    column_count, offset = protocol.read_lenenc(first, 0)
    if column_count is None or offset != len(first):
        raise GateError(f"invalid result column-count packet: {first.hex()}")
    sequence = 2
    columns = []
    for _ in range(column_count):
        columns.append(protocol.parse_column(connection.read_packet(sequence)))
        sequence += 1
    if column_count and not connection.deprecate_eof:
        protocol.assert_eof(connection.read_packet(sequence), False)
        sequence += 1
    return columns, sequence


def read_binary_string_rows(
    connection: Any, first: bytes, protocol: ModuleType
) -> list[list[str | None]]:
    columns, sequence = read_columns(connection, first, protocol)
    if any(column.type_code not in STRING_RESULT_TYPES for column in columns):
        raise GateError(
            "EXPLAIN returned non-string columns: "
            + ", ".join(f"{column.name}={column.type_code}" for column in columns)
        )
    rows: list[list[str | None]] = []
    null_bytes = (len(columns) + 9) // 8
    while True:
        packet = connection.read_packet(sequence)
        sequence += 1
        if is_result_terminator(packet):
            protocol.assert_eof(packet, connection.deprecate_eof)
            return rows
        if not packet or packet[0] != 0:
            raise GateError(f"invalid binary row: {packet.hex()}")
        if len(packet) < 1 + null_bytes:
            raise GateError(f"truncated binary row: {packet.hex()}")
        bitmap = packet[1 : 1 + null_bytes]
        offset = 1 + null_bytes
        row: list[str | None] = []
        for index, _column in enumerate(columns):
            if bitmap[(index + 2) // 8] & (1 << ((index + 2) % 8)):
                row.append(None)
                continue
            value, offset = protocol.read_lenenc_bytes(packet, offset)
            if value is None:
                raise GateError("binary row used inline NULL instead of its bitmap")
            row.append(value.decode("utf-8", "strict"))
        if offset != len(packet):
            raise GateError(f"binary row has trailing bytes: {packet.hex()}")
        rows.append(row)


def read_text_rows(
    connection: Any, first: bytes, protocol: ModuleType
) -> list[list[str | None]]:
    columns, sequence = read_columns(connection, first, protocol)
    rows: list[list[str | None]] = []
    while True:
        packet = connection.read_packet(sequence)
        sequence += 1
        if is_result_terminator(packet):
            protocol.assert_eof(packet, connection.deprecate_eof)
            return rows
        offset = 0
        row: list[str | None] = []
        for _column in columns:
            value, offset = protocol.read_lenenc_bytes(packet, offset)
            row.append(None if value is None else value.decode("utf-8", "strict"))
        if offset != len(packet):
            raise GateError(f"text row has trailing bytes: {packet.hex()}")
        rows.append(row)


def raise_mysql_error(payload: bytes, protocol: ModuleType, operation: str) -> None:
    error = protocol.parse_error(payload)
    raise GateError(f"{operation}: MySQL {error.code}/{error.state}: {error.message}")


def execute_text_query(connection: Any, sql: str, protocol: ModuleType) -> list[list[str | None]]:
    connection.write_packet(bytes((COM_QUERY,)) + sql.encode("utf-8"), 0)
    first = connection.read_packet(1)
    if first and first[0] == 0xFF:
        raise_mysql_error(first, protocol, sql)
    if first and first[0] == 0x00:
        return []
    return read_text_rows(connection, first, protocol)


def select_database(connection: Any, database: str, protocol: ModuleType) -> None:
    if not DATABASE_NAME.fullmatch(database):
        raise GateError(f"unsafe database name: {database!r}")
    execute_text_query(connection, f"USE `{database}`", protocol)


def collect_plan(
    endpoint: Endpoint,
    database: str,
    case: dict[str, Any],
    protocol: ModuleType,
) -> list[list[str | None]]:
    connection_type = protocol.MysqlConnection
    with connection_type(
        endpoint.host, endpoint.port, endpoint.user, endpoint.password
    ) as connection:
        select_database(connection, database, protocol)
        explain_sql = "EXPLAIN FORMAT='brief' " + case["sql"]
        if case["protocol"] == "direct":
            return execute_text_query(connection, explain_sql, protocol)

        parameters = parse_parameters(case["params"])
        prepared = connection.prepare(explain_sql)
        if isinstance(prepared, protocol.MysqlError):
            raise GateError(
                f"{endpoint.name} prepare {case['id']}: MySQL {prepared.code}/"
                f"{prepared.state}: {prepared.message}"
            )
        if len(prepared.parameters) != len(parameters):
            raise GateError(
                f"{endpoint.name} prepare {case['id']} advertised "
                f"{len(prepared.parameters)} parameters, manifest has {len(parameters)}"
            )
        payload = build_execute_payload(prepared.statement_id, parameters)
        connection.write_packet(bytes((COM_STMT_EXECUTE,)) + payload, 0)
        first = connection.read_packet(1)
        try:
            if first and first[0] == 0xFF:
                raise_mysql_error(first, protocol, f"{endpoint.name} execute {case['id']}")
            return read_binary_string_rows(connection, first, protocol)
        finally:
            connection.close_statement(prepared.statement_id)


def normalize_cell(value: str | None, column: int) -> str | None:
    if value is None:
        return None
    normalized = value.replace("\r\n", "\n").rstrip()
    normalized = COLUMN_ORDINAL.sub("Column#?", normalized)
    if column == 0:
        normalized = OPERATOR_ID.sub("", normalized)
    return normalized


def normalize_plan(rows: list[list[str | None]]) -> list[list[str | None]]:
    return [
        [normalize_cell(value, column) for column, value in enumerate(row)]
        for row in rows
    ]


def plan_lines(rows: list[list[str | None]]) -> list[str]:
    return ["\t".join("NULL" if value is None else value for value in row) for row in rows]


def plan_diff(go_rows: list[list[str | None]], rust_rows: list[list[str | None]]) -> list[str]:
    return list(
        difflib.unified_diff(
            plan_lines(go_rows),
            plan_lines(rust_rows),
            fromfile="go-tidb",
            tofile="rust-tidb",
            lineterm="",
        )
    )


def git_revision(path: Path) -> str:
    result = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_source_files(
    manifest: dict[str, Any], source_roots: dict[str, Path]
) -> dict[str, dict[str, str]]:
    inventories = manifest.get("source_files")
    if not isinstance(inventories, dict) or set(inventories) != set(source_roots):
        raise GateError("manifest source_files must match all configured source roots")
    actual: dict[str, dict[str, str]] = {}
    for name, root in source_roots.items():
        expected_files = inventories[name]
        if not isinstance(expected_files, dict) or not expected_files:
            raise GateError(f"{name} source inventory is empty")
        actual[name] = {}
        for relative, expected in expected_files.items():
            path = root / relative
            digest = sha256_file(path)
            actual[name][relative] = digest
            if digest != expected:
                raise GateError(
                    f"{name} source digest mismatch for {relative}: {digest} != {expected}"
                )
    return actual


def load_manifest(path: Path) -> dict[str, Any]:
    try:
        manifest = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise GateError(f"cannot read manifest {path}: {error}") from error
    if manifest.get("schema_version") != "1.0":
        raise GateError("unsupported plan manifest schema_version")
    if manifest.get("coverage_status") not in {"incomplete", "complete"}:
        raise GateError("manifest coverage_status must be incomplete or complete")
    cases = manifest.get("cases")
    if not isinstance(cases, list) or not cases:
        raise GateError("manifest must contain at least one case")
    identities: set[str] = set()
    for case in cases:
        required = {"id", "suite", "phase", "kind", "protocol", "source", "sql"}
        if not isinstance(case, dict) or not required.issubset(case):
            raise GateError(f"manifest case omits required fields: {case!r}")
        identity = case["id"]
        if not isinstance(identity, str) or not identity or identity in identities:
            raise GateError(f"invalid or duplicate case id: {identity!r}")
        identities.add(identity)
        if case["kind"] == "plan":
            if case["protocol"] not in {"direct", "prepared"}:
                raise GateError(f"{identity}: invalid plan protocol")
            if case["protocol"] == "prepared":
                parse_parameters(case.get("params"))
            elif case.get("params") not in (None, []):
                raise GateError(f"{identity}: direct plan case cannot have params")
        elif case["kind"] == "non_plan":
            if case["protocol"] not in {"direct", "prepared"}:
                raise GateError(f"{identity}: invalid non-plan protocol")
        else:
            raise GateError(f"{identity}: kind must be plan or non_plan")
    families = manifest.get("plan_families", [])
    if not isinstance(families, list):
        raise GateError("manifest plan_families must be a list")
    for family in families:
        required = {
            "id",
            "suite",
            "phase",
            "kind",
            "protocol",
            "source",
            "generator",
            "sql_prefix",
            "row_template",
            "row_counts",
        }
        if not isinstance(family, dict) or not required.issubset(family):
            raise GateError(f"plan family omits required fields: {family!r}")
        identity = family["id"]
        if not isinstance(identity, str) or not identity or identity in identities:
            raise GateError(f"invalid or duplicate plan family id: {identity!r}")
        identities.add(identity)
        if (
            family["kind"] != "plan"
            or family["protocol"] != "direct"
            or family["generator"] != "multi_row_insert"
        ):
            raise GateError(f"{identity}: unsupported plan family contract")
        family_row_counts(family)
        render_family_row(family, 1)
    expected_static = manifest.get("static_case_count")
    expected_families = manifest.get("plan_family_count")
    expected_expanded = manifest.get("expanded_case_count")
    actual_expanded = len(cases) + sum(
        len(family_row_counts(family)) for family in families
    )
    if expected_static != len(cases):
        raise GateError(
            f"manifest static_case_count mismatch: {expected_static} != {len(cases)}"
        )
    if expected_families != len(families):
        raise GateError(
            "manifest plan_family_count mismatch: "
            f"{expected_families} != {len(families)}"
        )
    if expected_expanded != actual_expanded:
        raise GateError(
            "manifest expanded_case_count mismatch: "
            f"{expected_expanded} != {actual_expanded}"
        )
    return manifest


def family_row_counts(family: dict[str, Any]) -> list[int]:
    raw = family["row_counts"]
    if isinstance(raw, list):
        counts = raw
    elif isinstance(raw, dict) and set(raw) == {"min", "max"}:
        minimum = raw["min"]
        maximum = raw["max"]
        if not isinstance(minimum, int) or not isinstance(maximum, int):
            raise GateError(f"{family['id']}: row-count range must contain integers")
        counts = list(range(minimum, maximum + 1))
    else:
        raise GateError(f"{family['id']}: invalid row_counts")
    if (
        not counts
        or any(isinstance(count, bool) or not isinstance(count, int) for count in counts)
        or counts != sorted(set(counts))
        or counts[0] < 1
        or counts[-1] > 4096
    ):
        raise GateError(f"{family['id']}: row counts must be unique sorted values in 1..4096")
    return counts


def render_family_row(family: dict[str, Any], row: int) -> str:
    template = family["row_template"]
    if not isinstance(template, str) or not template:
        raise GateError(f"{family['id']}: row_template must be a non-empty string")
    try:
        rendered = template.format(row=row)
    except (IndexError, KeyError, ValueError) as error:
        raise GateError(f"{family['id']}: invalid row_template: {error}") from error
    if not MULTI_ROW_INSERT_ROW.fullmatch(rendered):
        raise GateError(
            f"{family['id']}: rendered row must be parenthesized, with an optional trailing comment"
        )
    return rendered


def expand_plan_families(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    cases = list(manifest["cases"])
    identities = {case["id"] for case in cases}
    for family in manifest.get("plan_families", []):
        separator = family.get("separator", ",")
        if separator not in {",", ", "}:
            raise GateError(f"{family['id']}: unsupported row separator")
        rows: list[str] = []
        next_count_index = 0
        counts = family_row_counts(family)
        for row in range(1, counts[-1] + 1):
            rows.append(render_family_row(family, row))
            if row != counts[next_count_index]:
                continue
            identity = f"{family['id']}.rows_{row}"
            if identity in identities:
                raise GateError(f"duplicate expanded case id: {identity}")
            identities.add(identity)
            case = {
                "id": identity,
                "family": family["id"],
                "suite": family["suite"],
                "phase": family["phase"],
                "kind": "plan",
                "protocol": "direct",
                "source": family["source"],
                "sql": family["sql_prefix"] + separator.join(rows),
            }
            if "workloads" in family:
                case["workloads"] = family["workloads"]
            cases.append(case)
            next_count_index += 1
            if next_count_index == len(counts):
                break
    return cases


def resolve_database(case: dict[str, Any], args: argparse.Namespace) -> str:
    suite = case["suite"]
    if suite == "tpcc":
        return args.tpcc_database
    if suite == "sysbench":
        return args.sysbench_database
    raise GateError(f"{case['id']}: unsupported suite {suite!r}")


def write_markdown(report: dict[str, Any], path: Path) -> None:
    lines = [
        "# TPCC/Sysbench physical-plan parity",
        "",
        f"Manifest coverage: `{report['coverage_status']}`.",
        "",
        f"Matched: **{report['matched']}**. Mismatched: **{report['mismatched']}**. "
        f"Errors: **{report['errors']}**.",
        "",
    ]
    for case in report["cases"]:
        lines.extend([f"## `{case['id']}`", "", f"Status: `{case['status']}`.", ""])
        if case.get("diff"):
            lines.extend(["```diff", *case["diff"], "```", ""])
        if case.get("error"):
            lines.extend(["```text", case["error"], "```", ""])
    path.write_text("\n".join(lines) + "\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--go-tpc-root", type=Path, required=True)
    parser.add_argument("--sysbench-root", type=Path, required=True)
    parser.add_argument("--sysbench-contract-root", type=Path, required=True)
    parser.add_argument("--go-host", default="127.0.0.1")
    parser.add_argument("--go-port", type=int, default=4100)
    parser.add_argument("--rust-host", default="127.0.0.1")
    parser.add_argument("--rust-port", type=int, default=4000)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--tpcc-database", required=True)
    parser.add_argument("--sysbench-database", default="sysbench_plan_parity")
    parser.add_argument("--case", action="append", dest="case_ids")
    parser.add_argument("--suite", action="append", choices=("tpcc", "sysbench"))
    parser.add_argument(
        "--phase", action="append", choices=("prepare", "run", "check", "cleanup")
    )
    parser.add_argument("--allow-incomplete-manifest", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        manifest = load_manifest(args.manifest.resolve())
        pins = manifest["source_pins"]
        revisions = {
            "go_tpc": git_revision(args.go_tpc_root.resolve()),
            "sysbench": git_revision(args.sysbench_root.resolve()),
        }
        for name, revision in revisions.items():
            expected = pins[name]["commit"]
            if revision != expected:
                raise GateError(f"{name} revision mismatch: {revision} != {expected}")
        source_files = validate_source_files(
            manifest,
            {
                "go_tpc": args.go_tpc_root.resolve(),
                "sysbench": args.sysbench_root.resolve(),
                "sysbench_contract": args.sysbench_contract_root.resolve(),
            },
        )
        if manifest["coverage_status"] != "complete" and not args.allow_incomplete_manifest:
            raise GateError(
                "manifest coverage is incomplete; formal parity evidence is blocked"
            )

        expanded_cases = expand_plan_families(manifest)
        selected = set(args.case_ids or [])
        known = {case["id"] for case in expanded_cases}
        unknown = selected - known
        if unknown:
            raise GateError(f"unknown case ids: {', '.join(sorted(unknown))}")
        selected_suites = set(args.suite or [])
        selected_phases = set(args.phase or [])
        cases = [
            case
            for case in expanded_cases
            if case["kind"] == "plan"
            and (not selected or case["id"] in selected)
            and (not selected_suites or case["suite"] in selected_suites)
            and (not selected_phases or case["phase"] in selected_phases)
        ]
        if not cases:
            raise GateError("selection contains no plan cases")

        protocol = load_protocol_client()
        go_endpoint = Endpoint("go", args.go_host, args.go_port, args.user, args.password)
        rust_endpoint = Endpoint(
            "rust", args.rust_host, args.rust_port, args.user, args.password
        )
        results: list[dict[str, Any]] = []
        for case in cases:
            result: dict[str, Any] = {
                "id": case["id"],
                "source": case["source"],
                "sql_sha256": hashlib.sha256(case["sql"].encode()).hexdigest(),
            }
            try:
                database = resolve_database(case, args)
                go_raw = collect_plan(go_endpoint, database, case, protocol)
                rust_raw = collect_plan(rust_endpoint, database, case, protocol)
                go_normalized = normalize_plan(go_raw)
                rust_normalized = normalize_plan(rust_raw)
                result.update(
                    {
                        "database": database,
                        "go_raw": go_raw,
                        "rust_raw": rust_raw,
                        "go_normalized": go_normalized,
                        "rust_normalized": rust_normalized,
                    }
                )
                if go_normalized == rust_normalized:
                    result["status"] = "matched"
                    result["diff"] = []
                else:
                    result["status"] = "mismatched"
                    result["diff"] = plan_diff(go_normalized, rust_normalized)
            except (GateError, OSError) as error:
                result["status"] = "error"
                result["error"] = str(error)
            results.append(result)

        report = {
            "schema_version": "1.0",
            "coverage_status": manifest["coverage_status"],
            "source_revisions": revisions,
            "source_files": source_files,
            "normalization": ["operator numeric suffix", "Column#N ordinal"],
            "expanded_manifest_case_count": len(expanded_cases),
            "matched": sum(case["status"] == "matched" for case in results),
            "mismatched": sum(case["status"] == "mismatched" for case in results),
            "errors": sum(case["status"] == "error" for case in results),
            "cases": results,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        write_markdown(report, args.output.with_suffix(".md"))
        print(json.dumps({key: report[key] for key in ("matched", "mismatched", "errors")}))
        return 0 if report["mismatched"] == 0 and report["errors"] == 0 else 1
    except (GateError, OSError, subprocess.CalledProcessError) as error:
        print(f"plan parity gate error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
