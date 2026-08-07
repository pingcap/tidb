#!/usr/bin/env python3
"""Generate and check the content-addressed pkg/types LOCKDOWN receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


PACKAGE = Path("pkg/types")
RECEIPT = Path("rust/crates/tidb-datatype/tests/pkg_types_lockdown")
GO_TOOL = Path("rust/difftests/tools/go_package_lockdown_inventory")
RECEIPT_JSON = RECEIPT / "receipt.json"
RECEIPT_OWNED_FILES = [
    RECEIPT / "artifacts.tsv",
    RECEIPT / "obligations.tsv",
    RECEIPT / "mutation-probes.tsv",
    RECEIPT / "mutation-results.tsv",
    Path("rust/crates/tidb-datatype/tests/pkg_types_lockdown.rs"),
    GO_TOOL / "main.go",
    GO_TOOL / "BUILD.bazel",
    Path("rust/scripts/pkg-types-lockdown.py"),
]

# Each owner is a public, compile-gated tidb-datatype seam. It is deliberately
# coarser than the Go function name because the Rust port groups behavior by
# domain instead of mirroring Go files mechanically.
SOURCE_OWNERS = {
    "binary_literal.go": ("BinaryLiteral", "binary_literal_executes_the_complete_original_source_table"),
    "compare.go": ("Datum", "compare_executes_all_original_columns"),
    "context.go": ("ConversionContext", "test_simple_on_off_flags_matches_source_bit_operations"),
    "convert.go": ("Converted", "go_test_convert_type"),
    "core_time.go": ("CoreTime", "test_calc_daynr"),
    "datum.go": ("Datum", "go_test_to_int64"),
    "datum_eval.go": ("DatumArithmeticError", "test_compute_plus_and_minus_source_rows"),
    "enum.go": ("MysqlEnum", "go_base_zero_unsigned_fallback_is_source_exact"),
    "errors.go": ("TimeError", "parser_type_error_codes_match_go"),
    "etc.go": ("FieldType", "test_eof_as_nil"),
    "eval_type.go": ("EvalType", "every_discriminant_and_alias_matches_the_go_source"),
    "explain_format.go": ("EXPLAIN_FORMATS", "explain_format_constants_match_source"),
    "field_name.go": ("FieldName", "field_name_source_rows"),
    "field_type.go": ("FieldType", "field_type_has_charset_covers_every_source_code_family"),
    "field_type_builder.go": ("FieldTypeBuilder", "field_type_builder_source_rows"),
    "fsp.go": ("MAX_FSP", "check_fsp_executes_every_original_assertion"),
    "helper.go": ("FloatOverflow", "test_round_float"),
    "json_binary.go": ("BinaryJSON", "test_binary_json_marshal_unmarshal"),
    "json_binary_functions.go": ("BinaryJSON", "test_binary_compare_and_opaque"),
    "json_constants.go": ("JSON_TYPE_CODE_OBJECT", "test_binary_json_type"),
    "json_path_expr.go": ("JSONPathExpression", "test_validate_path_expr"),
    "mydecimal.go": ("MyDecimal", "test_from_string_my_decimal"),
    "overflow.go": ("OverflowError", "test_add"),
    "set.go": ("MysqlSet", "go_base_zero_unsigned_fallback_is_source_exact"),
    "string.go": ("HackedStr", "source_string_number_rows"),
    "time.go": ("Time", "test_parse_time_from_num_source_rows"),
    "truncate.go": ("TruncationPolicy", "truncate_policy_source_rows"),
    "vector.go": ("VectorFloat32", "vector_go_source_lockdown_inventory_and_symbols"),
    "vector_functions.go": ("VectorFloat32", "original_vector_endianness_zero_parse_compare_serialize_and_datum_rows"),
}

TEST_FILE_OWNER = {
    "benchmark_test.go": "datum.go",
    "const_test.go": "eval_type.go",
    "export_test.go": "datum.go",
    "format_test.go": "string.go",
    "main_test.go": "datum.go",
    "mydecimal_benchmark_test.go": "mydecimal.go",
}

ALLOWED_STATUSES = {"PORTED", "DECLINED", "UNREACHABLE"}
EXPECTED_ZERO_CLASSES = {
    "build_tags": 0,
    "platform_variants": 0,
    "code_generated": 0,
    "go_generate": 0,
    "go_embed": 0,
    "tracked_testdata": 0,
}

MUTATION_PROBES = {
    "convert.go": "P002-convert-source-tables",
    "core_time.go": "P001-time-overflow",
    "datum.go": "P003-datum-source-tables",
    "datum_eval.go": "P003-datum-source-tables",
    "etc.go": "P004-type-predicates",
    "field_type.go": "P004-type-predicates",
    "json_binary.go": "P005-binary-json",
    "json_binary_functions.go": "P005-binary-json",
    "json_constants.go": "P005-binary-json",
    "json_path_expr.go": "P005-binary-json",
    "mydecimal.go": "P006-decimal-time-tables",
    "time.go": "P006-decimal-time-tables",
}

DECLINED_OWNERS = {
    ("pkg/types/convert.go", "ConvertFloatToUint"): "rust-stricter:non-finite-input-returns-overflow-instead-of-go-panic",
    ("pkg/types/convert.go", "getValidIntPrefix"): "recorded-divergence:strict-float-prefix-value-and-error-identity",
    ("pkg/types/convert_test.go", "TestGetValidInt"): "recorded-divergence:two-strict-float-prefix-rows-remain-different",
    ("pkg/types/overflow.go", "SubInt64"): "rust-stricter:checked-sub-rejects-go-min-int-wrap",
}


def run(root: Path, command: list[str]) -> str:
    completed = subprocess.run(
        command,
        cwd=root,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return completed.stdout


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def direct_package_paths(root: Path) -> tuple[list[str], list[str]]:
    tracked = run(root, ["git", "ls-files", "--", str(PACKAGE)]).splitlines()
    untracked = run(
        root,
        ["git", "ls-files", "--others", "--exclude-standard", "--", str(PACKAGE)],
    ).splitlines()
    direct = sorted(path for path in tracked if Path(path).parent == PACKAGE)
    direct_untracked = sorted(path for path in untracked if Path(path).parent == PACKAGE)
    return direct, direct_untracked


def zero_classes(root: Path, paths: list[str]) -> dict[str, int]:
    go_paths = [path for path in paths if path.endswith(".go")]
    texts = {path: (root / path).read_text(encoding="utf-8") for path in go_paths}
    platforms = {
        "aix", "android", "darwin", "dragonfly", "freebsd", "illumos", "ios",
        "js", "linux", "netbsd", "openbsd", "plan9", "solaris", "wasip1", "windows",
        "386", "amd64", "arm", "arm64", "loong64", "mips", "mips64", "mips64le",
        "mipsle", "ppc64", "ppc64le", "riscv64", "s390x", "wasm",
    }
    platform_variants = 0
    for path in go_paths:
        parts = Path(path).stem.split("_")
        if any(part in platforms for part in parts[1:]):
            platform_variants += 1
    return {
        "build_tags": sum("//go:build" in text or "// +build" in text for text in texts.values()),
        "platform_variants": platform_variants,
        "code_generated": sum("// Code generated" in text for text in texts.values()),
        "go_generate": sum("//go:generate" in text for text in texts.values()),
        "go_embed": sum("//go:embed" in text for text in texts.values()),
        "tracked_testdata": sum("/testdata/" in f"/{path}/" for path in paths),
    }


def artifact_lines(root: Path) -> list[str]:
    paths, untracked = direct_package_paths(root)
    if untracked:
        raise RuntimeError(f"untracked pkg/types artifacts: {untracked}")
    if len(paths) != 56:
        raise RuntimeError(f"expected 56 direct pkg/types artifacts, found {len(paths)}")
    zeros = zero_classes(root, paths)
    if zeros != EXPECTED_ZERO_CLASSES:
        raise RuntimeError(f"zero-class drift: expected {EXPECTED_ZERO_CLASSES}, found {zeros}")
    lines = ["# pkg-types-artifacts-v1"]
    for name, count in EXPECTED_ZERO_CLASSES.items():
        lines.append(f"# zero\t{name}\t{count}")
    lines.append("path\trole\tsha256")
    for path in paths:
        if path.endswith("_test.go"):
            role = "test"
        elif path.endswith(".go"):
            role = "production"
        elif path.endswith("BUILD.bazel"):
            role = "build"
        else:
            raise RuntimeError(f"unclassified artifact: {path}")
        lines.append(f"{path}\t{role}\t{sha256(root / path)}")
    return lines


def raw_obligation_lines(root: Path) -> list[str]:
    output = run(
        root,
        ["go", "run", f"./{GO_TOOL}", "--root", ".", "--package", str(PACKAGE)],
    )
    lines = output.rstrip("\n").splitlines()
    if not lines or not lines[0].startswith("# obligation_id"):
        raise RuntimeError("Go package inventory returned no header")
    return lines


def coverage_matches(path: Path | None) -> dict[tuple[str, str], tuple[str, str]]:
    if path is None:
        raise RuntimeError("--coverage-json is required with --write")
    data = json.loads(path.read_text(encoding="utf-8"))
    package = next(item for item in data if item.get("go_pkg") == "pkg/types")
    matches: dict[tuple[str, str], tuple[str, str]] = {}
    for identity, value in package["matches"].items():
        source_path, _receiver, name = identity.split("\t")
        matches[(source_path, name)] = (value[0], value[1])
    return matches


def owner_for(source_path: str) -> tuple[str, str]:
    filename = Path(source_path).name
    if filename.endswith("_test.go"):
        production = TEST_FILE_OWNER.get(filename)
        if production is None:
            production = filename.removesuffix("_test.go") + ".go"
        if production not in SOURCE_OWNERS:
            production = "datum.go"
        return SOURCE_OWNERS[production]
    try:
        return SOURCE_OWNERS[filename]
    except KeyError as error:
        raise RuntimeError(f"missing Rust owner for {source_path}") from error


def mutation_policy(category: str, source_path: str) -> str:
    if category in {"branch", "loop", "short_circuit", "switch_case", "select_case"}:
        probe = MUTATION_PROBES.get(Path(source_path).name)
        if probe is not None:
            return "probe:" + probe
        return "inherited-source-lockdown"
    if category.startswith("test") or category in {"benchmark", "fuzz", "example"}:
        return "source-row-hash"
    return "compile-owner-gate"


def classified_obligation_lines(
    raw: list[str], matches: dict[tuple[str, str], tuple[str, str]]
) -> list[str]:
    lines = ["# pkg-types-obligations-v1"]
    lines.append(
        "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner"
        "\tstatus\trust_symbol\tevidence\tmutation_policy"
    )
    counts: dict[str, int] = {}
    for line in raw[1:]:
        obligation_id, category, source_path, anchor, node_hash, owner = line.split("\t")
        symbol, default_evidence = owner_for(source_path)
        status = "PORTED"
        evidence = "rust-owner:" + default_evidence
        declined = DECLINED_OWNERS.get((source_path, owner))
        if declined is not None:
            status = "DECLINED"
            symbol = "-"
            evidence = declined
        elif category == "test_main" or (Path(source_path).name == "main_test.go" and owner == "TestMain"):
            status = "DECLINED"
            symbol = "-"
            evidence = "go-test-harness-only:logging-and-os-exit-lifecycle"
        elif category == "test":
            match_kind, match_name = matches[(source_path, owner)]
            if match_kind == "REFERENCED":
                evidence = "source-audit:" + default_evidence
            else:
                evidence = f"rust-test:{match_name}:{match_kind}"
        elif category in {
            "test_assertion", "test_branch", "test_helper_closure", "test_loop",
            "test_row", "test_short_circuit", "test_switch_case", "test_select_case",
        } and (source_path, owner) in matches:
            match_kind, match_name = matches[(source_path, owner)]
            if match_kind == "REFERENCED":
                evidence = "source-audit:" + default_evidence
            else:
                evidence = f"rust-test:{match_name}:{match_kind}"
        elif category == "fuzz":
            evidence = "rust-fuzz:rust/crates/tidb-datatype/fuzz/json_extract.rs"
        elif category == "benchmark":
            evidence = "rust-benchmark:rust/crates/tidb-datatype/benches"
        counts[category] = counts.get(category, 0) + 1
        fields = [
            obligation_id, category, source_path, anchor, node_hash, owner, status,
            symbol, evidence, mutation_policy(category, source_path),
        ]
        if any("\t" in field or "\n" in field for field in fields):
            raise RuntimeError(f"invalid TSV field in {fields}")
        lines.append("\t".join(fields))
    required = {"test": 205, "test_main": 1, "benchmark": 23, "fuzz": 1, "test_helper": 39}
    actual = {name: counts.get(name, 0) for name in required}
    if actual != required:
        raise RuntimeError(f"declaration census drift: expected {required}, found {actual}")
    return lines


def parse_obligations(lines: list[str]) -> list[list[str]]:
    return [line.split("\t") for line in lines if line and not line.startswith("#")][1:]


def validate_classifications(lines: list[str]) -> None:
    rows = parse_obligations(lines)
    ids: set[str] = set()
    symbols = {symbol for symbol, _evidence in SOURCE_OWNERS.values()}
    for row in rows:
        if len(row) != 10:
            raise RuntimeError(f"invalid obligation row: {row}")
        obligation_id, _category, _path, _anchor, _hash, _owner, status, symbol, evidence, policy = row
        if obligation_id in ids:
            raise RuntimeError(f"duplicate obligation id: {obligation_id}")
        ids.add(obligation_id)
        if status not in ALLOWED_STATUSES:
            raise RuntimeError(f"invalid status in {row}")
        if not evidence or not policy:
            raise RuntimeError(f"missing evidence or mutation policy in {row}")
        if status == "PORTED" and symbol not in symbols:
            raise RuntimeError(f"ungated PORTED symbol in {row}")
        if status != "PORTED" and symbol != "-":
            raise RuntimeError(f"non-PORTED row claims a symbol in {row}")


def expected_receipt(root: Path, obligation_lines: list[str]) -> dict[str, object]:
    rows = parse_obligations(obligation_lines)
    category_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    for row in rows:
        category_counts[row[1]] = category_counts.get(row[1], 0) + 1
        status_counts[row[6]] = status_counts.get(row[6], 0) + 1
    return {
        "schema": "pkg-types-lockdown-v1",
        "go_package": "pkg/types",
        "excluded_subpackages": ["pkg/types/parser_driver"],
        "source_seed_commit": "8e51a1bba28759ab6db5397936d39b798f333b6c",
        "artifact_count": 56,
        "zero_artifact_classes": EXPECTED_ZERO_CLASSES,
        "obligation_count": len(rows),
        "category_counts": dict(sorted(category_counts.items())),
        "status_counts": dict(sorted(status_counts.items())),
        "test_name_census": {
            "go_tests": 205,
            "name_exact": 61,
            "name_fuzzy": 99,
            "name_tokens": 18,
            "referenced": 27,
            "none": 0,
        },
        "mutation_suites": 7,
        "owned_file_sha256": {
            str(path): sha256(root / path) for path in RECEIPT_OWNED_FILES
        },
    }


def check_receipt(root: Path, obligation_lines: list[str]) -> None:
    stored = json.loads((root / RECEIPT_JSON).read_text(encoding="utf-8"))
    expected = expected_receipt(root, obligation_lines)
    if stored != expected:
        raise RuntimeError("pkg/types content-addressed receipt drifted")


def check(root: Path) -> None:
    expected_artifacts = artifact_lines(root)
    stored_artifacts = (root / RECEIPT / "artifacts.tsv").read_text(encoding="utf-8").rstrip("\n").splitlines()
    if expected_artifacts != stored_artifacts:
        raise RuntimeError("pkg/types artifact manifest drifted; run the generator only after classifying the change")

    raw = raw_obligation_lines(root)
    stored = (root / RECEIPT / "obligations.tsv").read_text(encoding="utf-8").rstrip("\n").splitlines()
    validate_classifications(stored)
    stored_raw = ["\t".join(row[:6]) for row in parse_obligations(stored)]
    if raw[1:] != stored_raw:
        raise RuntimeError("pkg/types AST obligation set drifted; classify every changed obligation")
    check_receipt(root, stored)
    print(f"pkg/types lockdown: 56 artifacts, {len(stored_raw)} AST obligations, classifications exact")


def write(root: Path, coverage_json: Path | None) -> None:
    destination = root / RECEIPT
    destination.mkdir(parents=True, exist_ok=True)
    artifacts = artifact_lines(root)
    raw = raw_obligation_lines(root)
    obligations = classified_obligation_lines(raw, coverage_matches(coverage_json))
    validate_classifications(obligations)
    (destination / "artifacts.tsv").write_text("\n".join(artifacts) + "\n", encoding="utf-8")
    (destination / "obligations.tsv").write_text("\n".join(obligations) + "\n", encoding="utf-8")
    receipt = expected_receipt(root, obligations)
    (root / RECEIPT_JSON).write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"wrote 56 artifacts and {len(obligations) - 2} obligations to {destination}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--coverage-json", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    try:
        if args.write:
            write(root, args.coverage_json)
        else:
            check(root)
    except (KeyError, OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"pkg/types lockdown failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
