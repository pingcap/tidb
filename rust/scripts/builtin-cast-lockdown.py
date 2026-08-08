#!/usr/bin/env python3
"""Generate/check the exact source-owned builtin_cast.go lockdown ledger."""

from __future__ import annotations

import argparse
import collections
import hashlib
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


FILES = {
    "pkg/expression/builtin_cast.go": (104638, 3004, "12741863129c46a008a9064f94e11bf8be0a20f0b4efd83ef0a9e6b40b731ab5"),
    "pkg/expression/builtin_cast_test.go": (76362, 1841, "caa0b678d597d60452e2dda8c6a84f55cd21d6f2920f17661e85ae3d055ad6d7"),
    "pkg/expression/builtin_cast_bench_test.go": (2083, 66, "bfeea12e25ddfc8ae854f2670bb9e44d16e82cbe8167923723153c19a933b7ac"),
}
EXPECTED = {
    "benchmark": 2, "branch": 518, "closure": 7, "const": 6,
    "declaration": 64, "field": 77, "function": 151, "loop": 10,
    "short_circuit": 224, "switch_case": 146, "test": 14,
    "test_assertion": 174, "test_branch": 20, "test_helper": 1,
    "test_helper_closure": 5, "test_loop": 58, "test_row": 1552,
    "test_support_var": 22, "test_switch_case": 74, "var": 62,
}
LEDGER = Path("rust/crates/tidb-expr/src/builtin_cast.inventory.tsv")
ANCHORS = Path("rust/crates/tidb-expr/src/builtin_cast_lockdown.rs")
MUTATIONS = Path("rust/crates/tidb-expr/src/builtin_cast.mutations.tsv")
GO_TOOL = Path("rust/difftests/tools/go_package_lockdown_inventory")
SYMBOLS = {
    "CAST_EVAL": "builtin_cast_lockdown::CAST_EVAL",
    "CAST_REWRITE": "builtin_cast_lockdown::CAST_REWRITE",
    "CAST_JSON": "builtin_cast_lockdown::CAST_JSON",
}

UNION_OWNERS = {
    f"builtinCast{source}As{target}Sig.eval{target}"
    for source in ("Int", "Real", "Decimal", "String")
    for target in ("Int", "Real", "Decimal")
}
DECLINED_FUNCTIONS = {
    "BuildCastCollationFunction": "D_COLLATION",
    "BuildCastFunction4Union": "D_UNION",
    "BuildCastFunctionWithCheck": "D_UNION",
    "CanImplicitEvalInt": "D_IMPLICIT_EVAL",
    "CanImplicitEvalReal": "D_IMPLICIT_EVAL",
    "TryPushCastIntoControlFunctionForHybridType": "D_CONTROL_PUSH",
    "adjustRetFtForCastString": "D_METADATA",
    "decimalPrecisionToLength": "D_METADATA",
    "floatLength": "D_METADATA",
    "minimalDecimalLenForHoldingInteger": "D_METADATA",
    "newFakeSctx": "D_GO_TEST_HOOK",
    "setDataTypeDouble": "D_METADATA",
}
PORTED_REWRITE = {
    "BuildCastFunction", "WrapWithCastAsDecimal",
    "WrapWithCastAsInt", "WrapWithCastAsJSON", "WrapWithCastAsReal",
    "WrapWithCastAsString", "WrapWithCastAsTime",
}
PORTED_JSON = {"ConvertJSON2Tp", "convertJSON2Tp"}

TEST_STATUS = {
    "TestCastFunctions": ("DECLINED", "-", "D_SIGNATURE_MATRIX"),
    "TestCastFuncSig": ("DECLINED", "-", "D_SIGNATURE_MATRIX"),
    "TestCastJSONAsDecimalSig": ("PORTED", "CAST_EVAL", "P_JSON_SOURCE"),
    "TestWrapWithCastAsTypesClasses": ("DECLINED", "-", "D_METADATA"),
    "TestWrapWithCastAsTime": ("PORTED", "CAST_REWRITE", "P_TEMPORAL_WRAP"),
    "TestWrapWithCastAsDuration": ("DECLINED", "-", "D_DURATION_ERROR_CONTEXT"),
    "TestWrapWithCastAsString": ("PORTED", "CAST_REWRITE", "P_STRING_WRAP"),
    "TestWrapWithCastAsJSON": ("PORTED", "CAST_JSON", "P_JSON_WRAP"),
    "TestCastIntAsIntVec": ("DECLINED", "-", "D_VECTOR_ENGINE"),
    "TestCastStringAsDecimalSigWithUnsignedFlagInUnion": ("DECLINED", "-", "D_UNION"),
    "TestCastConstAsDecimalFieldType": ("DECLINED", "-", "D_METADATA"),
    "TestCastBinaryStringAsJSONSig": ("PORTED", "CAST_JSON", "P_BINARY_JSON"),
    "TestCastArrayFunc": ("DECLINED", "-", "D_ARRAY"),
    "TestCastAsCharFieldType": ("DECLINED", "-", "D_METADATA"),
    "BenchmarkCastIntAsIntRow": ("DECLINED", "-", "D_GO_BENCHMARK"),
    "BenchmarkCastIntAsIntVec": ("DECLINED", "-", "D_VECTOR_ENGINE"),
    "genCastIntAsInt": ("DECLINED", "-", "D_GO_BENCHMARK"),
}


def run(root: Path, args: list[str]) -> str:
    return subprocess.run(args, cwd=root, check=True, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout


def check_sources(root: Path) -> None:
    for rel, (size, lines, digest) in FILES.items():
        data = (root / rel).read_bytes()
        got = (len(data), data.count(b"\n"), hashlib.sha256(data).hexdigest())
        if got != (size, lines, digest):
            raise RuntimeError(f"source drift for {rel}: {got}")


def raw_rows(root: Path) -> list[list[str]]:
    with tempfile.TemporaryDirectory(prefix="builtin-cast-lockdown-") as tmp:
        isolated = Path(tmp)
        package = isolated / "pkg/expression"
        package.mkdir(parents=True)
        for rel in FILES:
            shutil.copy2(root / rel, package / Path(rel).name)
        output = run(root, ["go", "run", f"./{GO_TOOL}", "--root", str(isolated),
                            "--package", "pkg/expression"])
    lines = output.rstrip().splitlines()
    if not lines or not lines[0].startswith("# obligation_id"):
        raise RuntimeError("isolated Go AST inventory returned no header")
    rows = [line.split("\t") for line in lines[1:]]
    if any(len(row) != 6 for row in rows):
        raise RuntimeError("malformed AST inventory")
    counts = collections.Counter(row[1] for row in rows)
    if dict(sorted(counts.items())) != EXPECTED:
        raise RuntimeError(f"AST census drift: {dict(sorted(counts.items()))}")
    if len(rows) != 3187 or len({row[0] for row in rows}) != 3187:
        raise RuntimeError("obligation count/identity drift")
    return rows


def classify(source: str, category: str, owner: str) -> tuple[str, str, str]:
    if source.endswith("_test.go"):
        if owner.startswith("var:"):
            return "PORTED", SYMBOLS["CAST_EVAL"], "P_GO_CAST_ROWS"
        try:
            status, symbol, evidence = TEST_STATUS[owner]
        except KeyError as error:
            raise RuntimeError(f"unclassified direct test/support owner: {owner}") from error
        return status, SYMBOLS.get(symbol, symbol), evidence

    if owner.startswith("type:") or category in {"declaration", "field", "var"}:
        return "UNREACHABLE", "-", "U_SIGNATURE_OBJECT"
    if owner.endswith(".Clone"):
        return "UNREACHABLE", "-", "U_SIGNATURE_OBJECT"
    if "castJSONAsArray" in owner or "castAsArray" in owner:
        return "DECLINED", "-", "D_ARRAY"
    if "VectorFloat32" in owner:
        if "VectorFloat32AsStringSig.evalString" in owner or "VectorFloat32AsUnsupportedSig.eval" in owner:
            return "PORTED", SYMBOLS["CAST_EVAL"], "P_VECTOR_SOURCE_BOUNDARY"
        return "UNREACHABLE", "-", "U_VECTOR_CAST_TARGET"
    if owner in UNION_OWNERS:
        return "DECLINED", "-", "D_UNION"
    if "AsDurationSig.evalDuration" in owner:
        return "DECLINED", "-", "D_DURATION_ERROR_CONTEXT"
    if "AsTimeSig.evalTime" in owner:
        return "DECLINED", "-", "D_TEMPORAL_RESULT_DOMAIN"
    if owner == "castAsDurationFunctionClass.getFunction" or owner == "WrapWithCastAsDuration":
        return "DECLINED", "-", "D_DURATION_ERROR_CONTEXT"
    if owner in DECLINED_FUNCTIONS:
        return "DECLINED", "-", DECLINED_FUNCTIONS[owner]
    if owner in PORTED_REWRITE or owner.endswith("FunctionClass.getFunction"):
        return "PORTED", SYMBOLS["CAST_REWRITE"], "P_CAST_REWRITE"
    if owner in PORTED_JSON:
        return "PORTED", SYMBOLS["CAST_JSON"], "P_NATIVE_JSON"
    if owner == "padZeroForBinaryType":
        return "PORTED", SYMBOLS["CAST_EVAL"], "P_BINARY_WIDTH"
    if owner.startswith("builtinCast") and (".eval" in owner or ".handle" in owner):
        symbol = "CAST_JSON" if "AsJSONSig.evalJSON" in owner else "CAST_EVAL"
        return "PORTED", SYMBOLS[symbol], "P_CAST_EVAL"
    if category in {"const", "closure"}:
        return "DECLINED", "-", "D_METADATA"
    raise RuntimeError(f"unclassified production obligation: {category} {owner}")


def ledger_lines(rows: list[list[str]]) -> list[str]:
    result = ["# builtin-cast-lockdown-v1",
              "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\tgo_owner\tstatus\trust_symbol\tevidence"]
    statuses = collections.Counter()
    for obligation, category, source, anchor, node_hash, owner in rows:
        status, symbol, evidence = classify(source, category, owner)
        statuses[status] += 1
        result.append("\t".join((obligation, category, source, anchor, node_hash,
                                  owner, status, symbol, evidence)))
    if sum(statuses.values()) != 3187 or set(statuses) != {"PORTED", "DECLINED", "UNREACHABLE"}:
        raise RuntimeError(f"verdict census invalid: {statuses}")
    result.insert(1, "# verdicts " + " ".join(f"{k}={statuses[k]}" for k in ("PORTED", "DECLINED", "UNREACHABLE")))
    return result


def check_symbols(root: Path, lines: list[str]) -> None:
    anchor_text = (root / ANCHORS).read_text()
    declared = {symbol for symbol in SYMBOLS.values() if f'"{symbol}"' in anchor_text}
    used = {line.split("\t")[7] for line in lines if not line.startswith("#") and not line.startswith("obligation_id") and line.split("\t")[6] == "PORTED"}
    if used != declared:
        raise RuntimeError(f"PORTED symbol gate drift: ledger={used}, anchors={declared}")
    for needle in ("crate::cast::eval_cast", "builtin_cast_lockdown_result_type_anchor", "cast_as_json_typed"):
        if needle not in anchor_text:
            raise RuntimeError(f"compile anchor disappeared: {needle}")


def check_mutations(root: Path) -> None:
    rows = (root / MUTATIONS).read_text().rstrip().splitlines()
    if len(rows) != 13 or not rows[0].startswith("probe_id\trule\tmutation"):
        raise RuntimeError("mutation receipt drift")
    results = collections.Counter(row.split("\t")[4] for row in rows[1:])
    if results != {"KILLED": 7, "SURVIVED": 1, "REVERTED_DECLINED": 4}:
        raise RuntimeError(f"mutation result drift: {results}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    check_sources(root)
    lines = ledger_lines(raw_rows(root))
    check_symbols(root, lines)
    check_mutations(root)
    rendered = "\n".join(lines) + "\n"
    path = root / LEDGER
    if args.write:
        path.write_text(rendered)
    elif not path.exists() or path.read_text() != rendered:
        raise RuntimeError(f"inventory drift: run {Path(__file__).relative_to(root)} --write")
    print(f"builtin_cast lockdown OK: 3187 obligations; {lines[1].removeprefix('# verdicts ')}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"builtin_cast lockdown FAILED: {error}", file=sys.stderr)
        raise SystemExit(1)
