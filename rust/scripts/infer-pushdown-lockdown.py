#!/usr/bin/env python3
"""Generate/check the exact source-owned infer_pushdown.go lockdown ledger."""

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
    "pkg/expression/infer_pushdown.go": (25020, 598, "f5f9c97ee653aca12116249131affd467a26ded1b9b24d98754e59cac4f570eb"),
    "pkg/expression/expr_to_pb_test.go": (106420, 2219, "e03e21e0175014d938be6205a8a7b6f8b66333b36c2c06c2ab6a1d80a44e89d4"),
    "pkg/expression/scalar_function_test.go": (8406, 260, "1712f4df758514aa42e0710ef8562c0ca8c0c43cec308337ada1971e09f8ce15"),
    "pkg/expression/fts_to_like_test.go": (13898, 340, "aca2e0d0e1e5f7a4487209d225e65b4afad0828bbeb4b22efa0114c645b4ad0c"),
}
EXPECTED = {
    "branch": 52, "closure": 2, "declaration": 1, "field": 4,
    "function": 21, "loop": 8, "short_circuit": 36, "switch_case": 84,
    "test": 14, "test_assertion": 331, "test_branch": 8,
    "test_helper": 3, "test_helper_closure": 6, "test_loop": 30,
    "test_row": 393, "var": 3,
}
DIRECT_SUPPORT = {
    "TestConstant2Pb", "TestColumn2Pb", "TestCompareFunc2Pb",
    "TestJsonPushDownToFlash", "TestExprPushDownToFlash",
    "TestExprOnlyPushDownToFlash", "TestExprPushDownToTiKV",
    "TestExprOnlyPushDownToTiKV", "TestNewCollationsEnabled", "TestMetadata",
    "TestPushDownSwitcher", "TestPanicIfPbCodeUnspecified",
    "TestForbidUnixTimestampPushdown",
    "TestScalarExprSupportedByFlashRejectsNonDefaultFTSModifier",
    "genColumn", "columnCollation", "newFTSMatchAgainstForTest",
}
LEDGER = Path("rust/crates/tidb-expr/src/infer_pushdown.inventory.tsv")
ANCHORS = Path("rust/crates/tidb-expr/src/infer_pushdown_lockdown.rs")
MUTATIONS = Path("rust/crates/tidb-expr/src/infer_pushdown.mutations.tsv")
GO_TOOL = Path("rust/difftests/tools/go_package_lockdown_inventory")
SYMBOLS = {
    "STORE_MASK": "infer_pushdown_lockdown::STORE_MASK",
    "BLACKLIST_POLICY": "infer_pushdown_lockdown::BLACKLIST_POLICY",
    "POLICY_TIKV": "infer_pushdown_lockdown::POLICY_TIKV",
    "POLICY_FLASH": "infer_pushdown_lockdown::POLICY_FLASH",
    "POLICY_TIDB": "infer_pushdown_lockdown::POLICY_TIDB",
    "ENUM_POLICY": "infer_pushdown_lockdown::ENUM_POLICY",
}
PORTED_PRODUCTION = {
    "storeTypeMask": ("STORE_MASK", "P_STORE_MASK"),
    "IsPushDownEnabled": ("BLACKLIST_POLICY", "P_BLACKLIST_MASK"),
    "scalarExprSupportedByTiKV": ("POLICY_TIKV", "P_TIKV_POLICY"),
    "scalarExprSupportedByFlash": ("POLICY_FLASH", "P_FLASH_POLICY"),
    "scalarExprSupportedByTiDB": ("POLICY_TIDB", "P_TIDB_UNION"),
    "canEnumPushdownPreliminarily": ("ENUM_POLICY", "P_ENUM_POLICY"),
}
DECLINED_PRODUCTION = {
    "init": "D_GLOBAL_ATOMIC",
    "canFuncBePushed": "D_FAILPOINT_GLOBAL",
    "canScalarFuncPushDown": "D_EXPR_RUNTIME",
    "canExprPushDown": "D_EXPR_RUNTIME",
    "PushDownExprsWithExtraInfo": "D_EXPR_RUNTIME",
    "PushDownExprs": "D_EXPR_RUNTIME",
    "CanExprsPushDownWithExtraInfo": "D_EXPR_RUNTIME",
    "CanExprsPushDown": "D_EXPR_RUNTIME",
    "NewPushDownContext": "D_CONTEXT_RUNTIME",
    "NewPushDownContextFromSessionVars": "D_CONTEXT_RUNTIME",
    "PushDownContext.EvalCtx": "D_CONTEXT_RUNTIME",
    "PushDownContext.PbConverter": "D_CONTEXT_RUNTIME",
    "PushDownContext.Client": "D_CONTEXT_RUNTIME",
    "PushDownContext.GetGroupConcatMaxLen": "D_CONTEXT_RUNTIME",
    "PushDownContext.AppendWarning": "D_WARNING_RUNTIME",
}
PORTED_TESTS = {
    "TestForbidUnixTimestampPushdown": ("POLICY_TIKV", "P_TIKV_POLICY"),
    "TestScalarExprSupportedByFlashRejectsNonDefaultFTSModifier": ("POLICY_FLASH", "P_FLASH_POLICY"),
}


def run(root: Path, args: list[str]) -> str:
    return subprocess.run(args, cwd=root, check=True, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout


def check_sources(root: Path) -> None:
    for rel, expected in FILES.items():
        data = (root / rel).read_bytes()
        got = (len(data), data.count(b"\n"), hashlib.sha256(data).hexdigest())
        if got != expected:
            raise RuntimeError(f"source drift for {rel}: expected {expected}, found {got}")


def raw_rows(root: Path) -> list[list[str]]:
    with tempfile.TemporaryDirectory(prefix="infer-pushdown-lockdown-") as tmp:
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
    all_rows = [line.split("\t") for line in lines[1:]]
    if any(len(row) != 6 for row in all_rows):
        raise RuntimeError("malformed AST inventory")
    rows = [row for row in all_rows
            if row[2].endswith("infer_pushdown.go") or row[5] in DIRECT_SUPPORT]
    counts = collections.Counter(row[1] for row in rows)
    if dict(sorted(counts.items())) != EXPECTED:
        raise RuntimeError(f"AST census drift: {dict(sorted(counts.items()))}")
    if len(rows) != 996 or len({row[0] for row in rows}) != 996:
        raise RuntimeError("obligation count/identity drift")
    return rows


def classify(source: str, owner: str) -> tuple[str, str, str]:
    if source.endswith("_test.go"):
        if owner in PORTED_TESTS:
            symbol, evidence = PORTED_TESTS[owner]
            return "PORTED", SYMBOLS[symbol], evidence
        if owner in DIRECT_SUPPORT:
            return "DECLINED", "-", "D_GO_TEST_RUNTIME"
        raise RuntimeError(f"unclassified direct test/support owner: {owner}")
    if owner.startswith("type:PushDownContext"):
        return "DECLINED", "-", "D_CONTEXT_RUNTIME"
    if owner.startswith("var:"):
        return "DECLINED", "-", "D_GLOBAL_ATOMIC"
    if owner in PORTED_PRODUCTION:
        symbol, evidence = PORTED_PRODUCTION[owner]
        return "PORTED", SYMBOLS[symbol], evidence
    if owner in DECLINED_PRODUCTION:
        return "DECLINED", "-", DECLINED_PRODUCTION[owner]
    raise RuntimeError(f"unclassified production owner: {owner}")


def ledger_lines(rows: list[list[str]]) -> list[str]:
    result = ["# infer-pushdown-lockdown-v1",
              "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\tgo_owner\tstatus\trust_symbol\tevidence"]
    statuses = collections.Counter()
    for obligation, category, source, anchor, node_hash, owner in rows:
        status, symbol, evidence = classify(source, owner)
        statuses[status] += 1
        result.append("\t".join((obligation, category, source, anchor, node_hash,
                                  owner, status, symbol, evidence)))
    if sum(statuses.values()) != 996 or set(statuses) != {"PORTED", "DECLINED"}:
        raise RuntimeError(f"verdict census invalid: {statuses}")
    result.insert(1, "# verdicts " + " ".join(
        f"{key}={statuses[key]}" for key in ("PORTED", "DECLINED", "UNREACHABLE")))
    return result


def check_symbols(root: Path, lines: list[str]) -> None:
    text = (root / ANCHORS).read_text()
    declared = {symbol for symbol in SYMBOLS.values() if f'"{symbol}"' in text}
    used = {line.split("\t")[7] for line in lines
            if not line.startswith("#") and not line.startswith("obligation_id")
            and line.split("\t")[6] == "PORTED"}
    if used != declared:
        raise RuntimeError(f"PORTED symbol gate drift: ledger={used}, anchors={declared}")
    for needle in ("store_type_mask", "is_push_down_enabled", "can_function_be_pushed",
                   "scalar_expr_supported_by_tikv", "scalar_expr_supported_by_flash",
                   "scalar_expr_supported_by_tidb", "can_enum_pushdown_preliminarily"):
        if needle not in text:
            raise RuntimeError(f"compile anchor disappeared: {needle}")


def check_mutations(root: Path) -> None:
    rows = (root / MUTATIONS).read_text().rstrip().splitlines()
    if len(rows) != 24 or not rows[0].startswith("probe_id\trule\tmutation"):
        raise RuntimeError("mutation receipt drift")
    fields = [row.split("\t") for row in rows[1:]]
    if any(len(row) != 6 for row in fields):
        raise RuntimeError("malformed mutation receipt")
    if collections.Counter(row[4] for row in fields) != {"KILLED": 23}:
        raise RuntimeError("all reachable boundary mutations must be killed")


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
    print(f"infer_pushdown lockdown OK: 996 obligations; {lines[1].removeprefix('# verdicts ')}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"infer_pushdown lockdown FAILED: {error}", file=sys.stderr)
        raise SystemExit(1)
