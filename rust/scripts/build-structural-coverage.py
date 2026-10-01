#!/usr/bin/env python3
"""Account for the entire inventory without treating mapping as acceptance.

Run from the repository root after inventory-go-rust-parity.py. Crate ownership
is a review grouping, not a claim that one crate transcreates one Go package.
The source inventory remains the authority for each individual artifact.
"""
import collections
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
AUDIT = ROOT / "rust/docs/parity/current-audit"

# Every crate has exactly one primary review queue; repairs can cross queues.
CRATES = {
    "Syntax and name resolution": "ast lexer parser resolve hint naming mysql",
    "Values and expressions": "datatype expr chunk codec tablecodec schemacmp hash hack",
    "Planning": "planner funcdep kvcache planner-coretestsdk",
    "Execution": "exec executor sqlexec sqlexec-mock",
    "Session and authorization": "session vardef stmtsummary",
    "Domain and shared services": "domain owner schemaver syssession workloadrepo",
    "Metadata and tables": "meta metadef model placement",
    "DDL": "ddl-copr ddl-logutil ddl-mock ddl-notifier ddl-resourcegroup ddl-serverstate ddl-session ddl-testargsv1",
    "Storage and distributed reads": "txnkv distsql unistore tikvutil gcutil",
    "Protocols and external services": "proto protocol pd-client",
    "Server and configuration": "server config",
    "Background jobs and bulk data": "dxf dxf-operator br ttl timer resourcemanager",
    "Utilities and errors": "allocator-stats errmsg error log util",
}

# Most-specific matching prefix wins. Unmapped product surfaces remain explicit.
GO_PREFIXES = {
    "Syntax and name resolution": "pkg/parser pkg/util/parser pkg/util/hint pkg/util/resolve",
    "Values and expressions": "pkg/types pkg/expression pkg/util/chunk pkg/util/codec pkg/tablecodec pkg/util/collate pkg/util/rowcodec pkg/util/hack pkg/util/schemacmp",
    "Planning": "pkg/planner pkg/util/kvcache",
    "Execution": "pkg/executor",
    "Session and authorization": "pkg/session pkg/sessionctx pkg/sessiontxn pkg/privilege pkg/bindinfo pkg/util/stmtsummary",
    "Domain and shared services": "pkg/domain pkg/owner pkg/util/syssession pkg/util/workloadrepo",
    "Metadata and tables": "pkg/meta pkg/structure pkg/table pkg/autoid_service pkg/infoschema",
    "DDL": "pkg/ddl",
    "Statistics": "pkg/statistics",
    "Storage and distributed reads": "pkg/store pkg/kv pkg/distsql pkg/keyspace",
    "Server and configuration": "cmd/tidb-server pkg/server pkg/config",
    "Background jobs and bulk data": "pkg/dxf pkg/ttl pkg/timer pkg/resourcemanager pkg/resourcegroup br pkg/lightning lightning pkg/ingestor pkg/objstore pkg/importsdk pkg/dumpformat dumpling",
    "Utilities and errors": "pkg/util pkg/errno pkg/errctx pkg/metrics pkg/format",
    "Build, tools and test support": "tests build tools pkg/testkit cmd",
}

FINDINGS = {
    "Syntax and name resolution": "A01, N01; grammar/AST variants still require full review",
    "Values and expressions": "X01, K03, N01",
    "Planning": "Q01, C02, E02, M01–M02",
    "Execution": "E02–E07, K03; E01 repaired",
    "Session and authorization": "A01–A04, B01–B02, S01–S04, I01–I03, N04; C01 repaired",
    "Domain and shared services": "O01–O11, O13, I04, C02; O12 repaired",
    "Metadata and tables": "K01–K03, I04, T01, D01–D11",
    "DDL": "D01–D11, F01–F03",
    "Statistics": "O07; cache/loading/analyze have live owners, full contract review remains",
    "Storage and distributed reads": "T01–T03, C03, M01–M04, O03, O13",
    "Protocols and external services": "P03, T02; P01–P02, P04 repaired; other helpers/variants unreviewed",
    "Server and configuration": "N01–N05, A02–A03, O01–O11, O13; O12 repaired",
    "Background jobs and bulk data": "O04–O06, O10, E05, E07; P04 repaired; other bulk-data packages unreviewed",
    "Utilities and errors": "O11; other utility/error contracts unreviewed",
    "Build, tools and test support": "Original suites/build variants not accepted at current master",
    "Other upstream product surfaces": "Unreviewed: no inference of absence from missing crate names",
}


def main():
    inventory = json.loads((AUDIT / "package-coverage.json").read_text())
    assigned = {}
    for group, names in CRATES.items():
        for name in names.split():
            crate = "rust/crates/tidb-" + name
            if crate in assigned:
                raise ValueError(f"duplicate crate assignment: {crate}")
            assigned[crate] = group
    actual = {entry["path"] for entry in inventory["rust_crates"]}
    manifests = {
        str(path.parent.relative_to(ROOT))
        for path in (ROOT / "rust/crates").glob("*/Cargo.toml")
    }
    if actual != manifests:
        raise ValueError("Rust crate inventory is stale; rerun inventory-go-rust-parity.py")
    for crate in actual:
        if pathlib.PurePosixPath(crate).name.startswith("tidb-stats"):
            if crate in assigned:
                raise ValueError(f"duplicate statistics assignment: {crate}")
            assigned[crate] = "Statistics"
    if set(assigned) != actual:
        raise ValueError(f"crate coverage changed: missing={actual - assigned.keys()}, extra={assigned.keys() - actual}")

    prefixes = sorted(
        ((prefix, group) for group, paths in GO_PREFIXES.items() for prefix in paths.split()),
        key=lambda item: len(item[0]), reverse=True,
    )
    packages = collections.defaultdict(list)
    for entry in inventory["packages"]:
        name = entry["package"]
        group = next((group for prefix, group in prefixes if name == prefix or name.startswith(prefix + "/")), "Other upstream product surfaces")
        packages[group].append(name)
    if len({entry["package"] for entry in inventory["packages"]}) != len(inventory["packages"]):
        raise ValueError("duplicate Go package in inventory")
    if sum(map(len, packages.values())) != len(inventory["packages"]):
        raise ValueError("package coverage is incomplete")

    lines = [
        "# Complete structural-audit scope", "",
        f"Go reference: `{inventory['go_master']}`. Regenerate with",
        "`python3 rust/scripts/build-structural-coverage.py`.", "",
        f"This accounts for **all {len(actual)} Rust crates and all {len(inventory['packages'])} inventoried TiDB Go package directories**.",
        "Counts include test/support package directories. The grouping is a work queue, not a semantic mapping or acceptance receipt.",
        "An entrypoint review can establish a finding but cannot clear the rest of its package.",
        "Every row still requires complete production, generated/platform/build, original-test, fixture and integration validation.",
        "The complete [globalconfigsync receipt](global-config-sync-repair.md) records one leaf package and its integration; it does not accept its whole parent crate.",
        "The [restore-utils receipt](restore-utils-protocol-repair.md) records the complete package review and P04 protocol repair; live BRIE and other BR owners remain open.",
        "The exact package/artifact list remains in [package-coverage.json](package-coverage.json); no copy replaces it.", "",
        "## Subsystem queues", "",
        "| Queue | Go package directories | Rust crates | Confirmed findings / review limits |",
        "| --- | ---: | ---: | --- |",
    ]
    for group in FINDINGS:
        lines.append(f"| {group} | {len(packages[group])} | {sum(value == group for value in assigned.values())} | {FINDINGS[group]} |")
    lines += ["", "## Every Rust crate", "", "The register describes the reviewed entrypoints; **none of these rows asserts full current-master package acceptance**.", "", "| Crate | Primary queue |", "| --- | --- |"]
    for crate, group in sorted(assigned.items()):
        manifest = ROOT / crate / "Cargo.toml"
        if not manifest.is_file():
            raise FileNotFoundError(manifest)
        lines.append(f"| `{manifest.parent.name}` | {group} |")
    lines += ["", "## External package obligations", "", "| Inventory | Package directories | Revision |", "| --- | ---: | --- |"]
    for stem in ("client-go", "kvproto", "pd-client", "etcd-api"):
        path = AUDIT / f"{stem}-package-coverage.json"
        data = json.loads(path.read_text())
        if data["go_master"] != inventory["go_master"]:
            raise ValueError(f"mixed master baselines: {path}")
        lines.append(f"| [{stem}]({path.name}) | {len(data['packages'])} | `{data['version']}` |")
    lines += ["", "TiPB complete-source and descriptor receipts are separate. Other external modules still need full inventories; the four rows above are not all dependencies.", "Native client-rust's local latches and region TTL have live production callers; their existence does not accept all 41 client-go packages.", "", "## Upstream surfaces not yet assigned a runtime owner review", "", "These are explicit outstanding scope, not automatically confirmed defects:", ""]
    lines += [f"- `{name}`" for name in packages["Other upstream product surfaces"]]
    lines += ["", "NextGen/starter/standby and platform/build variants remain in the artifact inventory. Classic-only refusals must be compared with Go classic before becoming findings.", "Benchmarks and distributed fault testing remain required; none was performed by this scope generator.", ""]
    (AUDIT / "structural-coverage.md").write_text("\n".join(lines))
    print(f"Structural scope accounted for: {len(actual)} crates, {len(inventory['packages'])} TiDB package directories; no acceptance inferred")


if __name__ == "__main__":
    main()
