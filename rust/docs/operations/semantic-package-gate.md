# Semantic package gate

The gate protects only facts that matter to the rewrite:

- the accepted Go source is pinned and unchanged;
- the named Rust implementation and fixtures exist;
- executable semantic tests pass.

Each `*.semantic.toml` is either a `whole-go-package` claim, which inventories
the complete tracked Go package, or a `package-seed`, which lists its accepted
Go files and makes no completion claim. There are no generated receipts,
branch ledgers, status taxonomies, mutation transcripts, or historical logs.

Run one boundary while iterating:

```bash
python3 rust/scripts/semantic-package-gate.py rust/crates/<crate>/tests/<name>.semantic.toml
```

Run every boundary before integration:

```bash
python3 rust/scripts/semantic-package-gate.py --all
```

Use `--no-tests` for the fast source/file check. Identical test commands across
specifications are executed once.
