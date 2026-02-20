# In-DB ONNX Model Serving Progress

Status log for phased implementation.

## Phase 0: Parser, AST, System Tables
- [x] Parser grammar for `CREATE/ALTER/DROP MODEL` and `MODEL_PREDICT`
- [x] AST nodes and restore/visitor wiring
- [x] System tables and infoschema exposure
- [x] Phase 0 tests green

## Phase 1: DDL, Privileges, Feature Flags
- [x] DDL execution and MVCC version resolution (direct SQL path for create/alter/drop; snapshot-aware SHOW CREATE)
- [x] Privilege checks for model DDL and inference
- [x] Sysvars: `tidb_enable_model_ddl`, `tidb_enable_model_inference`
- [x] Phase 1 tests green (TestModelDDL*)

## Phase 2: Runtime, Expression, Execution
- [ ] ONNX runtime integration (always-on build)
- [ ] Artifact loader + checksum + cache in util
- [ ] `MODEL_PREDICT` expression and executor batching
- [ ] Input validation (shape/type)
- [ ] Phase 2 tests green

## Notes
- TiFlash model pushdown is future work; v1 only pushes non-model predicates.
- Tests: `go test -run TestModelStatements --tags=intest ./pkg/parser`; `go test -run TestModelSystemTablesBootstrap --tags=intest ./pkg/session`
