# 索引 reader 保留动态分区裁剪集合

基线 `541e47c084`，Go master 固定为 `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`。

## 根因和证据

原回归 `tests_partition_projection::a_pruned_index_reader_decodes_the_column_the_scope_names` 在 `/tmp/global-index-projection-red.log` 退出 101。GLOBAL idx1(b) 查询 `SELECT b FROM t PARTITION(p0) USE INDEX(idx1) WHERE b<=2` 错误返回 1、2，Go 只返回 1。

Rust `build_index_reader` 忽略 PhysicalIndexScan.dynamic_partition_access。全局索引所有分区共用逻辑表 key prefix，底层 IndexRangeCursor 虽然能解码索引值内的分区 ID 并过滤，但传入的是全分区集合。读取列已正确，当前失败根因是额外分区的行漏过滤。

Go `pkg/executor/builder.go:4498` 给全局索引 reader 设置 getPartitionIDsAfterPruning 的 partitionIDMap。实机 `/tmp/global-partition-go.out` 中覆盖索引和回表计划都有 `in(_tidb_tid, 130)`；前者返回 b=1，后者返回 (a,b)=(5,1)。双分区与空结果对照在 `/tmp/global-partition-controls-go.out`，局部索引对照在 `/tmp/local-partition-controls-go.out`。

修复在创建索引 source 前，将非 all_partitions 的动态裁剪集合传入 KvTable.restrict_read_to_partitions；包括空集合。原游标继续负责全局索引 partition ID 解码和过滤，本地索引则限制扫描的物理前缀。没有更改原回归断言，补充覆盖索引、回表零匹配及分区交集为空的查询。

## Ready 验证

目录 `/tmp/tidb-hparser-current`；cargo 环境 `RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`。

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib a_pruned_index_reader_decodes_the_column_the_scope_names
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_partition
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
make lint
git diff --check
```

原回归及新增检查全部通过（`/tmp/global-index-projection-green.log`）。相关分区套件 90 passed / 6 failed（`/tmp/global-partition-suite.log`），上一轮为 89 / 7；其余六项保留失败。集成 310 passed / 0 failed（`/tmp/global-partition-integration.log`），lint 退出 0（`/tmp/global-partition-lint.log`）。Go 对照节点正常停止并回收，退出 0。

本轮未运行真实 TiKV 索引下推套件或完整 Rust/Go/Bazel 门禁；不能以本地行语义回归代替这些验收。原 shared-statistics WIP 保留。非分区表 PARTITION 错误码的测试契约另行提交，不包含在本修复中。整体所有 failed cases 目标继续保持未完成。
