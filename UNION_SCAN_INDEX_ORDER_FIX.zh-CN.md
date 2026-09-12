# Dirty UnionScan 索引顺序修复

## 根因

Go 的 `UnionScanExec` 在 dirty table 上合并 snapshot 与 membuffer 时，两个输入都按索引键和 handle 顺序读取。Rust 的非覆盖索引 reader 仍会把 handle batch 重排，导致 staged insert/update/delete 的结果退化为 handle 顺序。

## 修复

当表存在事务 dirty content 时，physical index reader 调用 `answer_in_index_order()`，保留索引游标顺序；clean table 继续允许 handle batch 重排。

## 验证

- `tests_union_scan`：`11 passed, 0 failed`；
- 覆盖组合 mutation、单独 insert/update/delete、tie handle、rollback 和 clean-table 对照；
- `make lint` 通过。
