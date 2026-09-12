# JSON_SCHEMA_VALID 外部引用修复

## 根因

Rust `jsonschema` 的默认 HTTP retriever 在当前构建配置下未可靠访问 loopback；因此 HTTP `$ref` 没有发出请求，集成测试一直得到“resource not present in registry”。Go master 的 `qri-io/jsonschema` 会在文档验证阶段按绝对 URI 获取外部 schema。

## 修复

在 `tidb-expr` 的 validator 边界安装自定义 `Retrieve`：

- `http`/`https` 使用 `reqwest` blocking client，并显式禁用代理；
- `file` 从 URI 路径读取并解析 JSON；
- 其他 scheme 返回错误；
- 外部资源仍在真实验证阶段按 URI 获取，未缓存伪造内容或放宽校验。

## 证据

- HTTP listener 收到 `GET /integer.json`，测试通过；
- `cargo test -p tidb-expr --lib`：`1175 passed; 0 failed; 99 ignored`；
- `make lint`：通过。
