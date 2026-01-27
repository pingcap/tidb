# TiCI integrationtest2 运行说明

## 依赖工具

运行 `tests/integrationtest2/run-tests.sh` 时：
- 必须：`bash`
- 端口检查：`ss` 或 `nc`（macOS 一般用 `nc`）
- 配置渲染：`envsubst`（来自 `gettext`）

需要下载二进制时（使用 `download.sh`）：
- `oras`
- `yq`
- `tar`/`gzip`

如果不使用 `-b n` 跳过构建：
- `go`（用于 `build_mysql_tester`）
- `make`（用于 `build_tidb_server`）

## 直接运行（已有 third_bin）

如果 `../third_bin` 已包含下列二进制，可直接运行：
- `pd-server`
- `tikv-server`
- `tidb-server`
- `dumpling`
- `cdc`（或 `ticdc`）
- `tiflash`（若是目录则需要 `tiflash/tiflash`）
- `tici-server`
- `minio`（`mc` 可选）

运行：
```bash
cd /tidb/tests/integrationtest2
./run-tests.sh -t tici/tici_integration
```

## 自动下载（third_bin 不完整）

如果 `../third_bin` 缺少上述二进制，`run-tests.sh` 会直接报错。
请先执行 `tici/prepare-binaries.sh` 下载。此脚本依赖 `download.sh`、`oras`、`yq`。

示例（指定 download.sh 路径）：
```bash
cd /tidb/tests/integrationtest2
OCI_ARTIFACT_HOST='hub-zot.pingcap.net/mirrors/hub' \
DOWNLOAD_SH=./tici/download.sh \
./run-tests.sh -t tici/tici_integration
```

也可以先手动下载某一个二进制：
```bash
cd /tidb/tests/integrationtest2/tici
OCI_ARTIFACT_HOST='hub-zot.pingcap.net/mirrors/hub' ./download.sh --tiflash=feature/fts
```
