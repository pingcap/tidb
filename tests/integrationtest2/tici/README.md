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

## 直接运行（已有 third_bin）

如果 `../third_bin` 已包含下列二进制，可直接运行：
- `pd-server`
- `tikv-server`
- `tidb-server`
- `dumpling`
- `cdc`
- `tiflash`（若是目录则需要 `tiflash/tiflash`）
- `tici-server`
- `minio`
- `mc`

运行：
```bash
cd /tidb/tests/integrationtest2
./run-tests.sh -t tici/tici_integration
```

## 下载二进制（third_bin 不完整）

如果 `../third_bin` 缺少上述二进制，`run-tests.sh` 会直接报错。
请先执行 `tici/download.sh` 下载。此脚本依赖 `oras`、`yq`。

示例（下载指定分支的 tiflash）：
```bash
cd /tidb/tests/integrationtest2/third_bin
OCI_ARTIFACT_HOST='hub-zot.pingcap.net/mirrors/hub' ../tici/download.sh --tiflash=feature/fts
```

示例（下载最新的 cdc）：
```bash
cd /tidb/tests/integrationtest2/tici/third_bin
OCI_ARTIFACT_HOST='hub-zot.pingcap.net/mirrors/hub' ../tici/download.sh --ticdc-new=master
```

示例（下载最新的 tici）：
```bash
cd /tidb/tests/integrationtest2/tici/third_bin
OCI_ARTIFACT_HOST='hub-zot.pingcap.net/mirrors/hub' ../tici/download.sh --tici=master
```
