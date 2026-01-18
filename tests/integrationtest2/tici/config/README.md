# TiCI Config

Place TiCI config files here and point the runner to this directory.

Defaults:
- The runner uses `tici.toml` under this folder if `TICI_CONFIG` is not set.

Example usage:
```bash
tests/integrationtest2/tici/run-docker-cluster.sh \
  /path/to/tiflash \
  /path/to/tici \
  tests/integrationtest2/tici/config
```

Set a custom config:
```bash
TICI_CONFIG=/workspace/tici-config/custom.toml \
  tests/integrationtest2/tici/run-docker-cluster.sh /path/to/tiflash /path/to/tici /path/to/config
```
