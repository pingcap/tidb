## About

Use [jsonnet](https://github.com/google/go-jsonnet) to generate Grafana-compliant json scripts for use with TiDB.

Why jsonnet?

1. jsonnet is a DSL created by Google for json, which is good for advanced json editing work.
2. Grafana provides the [jsonnet library](https://grafana.github.io/grafonnet-lib/) specifically for generating Grafana json, which makes maintaining TiDB's json scripts much easier.

## Usage

1. Modify the jsonnet files (e.g. tidb_summary.jsonnet).
2. Run `generate_json.sh` to generate the json files by the jsonnet files.
3. Commit the modifications.