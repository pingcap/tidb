# client-go differential harness

This isolated Go module pins the exact client-go source revision used by the
parity ledger. It runs the same codec-neutral raw and transaction contract as
`tests/api_version_live_tests.rs` and prints the same canonical result.

Use a TiKV/PD version compatible with the pinned client's PD router RPC. The
recorded completion run used the installed v9.0.0 beta-nightly binaries from
2026-08-21. For API v1, TiKV must use `storage.api-version = 1` with TTL
disabled; for API v2, use the `DEFAULT` keyspace.

Run the Go and Rust sides against each cluster:

```console
cd tests/client_go_differential
go run . --api-version 1 --pd 127.0.0.1:3479
go run . --api-version 2 --pd 127.0.0.1:3379

cd ../..
PD_ADDRS=127.0.0.1:3479 TIKV_API_VERSION=1 \
  cargo test --test api_version_live_tests --features integration-tests -- \
  --ignored --no-capture raw_and_transaction_contract_matches_client_go
PD_ADDRS=127.0.0.1:3379 TIKV_API_VERSION=2 \
  cargo test --test api_version_live_tests --features integration-tests -- \
  --ignored --no-capture raw_and_transaction_contract_matches_client_go
```

All four commands must print:

```text
client-parity api=<version> raw=batch_get:c,-,a scan:a,c reverse:c,a txn=get:b,- scan:b,d
```
