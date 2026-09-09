# Meta-service client parity receipt

Date: 2026-09-02

## Inventory and scope

Before editing, the complete `pkg/metaservice` package was read: 6 artifacts
and 708 lines (BUILD, `etcd.go`, `etcd_test.go`, `metamanager.go`, and
`metamanager_test.go`). There is no package `doc.go`, fixture/testdata,
generated/platform variant, fuzz target, benchmark input, or additional build
artifact.

Go-master behavior restored:

- Resolve dedicated keyspace meta-service groups and namespace etcd clients
  from PD, with caller endpoint preservation for the global group.
- Add injectable PD-client construction and explicit missing-keyspace errors.
- Parse HTTP(S) and Unix PD service URLs, skip malformed members when a usable
  endpoint remains, and expose the renamed `GetPDServiceURLs` contract.
- Add focused tests for keyspace namespaces, endpoint selection, API-context
  construction, Unix sockets, malformed URL handling, and missing metadata.

This client is coupled to TiDB's Go PD/etcd lifecycle and remains Go-owned; no
dependency-closed Rust replacement is integrated or claimed.

## Validation

Passing failpoint-safe package tests and compile checks:

```text
tools/check/failpoint-go-test.sh ./pkg/metaservice -run '^(TestGetPDAddrsPDOnlyClient|TestDialEtcdClientMissingKeyspaceMetaIncludesKeyspaceName|TestGetPDAddrsWithRealClient|TestParseURL|TestGetGroup|TestGetInfo)$' -count=1
go test ./pkg/metaservice ./pkg/server ./pkg/store/mockstore/unistore -run '^$' -count=1 -vet=off
git diff --check
```

The final Ready gate reruns `make lint`. `make bazel_prepare` is mandatory for
the BUILD and Go changes but is blocked locally because `bazel` is unavailable.
