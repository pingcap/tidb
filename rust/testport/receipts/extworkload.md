# External workload manager parity receipt

Date: 2026-09-02

## Inventory and scope

Before editing, the complete `pkg/extworkload` tree was read: 9 artifacts and
1,400 lines, including the root manager/interface/build files and the nested
gRPC client, tests, and BUILD input. There is no doc.go, fixture/testdata,
generated/platform variant, fuzz target, or benchmark artifact.

Go-master behavior restored:

- Use `time.Duration` for GCV2 and GC-lifetime APIs and convert to controller
  seconds at the RPC boundary instead of seeding a fixed 600-second lifetime.
- Rename the TTL registration method to `RegisterTTLTableInfo` to reflect its
  table metadata contract.
- Bind managers to stores, expose upgrade-time GCV2 abort handling, and keep
  role predicates nil-safe.
- Add focused manager deadline/metric, duration-conversion, store-binding, and
  abort-error regressions; update the TTL worker test double to the interface.

The manager remains coupled to TiDB's Go session/store lifecycle and the
external-workload protobuf service. No dependency-closed Rust replacement is
integrated or claimed.

## Validation

Passing failpoint-safe tests:

```text
tools/check/failpoint-go-test.sh ./pkg/extworkload -run '<manager and lifecycle tests>' -count=1
tools/check/failpoint-go-test.sh ./pkg/extworkload/client -run '^(TestClientRoundTrip|TestClientInterceptor|TestClientErrorMapping|TestMapResponseNilResponse|TestNewClientValidation|TestNormalizeAddr)$' -count=1
git diff --check
```

The final Ready gate reruns `make lint`. `make bazel_prepare` is required for
these Go/BUILD changes but is blocked locally because no `bazel` executable is
installed.
