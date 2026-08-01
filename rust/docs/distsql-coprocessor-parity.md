# Coprocessor request/response parity: `rust/crates/tidb-distsql` vs `pkg/distsql` + `pkg/store/copr`

Status: in progress. Nothing in this document was executed: this machine cannot
run a freshly built binary, so every claim is read from source on both sides.

## 1. ScalarFuncSig numbering

Verdict: the 52 signature constants declared in
`rust/crates/tidb-proto/proto/select.proto` all carry the same integer as
upstream `github.com/pingcap/tipb` (pinned at `v0.0.0-20260623093813-5f9928e91afe`
in `go.mod:108`). Checked mechanically against
`go-tipb/expression.pb.go`'s `ScalarFuncSig_value` map. Zero mismatches.
