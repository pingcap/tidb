# `pkg/parser/tidb` — complete package parity receipt

Pinned Go source: `c6054025ed4c32ab3672a2a24ea46892714d21ec` (`origin/master`).

## Complete inventory

The package contained two tracked artifacts before editing: `BUILD.bazel` and
`features.go` (75 lines total). It has no `doc.go`, additional production
files, tests, fixtures, generated/platform variants, fuzz/benchmark targets,
or other build inputs. The focused regression added in this batch is
`features_test.go`; BUILD metadata now declares its Go test target.

## Restored Go behavior

`FeatureIDPreSplit` is the canonical pre-split identifier, with the old
`FeatureIDPresplit` spelling retained as a deprecated alias. The
`FeatureIDAutoPreSplit` identifier (`auto_presplit`) is now accepted by
`CanParseFeature`, while `resource_group` remains intentionally excluded from
parser special-comment allowlisting. `TestCanParseFeaturePreSplitVariants`
locks these compatibility and allowlist contracts in place.

## Validation

Profile: Ready.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/parser/tidb -count=1
make lint
git diff --check
make bazel_prepare (blocked: bazel executable is unavailable)
```

No Rust owner consumes this registry, so no Rust API was invented and no Rust
behavior was removed. Bazel regeneration remains the only unavailable gate.

## Risks

Feature identifiers are parser comment/API tokens; changing their spelling or
allowlist can alter feature-gated SQL acceptance. The alias preserves source
compatibility, and the new identifier is covered by a deterministic unit test.
