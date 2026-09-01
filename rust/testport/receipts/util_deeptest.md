# `pkg/util/deeptest` — Go-master parity boundary receipt

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).
This is a reflection-heavy Go test utility; it has no Rust production owner.

## Complete inventory

All three artifacts were read in full:

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 21 | `9cd5b19e1bb2d7536e3c4abc205607475defb658` | `ddfc3dd244cd6c696d7e645d464ce8759e0b71039406561817c4c965011beb74` | library/test target and glob dependency inventoried |
| `statictesthelper.go` | 274 | `73ea44229c5c9f70dbe559e22ebc535790fad835` | `99a5a071ce0061144c608de698e2400afea6a0fac97029c2e9c8e307f8effd5d` | recursive reflection comparators/options inventoried |
| `statictesthelper_test.go` | 208 | `577299505e9742f0ef742cdaf6766d5503908cba` | `ab7a5bb540b48d6a224049e4442fde18f518b7dffd13e1ad06ff4f4c859b770a` | pointer/function/map/slice failure matrix inventoried and executed |

There is no `doc.go`, generated/platform variant, fixture, benchmark, fuzz
target, or nested package. The helper has two public generic assertions, two
glob-path options, and a reflection walk covering structs, pointers, slices,
arrays, maps, interfaces, functions, and channels.

## Go behavior

`AssertRecursivelyNotEqual` requires every corresponding field in the common
shape to differ, treating invalid values, type differences, path ignores,
pointer identity, and shared map/slice prefixes according to the options.
`AssertDeepClonedEqual` requires equal values and recursively distinct pointer,
slice, and map storage unless a path explicitly compares pointer identity; it
handles nil interfaces/functions/channels and fails unsupported kinds. Glob
patterns select ignored paths or pointer-comparison paths. The source test
drives all supported kinds and captures expected assertion failures through a
panic-backed `TestingT`.

## Rust ownership and integration decision

Rust test modules contain local assertions and comments derived from Go's
`deeptest` in planner/expression tests, but no reusable reflection comparator
with the source's path glob, pointer identity, and intentional failure
semantics. This package is test-only infrastructure; adding a Rust runtime
dependency or a second generic assertion framework would not port a production
contract and would create Rust-only test policy. No source change is justified.

## Validation

Profile: **WIP**. This is an inventory and explicit boundary audit with no
code fix or package-completion claim; `make bazel_prepare` and the Ready lint
gate are not triggered.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/deeptest -count=1
# ok
```

## Risks and unverified behavior

- Correctness: the full Go assertion matrix passes; no Rust assertion owner is
  claimed.
- Compatibility: pointer alias, map/slice storage, and glob-path semantics
  remain Go test infrastructure contracts.
- Performance: no runtime code changed. A future test-only port must preserve
  reflection walk termination and avoid production dependencies.
- Not verified locally: Bazel target execution, architecture-specific reflect
  behavior, and any Rust test suite that would consume a shared comparator.
