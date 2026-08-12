# Classify `pkg/util/linter/constructor` as a Go-only package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

`pkg/util/linter/constructor` is not TiDB runtime behavior. It is a zero-sized marker imported only by Go structs so a Go AST analyzer can reject composite literals outside named constructor functions. This certification fixes the complete package boundary and records why a Rust runtime port or Rust unit test would be false evidence: Rust expresses the constraint with visibility and constructor APIs, and the Go marker has no value or operation to execute.

## Progress

- [x] (2026-08-12) Determined that `pkg/util/linter` is only a directory; the atomic Go package is `pkg/util/linter/constructor`.
- [x] (2026-08-12) Fixed the two-file inventory and accepted source pin `f2d25f809db5b5298111714db10944359026d9e0`; current bytes match the pin.
- [x] (2026-08-12) Read the complete production file, Bazel target, Go analyzer, analyzer tests, testdata copy, and every production import of the marker.
- [x] (2026-08-12) Confirmed there is no `doc.go`, Go unit test, `TestMain`, fixture owned by this package, benchmark, fuzz target, example, failpoint, generated input, platform/build-tag variant, `go:generate`, or `go:embed`.
- [x] (2026-08-12) Compiled the package normally and under race; both report `[no test files]` and succeed. The analyzer's `TestUtilPath` integration test also passes.
- [x] (2026-08-12) Classified the package as eliminated by Rust language semantics rather than an unported runtime package.
- [x] (2026-08-12) Ran final repository lint, source/inventory, staged whitespace, and Bazel prerequisite gates; all applicable gates passed.
- [x] (2026-08-12) Synchronized the target and prepared one package-scoped classification commit for linear publication and remote verification.

## Surprises & Discoveries

- Observation: the package has no runtime consumer despite imports in runtime Go packages.
  Evidence: `Constructor` is an empty struct with no method or state. Imports occur only in blank fields carrying a `ctor` struct tag; the separate `build/linter/constructor` analyzer reads the type and tag during static analysis.

- Observation: the package has no authoritative Go unit test to transcreate.
  Evidence: its Bazel target is a `go_library` with one source and no `go_test`. Both normal and race `go test` compile successfully and report `[no test files]`.

- Observation: the marker-path integration passes, while the analyzer's full diagnostic test is already broken on the clean target base.
  Evidence: `TestUtilPath` confirms the reflected package path equals `ConstructorUtilPath`. `TestAnalyzer` reports the same eleven missing diagnostics from testdata on target base `0c6d021686d78e070bb88bb98863ac0a7646e747`; the audit commit changes only this Markdown plan, so this belongs to `build/linter/constructor`, not to the two-file marker package, and `make lint` still passes.

## Decision Log

- Decision: Use `f2d25f809db5b5298111714db10944359026d9e0` as the complete package pin.
  Rationale: it introduced the package and is its only package-changing commit; both current files are byte-identical.
  Date/Author: 2026-08-12 / Codex

- Decision: Map the package to no Rust runtime module and no Rust unit test.
  Rationale: a zero-sized marker plus Go struct tag exists only to compensate for Go's exported-field construction rules. Adding an unused Rust marker would not enforce constructors and would create a misleading runtime artifact. Rust owners enforce construction through private fields, module visibility, and public constructors at compile time.
  Date/Author: 2026-08-12 / Codex

- Decision: Do not absorb the failing Go analyzer test into this package unit.
  Rationale: the analyzer is a different Go package under `build/linter/constructor`, with its own production code, tests, and testdata. The accepted package's integration invariant, its package path, passes independently. Fixing analyzer behavior here would violate the one-Go-package commit boundary.
  Date/Author: 2026-08-12 / Codex

## Outcomes & Retrospective

The package is completely inventoried, classified, and validated. There is no source behavior or source test to implement in Rust, and no partial port remains. The package-scoped commit is ready for linear publication; remote SHA equality is verified as an external publication receipt.

## Context and Orientation

The accepted package contains `BUILD.bazel` and `constructorflag.go`. The only exported symbol is:

    type Constructor struct{}

Go owners embed it as a blank field of type `constructor.Constructor` carrying a `ctor` struct tag. `build/linter/constructor` scans Go type information for that exact package path and type name, parses the tag, and reports manual construction outside the named functions.

The Rust rewrite does not run Go AST analyzers over Rust code. Its equivalent invariant belongs to each Rust type definition: fields that must not be manually constructed remain private, while sanctioned constructors are public. This is a native-language integration decision, not a missing crate.

## Plan of Work

Add only this classification ExecPlan. Do not add a Rust crate, marker type, semantic receipt, or vacuous Rust test. A semantic receipt is intentionally absent because there is no Rust owner or Rust command whose success could prove a Go-only AST marker; inventing an unrelated cargo command would weaken the receipt system.

Run the package compile normally and under race, the analyzer's package-path integration, repository lint, source pin/inventory checks, staged whitespace review, and the Bazel prerequisite gate. Record the analyzer diagnostic baseline failure without modifying its separate package.

Fetch `hparser-integration` before commit and again before push. Publish one classification commit normally, fetch afterward, and require equality among local HEAD, remote-tracking state, and `git ls-remote`.

## Concrete Steps

From repository root:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -count=1 ./pkg/util/linter/constructor
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -race -count=1 ./pkg/util/linter/constructor
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^TestUtilPath$' -count=1 ./build/linter/constructor
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint
    git diff --exit-code f2d25f809db5b5298111714db10944359026d9e0..HEAD -- pkg/util/linter/constructor

The full analyzer baseline command is diagnostic evidence, not a passing gate for this package:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -count=1 ./build/linter/constructor

## Validation and Acceptance

The two accepted files must remain byte-identical to the pin. Normal/race package compilation, `TestUtilPath`, repository lint, inventory, whitespace, and remote synchronization must pass. No Rust production or test file should be added.

The classification is complete only if it explicitly distinguishes the Go marker package from the separate analyzer package and records why Rust visibility enforces the intended constraint without a runtime artifact.

## Idempotence and Recovery

All validation commands are read-only. The ExecPlan is additive. If the remote advances, rebase only this one classification commit and repeat the final gates. Never force push.

## Artifacts and Notes

Failpoint decision: neither accepted file references failpoint or testfailpoint. No failpoint lifecycle is needed.

Bazel decision: the final diff adds only this Markdown classification. It changes no Go file, import, top-level Go test, Bazel file, Go module, or Bazel target, so `make bazel_prepare` is not required.

Initial evidence:

    pkg/util/linter/constructor normal compile: pass, no test files.
    pkg/util/linter/constructor race compile: pass, no test files.
    build/linter/constructor TestUtilPath: pass.
    build/linter/constructor TestAnalyzer: clean-base failure with eleven missing expected diagnostics.
    PATH=... GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint: pass.
    source pin, two-file inventory, staged whitespace, and Bazel prerequisite gates: pass.

## Interfaces and Dependencies

There is no Rust interface or dependency. The Go package exports only `constructor.Constructor`, a zero-sized static-analysis marker. Rust construction constraints remain local to the types they protect.

Plan revision note (2026-08-12): created after complete inventory/source/analyzer review and clean-base validation established a Go-only language-tool classification.

Plan revision note (2026-08-12): recorded all applicable Ready classification gates.

Plan revision note (2026-08-12): rebased onto target `0c6d021686d78e070bb88bb98863ac0a7646e747` and repeated the Ready gates and analyzer baseline reproduction.
