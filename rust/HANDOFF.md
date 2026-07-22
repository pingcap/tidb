# Active package: `pkg/parser/ast`

This is the living ExecPlan required by `PLANS.md`.

## Done when

- Every file under `pkg/parser/ast`, including every original test vector and
  support artifact, has a behaviorally equivalent Rust home.
- Restore, errors, flags, labels, read-only classification, SEM, and visitor
  traversal match Go.
- The original Go package tests and all owning Rust crate tests pass.
- The complete package is reviewed, committed, and pushed as one checkpoint.

## Work

1. Take one Go production file and its original test file together; transcreate
   their complete behavior directly into the stable Rust owner.
2. While touching that owner, fold campaign-era syntax leaves and micro-test
   files into it. Do not preserve files whose only purpose was partial work.
3. During coding, run only the owning AST/parser tests. Run a workspace compile
   only when a shared public API changes.
4. At package close, run the original Go tests plus Rust format, Clippy,
   workspace tests, doc tests, and diff checks once.
5. Commit and push every cohesive green checkpoint. Until all owners close,
   every checkpoint remains explicitly incomplete `pkg/parser/ast` work.

Current state: `base`, `model`, `flag`, `util`, `sem`, and `stats` original test
files are directly represented. The package-wide visitor contract and all nine
original visitor obligations pass. SHOW is folded into its owner and preserves
`SHOW OPEN TABLES ... LIKE/WHERE`. DDL now includes typed ENABLE/DISABLE KEYS and
FORCE AUTO_INCREMENT actions. All 127 original test entry points now have direct
Rust anchors, and the original `TestAstFormat` vectors pass. This is test
inventory evidence, not a parity claim. Statement restore now preserves Go's
separate failure boundary, and the checked 51,598-row integration parser corpus
has zero restore mismatches and zero false accepts. Original AST test owners no
longer use campaign `*_source` names. Root statements now carry exact source
text and origin-position metadata through the existing boxed family boundary.
The package remains open until the production-file audit, including metadata
on nested nodes populated by the Go parser, is behaviorally closed.

Go is the inventory. Go and Cargo tests are the proof. Git is the checkpoint.
