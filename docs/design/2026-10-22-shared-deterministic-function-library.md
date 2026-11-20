# A shared library for deterministic SQL functions across TiDB, TiKV and TiFlash

- Author(s): [Mattias Jonsson](https://github.com/mjonss)
- Discussion PR: https://github.com/pingcap/tidb/pull/72218
- Tracking Issue: https://github.com/pingcap/tidb/issues/71807

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
    * [Out of scope](#out-of-scope)
    * [Function groups](#function-groups)
    * [Result definition](#result-definition)
    * [MySQL-compatible functions](#mysql-compatible-functions)
    * [Build and linking per engine, and the cgo question](#build-and-linking-per-engine-and-the-cgo-question)
    * [Distribution and integrity of the archives](#distribution-and-integrity-of-the-archives)
    * [Versioning](#versioning)
    * [Compatibility](#compatibility)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

A floating-point SQL function such as `SIN`, `POW` or `LOG`, and the pattern functions
`REGEXP` and `REGEXP_LIKE`, can return different results in TiDB, TiKV and TiFlash, and
between nodes with different CPUs. This document proposes a single shared implementation of
these functions, linked into every engine, so that the result of a given function is the
same on every component and every node. Where there is a choice of implementation, it is
made for correctness and for speed: correctly rounded math, so a result is the representable
value nearest the true one and is fixed by the input alone, and a regular-expression engine
with guaranteed linear-time matching and no catastrophic backtracking.

## Motivation or Background

A TiDB cluster can compute the same expression in more than one place. TiDB evaluates it in
the SQL layer, and the execution engine can push the same function down to the TiKV or the
TiFlash coprocessor. The result should depend only on the input, but today it also depends on where
the query ran: on which engine evaluated the function, and on that node's CPU. The same query
can return one value from a TiFlash scan, another when the function is pushed to TiKV, and
another again on an arm64 node than on an x86_64 one. The result is not deterministic.

This has been reported against every component, both still open and already closed:

- **TiDB:** [#27505](https://github.com/pingcap/tidb/issues/27505) "function `exp` result is
  incorrect" (an index on `exp(a)` finds a row that the pushed-down scan does not),
  [#56777](https://github.com/pingcap/tidb/issues/56777) "After `cot()` is evaluated, the
  returned result is incorrect", [#43288](https://github.com/pingcap/tidb/issues/43288) "std
  result is not as expected on the aarch64 environment",
  [#70100](https://github.com/pingcap/tidb/issues/70100) "Indexed virtual generated columns
  can drift across session contexts and corrupt secondary indexes", and
  [#55769](https://github.com/pingcap/tidb/issues/55769) "Incorrect Result in SQL Query with
  POWER Function" (closed with a push-down-blacklist workaround).
- **TiKV:** [#19885](https://github.com/tikv/tikv/issues/19885) "[coprocessor] TiKV
  DOUBLE-to-UNSIGNED half-tie rounding drift can silently delete rows", and
  [#17934](https://github.com/tikv/tikv/issues/17934) "Floating Point Arithmetic Behavior
  Differences Between Unistore and TiKV" (closed, fixed for the signed cast only).
- **TiFlash:** [#9654](https://github.com/pingcap/tiflash/issues/9654) "Missing double
  overflow error exception in Tiflash".
- **Pattern matching:** [#48360](https://github.com/pingcap/tidb/issues/48360) "REGEXP fails
  with invalid pattern": the three engines use different regular-expression engines, so the
  same pattern and string can match in one and not in another.

Three independent causes produce the numeric divergence:

1. **Platform math library.** Each engine calls the C or Go math library of the platform it
   was built for. These libraries are not correctly rounded and differ in their last bits
   between versions and vendors.
2. **Fused multiply-add.** On arm64 the compiler may fuse a multiply and an add into one
   instruction that rounds once instead of twice, changing the result of a function that is
   written as a sequence of multiplies and adds.
3. **Different formulas.** Some functions are written differently in each engine. `DEGREES`
   has three algebraic forms across the three engines today.

The divergence outlives a single query. A function result can be stored and indexed, through
an index on a generated column, an index on an expression, a `CHECK` constraint, or a
partition key on a generated column, so a disagreement between the node that wrote a value and
the node that later reads or re-checks it can corrupt an index or drop a row. So the
requirement is one result per input, the same on every component and every CPU.

## Detailed Design

The design is one shared library of these functions with a single, well defined result per
input, linked into each engine so that all three run the same computation. The engines keep
ownership of SQL semantics around the call: NULL handling, domain checks, warnings, errors
and overflow. The shared library computes values only.

### Out of scope

The shared library makes the computed value deterministic. The rest of the work around a call
splits into three parts: classifying a result (in domain, out of domain, overflow, NaN),
turning a classification into SQL behavior (a value, a NULL, a warning or an error), and the
wording of any error. This design shares the value; it leaves the SQL behavior and the wording
to each engine. Whether it also shares the classification is an open question below. In
particular, out of scope here:

- **Turning a classification into SQL behavior.** Whether an out-of-domain input becomes a
  NULL, a warning or an error, and whether an overflow errors or truncates, depends on the
  session's SQL mode, the statement type and per-engine flags. This stays with each engine, as
  it is today.
- **Error and warning codes and wording.** A query that errors must error on every node, but
  each engine raises and words its errors differently today (TiKV prefixes its warning text,
  for example); unifying the codes and the wording is not part of this design.
- **Order-dependent aggregation** such as `SUM`, `AVG` and variance, whose result depends on
  the order rows are combined rather than on a single function value.
- **Function versioning:** which result changes need a new version, and how a cluster records,
  chooses and changes the version used by a stored expression or a statement. That is a
  separate design; this design supplies shared implementations it can choose from.
- **Regular-expression behavior changes in the engines that adopt the shared engine.** Moving
  an engine to the shared regular-expression engine can change which strings match, for
  patterns that rely on engine-specific syntax. How much change is acceptable, and whether an
  engine needs a compatibility mode, is for the engines that adopt it to decide.
- **A startup self-check.** The library can carry one like any other function, running its own
  functions over embedded test cases; whether to provide it, and what an engine does when it
  fails, is for the engines to decide.

### Function groups

The library covers these groups, named by kind rather than by delivery order:

- **Elementary math:** `SIN`, `COS`, `TAN`, `COT`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `EXP`,
  `LN`, `LOG`, `LOG2`, `LOG10`, `POW`, `SQRT`, `DEGREES`, `RADIANS`.
- **Conversions:** `CAST(double AS SIGNED/UNSIGNED)`, and `ROUND(x, d)` and `TRUNCATE(x, d)`
  on doubles.
- **Pattern matching:** `REGEXP`, `REGEXP_LIKE`, `REGEXP_INSTR`, `REGEXP_SUBSTR`,
  `REGEXP_REPLACE`, on one shared regular-expression engine.
- **Geospatial:** the distance helpers, where that feature needs them.

This document is a framework for giving a cross-engine function one result everywhere; it does
not prescribe which groups or functions are delivered first, or in how many steps. The same
framework extends to other behavior that diverges across engines; string collations
(comparison and sorting) are the natural next candidate and are noted as a future addition.

### Result definition

The library's functions are **correctly rounded**: a result is the representable double
nearest the exact mathematical value. It is defined by the input alone, so it does not depend
on the CPU, the compiler or the math-library version, and that is the property that makes the
bits identical everywhere by construction. (`SQRT` is already a single IEEE operation and
identical everywhere; the library carries it for completeness and as a guard against
regression.) The library returns the computed value, including an infinity or a NaN
for an out-of-domain input; mapping that to a SQL value, NULL, warning or error is the
engine's job, as it is today.

A caller must not combine a result with further floating-point arithmetic that its compiler
could fuse, as that would reintroduce a per-CPU difference. Any such composition is done
inside the shared library.

### MySQL-compatible functions

A few functions return a value that differs from the correctly rounded one because MySQL
computes them with a particular algebraic formula rather than to the nearest representable
value: `COT` as `1/tan`, `DEGREES` and `RADIANS` as a multiply by a rounded constant, and the
two-argument `LOG(base, x)` as `ln(x)/ln(base)`. Some edge-case results are MySQL choices as
well, for example whether an out-of-domain input yields an error, a NULL or a NaN.

The library can carry a distinct MySQL-compatible function (for example a MySQL `COT`) next to
the correctly rounded one, each deterministic on every engine and CPU. Which variants it
carries, and which one an engine uses, are decided outside this design.

### Build and linking per engine, and the cgo question

One library, three link paths, because the three engines are written in three languages:

- **TiKV (Rust)** depends on the library as an ordinary native dependency and compiles it
  from source with the rest of TiKV. No foreign-function boundary is involved.
- **TiFlash (C++)** links the library through its C interface.
- **TiDB (Go)** links a prebuilt static archive through cgo.

Using cgo in TiDB is acceptable. The shipped `tidb-server` is already built with cgo enabled
(`CGO_ENABLED=1`); the production build turns it on deliberately for performance and links
C code today. So linking one more archive does not change TiDB's build model. The archive is
prebuilt per platform and shipped inside a Go module, so TiDB's build pins its hash and no
Rust toolchain is required to build TiDB; only changing the library requires one.

Some TiDB build paths set `CGO_ENABLED=0`. These are **compile-only or do-not-evaluate**
paths: the backup and restore tool on non-macOS and Windows, the FreeBSD cross-compile, and
the static-analysis compile check. They import the package transitively but never evaluate
these functions. For them the package provides a **fail-closed** implementation that compiles
and panics if ever called. It deliberately does not fall back to the Go standard math
library, because the standard library is not bit-identical and would silently return results
that disagree with the rest of the cluster, which is exactly the problem this design exists to
remove. A bit-identical no-cgo implementation (the library translated to pure Go) is possible
in principle and can be added later if a no-cgo binary ever has to evaluate these functions;
nothing requires it today.

Under Bazel the archive is provided as a declared build input rather than through link flags,
so the build is hermetic and the archive's bits are checked.

### Distribution and integrity of the archives

For TiDB the per-platform archives are committed inside a versioned Go module, one per
platform that evaluates the library in TiDB: Linux and macOS, each on amd64 and arm64, so
four per library release. The no-cgo build paths (Windows, and the FreeBSD cross-compile) use the
fail-closed stub and need no archive. Go records a cryptographic hash of every module it pulls into the
consuming project's `go.sum`, so the exact archive bits are pinned and any change is caught at
build time; a loose build artifact could not be pinned this way, which is why the archive
lives inside a module. Under Bazel the archive is a declared input with a recorded checksum.
TiKV compiles the library from source pinned by its dependency lock, and TiFlash links the
archive from the library's own build. A missing or mismatched archive fails the build; there
is no fallback to another implementation. The library's build is reproducible, so the same
source yields the same bits on any builder, and it publishes a results identity (a hash of the
library's output over a fixed corpus) that a build or test can compare to confirm two archives
compute the same results.

### Versioning

A result must only change deliberately, never as a side effect of rebuilding on a new machine,
compiler or math-library version. The library provides what that needs from the computation:

- **Reproducible results.** A given build of the library gives the same result for a function
  on every engine and every CPU, and every component of a release links the same build, pinned
  by the cluster version.
- **Detectable changes.** The library adds tests, a fixed corpus and a results identity, that
  reasonably identify result changes, so a change is not silent.
- **Several versions side by side.** The library can provide more than one implementation of a
  function at a time.

Function versioning, a separate design, decides which result changes need a new version, which
version a stored expression or a statement uses, and when a cluster moves to a new one. It is
needed without this library: session settings, toolchain upgrades and bug fixes already change
the results behind stored expressions (#70100).

### Compatibility

- **Stored and indexed derived values.** Indexes on generated columns, indexes on
  expressions, `CHECK` constraints and partition keys on generated columns persist computed
  values. Which version each one uses is decided by function versioning (see Versioning).
- **Internal components:** parser and planner are unaffected; the executor and the coprocessor
  pushdown path change their function implementation but not their interfaces.
- **External components:** TiKV and TiFlash link the same library; BR restores data without
  recomputing these functions; a MySQL or TiDB downstream of TiCDC recomputes generated
  columns and their index entries, so it must use the same function versions. PD is
  unaffected.
- **Upgrade and downgrade:** handled by function versioning (see Versioning).

## Test Design

### Functional Tests

- A per-function corpus of inputs (ordinary values, domain boundaries, denormals, infinities,
  NaN, out-of-domain inputs) with expected bit-exact outputs, run as a unit test in each engine
  and as a language-neutral data set shared by all three.
- Existing expression unit tests whose expected values were generated by the Go standard math
  library are regenerated against the shared library, since the shared library is the oracle.

### Scenario Tests

- A three-engine cluster running the same queries through TiDB, through TiKV pushdown and
  through TiFlash, asserting identical bits for every in-scope function.
- The same run repeated on x86_64 Intel, x86_64 AMD, and arm64 nodes, and on a CPU without
  fused multiply-add, asserting the results do not change with the hardware.

### Compatibility Tests

- Indexes on generated columns and on expressions over these functions, `CHECK` constraints,
  and partition keys on generated columns: written before and read and re-checked after an
  engine switches a function to its shared implementation.
- A rolling upgrade with mixed versions, exercising pushdown of an in-scope function while the
  cluster is mixed.
- Backup and restore, and change-data-capture, of tables holding such derived values.

### Benchmark Tests

- The cost of the foreign-function boundary in TiDB for a single scalar call, and its
  amortized cost under vectorized evaluation, where TiDB evaluates a whole chunk (its internal
  columnar batch of rows) with one call across the boundary.
- Standard online workloads (for example point-select, read-only and read-write mixes) to
  confirm no material throughput change.
- Function-heavy scans for the covered functions, per engine.

Early measurements: the boundary costs on the order of tens of nanoseconds per scalar call,
and amortizes toward zero under vectorized evaluation, where one boundary call processes a
whole chunk (TiDB's internal batch of rows, by default on the order of a thousand); standard
mixed workloads move within noise; the server binary grows by a few megabytes.

## Impacts & Risks

**Impacts (intended):**

- Identical results for the covered functions across engines and CPUs, which is the goal.
- More functions can be pushed down. Because a function now computes the same result
  everywhere, more functions become safe to push down to TiKV and TiFlash, which evaluate them
  natively with no foreign-function boundary. The cgo cost applies only when `tidb-server`
  evaluates a function itself; pushing the work down removes even that, so the change can raise
  total cluster throughput rather than cost it.
- A small, measured increase in server binary size, and a small per-call cost at the TiDB
  boundary when `tidb-server` evaluates a function itself, amortized by vectorized,
  chunk-at-a-time evaluation and avoided entirely where the function is pushed down.

**Risks (unintended, may or may not happen):**

- **Result changes invalidating persisted derived values.** Addressed by function versioning,
  a separate design (see Versioning).
- **Mixed-version push-down.** During a rolling upgrade a function could be evaluated at one
  version on one node and another version on another. Avoiding that is up to the engines that
  use the library, through function versioning (see Versioning).
- **Per-platform archive supply.** The Go module ships four archives per library release
  (Linux and macOS, each amd64 and arm64; see Distribution). Each must be built, checksummed
  and present, and a missing or mismatched one fails the build rather than falling back.

## Investigation & Alternatives

- **Fix each engine independently.** Rejected: it does not converge. Three engines on three
  platform math libraries keep three results, and the per-formula differences (three forms of
  `DEGREES`) remain.
- **Compile the math from C source inside each engine's build.** Rejected: the floating-point
  contraction flags that would be needed cannot be pinned reliably as package build flags, so
  the resulting bits would depend on whoever built the engine. Building the shared source once,
  in one place, fixes the bits.
- **A pure-Go reimplementation for TiDB.** Possible as a no-cgo fallback, kept behind the same
  interface and checked against the shared corpus, but not needed for the engines that
  evaluate these functions, all of which build with their foreign-function support enabled.

Implementation detail, for reference rather than part of the requirement: the correctly
rounded elementary functions come from published correctly-rounded implementations (the
CORE-MATH project and the LLVM libc), chosen per function for speed; the library is built in
Rust so the compiler pins the floating-point behavior, and ships as a C archive plus a Go
module of prebuilt archives. Prior art for packaging a prebuilt native archive inside a Go
module exists (for example DuckDB's Go bindings), as does a Rust binding to CORE-MATH.

## Unresolved Questions

- Whether the shared library also owns the classification of a result (in domain, out of
  domain, overflow, NaN), returning it alongside the value so the value, NULL and error choice
  is deterministic by construction, or whether each engine keeps classifying as it does today.
  Owning it removes a per-engine source of divergence but adds a status to every function's
  interface; the SQL-mode-dependent policy and the error wording stay with each engine either
  way.
- Where the shared library lives (repository, owner, organization, visibility) and its name
  and license. Published names are permanent, so this is decided before anything is published.
