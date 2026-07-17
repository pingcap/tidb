# Auto-Embedding Planner Provenance and Resolution

- Author(s): [Yiding Cui](https://github.com/winoros)
- Discussion PR: [pingcap/tidb#69014](https://github.com/pingcap/tidb/pull/69014)
- Tracking Issue: TBD after design acceptance

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Goals](#goals)
* [Non-Goals](#non-goals)
* [Detailed Design](#detailed-design)
  * [Catalog Metadata and Search Options](#catalog-metadata-and-search-options)
  * [Execution Equivalence and Security Context](#execution-equivalence-and-security-context)
  * [Bindings, Resolution Algebra, and Provenance Barrier](#bindings-resolution-algebra-and-provenance-barrier)
  * [Operator and JOIN Policy](#operator-and-join-policy)
  * [INSERT Binding and Snapshot](#insert-binding-and-snapshot)
  * [Rewrite and NULL Semantics](#rewrite-and-null-semantics)
  * [Optimizer, Cache, and Errors](#optimizer-cache-and-errors)
* [Compatibility](#compatibility)
* [Performance](#performance)
* [Test Design](#test-design)
* [Observability and Maintenance](#observability-and-maintenance)
* [Impacts and Risks](#impacts-and-risks)
* [Investigation and Alternatives](#investigation-and-alternatives)
* [Rollout](#rollout)
* [Unresolved Questions](#unresolved-questions)

## Introduction

TiDB auto-embedding columns store vectors produced by `EMBED_TEXT()`. The `VEC_EMBED_*_DISTANCE()` functions shall let users search such columns with text without repeating catalog model and options.

Before lowering a convenience function, TiDB must bind its first argument to one logical value and prove every possible non-NULL source of that value. The proof shall use validated catalog metadata, a closed operator policy, and a tree-local provenance barrier. Derived, ambiguous, conflicting, or unknown lineage shall fail closed.

## Motivation or Background

This provider-neutral example uses illustrative provider, model, and option names. A deployment must substitute a configured provider whose output dimension matches the vector type.

```sql
CREATE TABLE documents (
    id BIGINT PRIMARY KEY,
    body TEXT NOT NULL,
    embedding VECTOR(3) GENERATED ALWAYS AS (
        EMBED_TEXT(
            'example-provider/example-model',
            body,
            '{"mode":"index","mode@search":"query"}'
        )
    ) STORED
);

SELECT id
FROM documents
ORDER BY VEC_EMBED_L2_DISTANCE(embedding, 'distributed SQL')
LIMIT 10;
```

At the provenance barrier, the planner shall derive effective search options `{"mode":"query"}` from the static catalog options `{"mode":"index","mode@search":"query"}`. It then lowers the convenience call to an ordinary distance expression containing `EMBED_TEXT()` with the canonical model, original search expression, and the effective options as an ordinary string argument.

A user-written fallback spells the same effective options explicitly:

```sql
SELECT id
FROM documents
ORDER BY VEC_L2_DISTANCE(
    embedding,
    EMBED_TEXT(
        'example-provider/example-model',
        'distributed SQL',
        '{"mode":"query"}'
    )
)
LIMIT 10;
```

The synthesized call uses `'distributed SQL'`, not the generated column's source expression `body`. The same mapping applies to all four functions:

| Convenience function | Ordinary distance function |
| --- | --- |
| `VEC_EMBED_L1_DISTANCE` | `VEC_L1_DISTANCE` |
| `VEC_EMBED_L2_DISTANCE` | `VEC_L2_DISTANCE` |
| `VEC_EMBED_COSINE_DISTANCE` | `VEC_COSINE_DISTANCE` |
| `VEC_EMBED_NEGATIVE_INNER_PRODUCT` | `VEC_NEGATIVE_INNER_PRODUCT` |

A bare alias may preserve provenance; a cast or other derived expression shall not:

```sql
SELECT VEC_EMBED_L2_DISTANCE(e, 'distributed SQL')
FROM (SELECT embedding AS e FROM documents) AS d;

SELECT VEC_EMBED_L2_DISTANCE(CAST(embedding AS VECTOR(3)), 'distributed SQL')
FROM documents;
```

## Goals

- Prove every possible non-NULL source path for one unambiguous consumer binding, including multi-branch consensus.
- Define deterministic model parsing and one-level `@search` option transformation.
- Make convenience lowering use the same authorization and expression-execution path as the equivalent explicit call.
- Preserve approved SELECT, DML, view, subquery, CTE, JOIN, union, and INSERT bindings without relying on runtime schema layout.
- Consume provenance before source-changing transformations and keep proofs operation-local.
- Keep nested-tree summaries, INSERT transfers, optimizer interaction, and cache behavior operation-local and fail closed.

## Non-Goals

- General-purpose lineage or proof of arbitrary expression equivalence.
- DDL-time validation of provider registration, credentials, model existence, or returned-vector dimension.
- JSON canonicalization for metadata consensus.
- New SQL privileges, principal-selection rules, provider billing/accounting, quota units, or credential management.
- Unrelated DDL or table-kind restrictions beyond validity of an auto-embedding root definition; provider-contract changes for dynamic explicit options; deployment gates, cache epochs, or distributed configuration protocols.
- Optimizer-output resolution or persistent provenance identifiers.

## Detailed Design

### Catalog Metadata and Search Options

The resolver consumes an immutable metadata value containing a canonical provider/model identifier and the accepted raw options string. The value is copied from catalog metadata, compared by those two fields, and never owns credentials, expression objects, runtime columns, or provider state. Its programming-language representation is an implementation choice.

An auto-embedding root candidate is observable: its column definition has the auto-embedding root shape, or catalog metadata marks it as an auto-embedding root. A valid root must be a public, visible, stored generated vector column whose top-level expression is direct `EMBED_TEXT()` with a string-literal model, valid optional string-literal options, and no dependency on another auto-embedding column. Every newly created or actually modified candidate must pass the model and strict options parser before publication.

#### Model identifier

The first slash separates provider from model. Both components are trimmed and must be non-empty. Provider is case-insensitive and canonical lowercase; model is case-sensitive after trimming; later slashes belong to model.

Thus ` Provider /model ` equals `provider/model`, while `provider/Model` differs from `provider/model`. Empty strings, missing slash, empty provider, and empty model are invalid static metadata. DDL and catalog-root validation shall share this parser. Provider registration, credentials, and model existence remain runtime checks.

#### Options grammar and transformation

Strict search-option parsing applies only to static options stored for an auto-embedding catalog root. It shall not change the provider contract for an ordinary explicit `EMBED_TEXT()` whose options are evaluated dynamically.

For every newly created or actually modified root candidate, parse static options in this order:

1. Accept omission, `''`, or a JSON object including `{}`; reject SQL NULL, JSON `null`, arrays, scalars, malformed JSON, and duplicate member names at any object depth.
2. Inspect exact top-level keys without normalization; reject an empty key, empty override base, and repeated suffix chains such as `x@search@search`.
3. Treat only a final, single `@search` suffix as reserved. A key containing `@search` elsewhere remains provider-defined.
4. Build materialization and search objects from the immutable parsed object, then reject any derived-key collision except exactly one ordinary key `x` paired with exactly one override `x@search`.

Transformation uses an immutable input object:

- materialization copies normal members and removes search overrides;
- search copies normal members, then applies every one-level override to its base key;
- an override deterministically wins over its base value, or creates the base key when none exists;
- input member order and map iteration order do not affect the derived key/value mapping; and
- a second search transformation produces the same object.

Search derivation also emits a deterministic compact JSON string for the synthesized ordinary argument. The parser retains accepted top-level member order. A search override replaces its existing base value at the base member's position, or occupies the override member's position when no base exists; remaining members retain their relative order. Nested values retain their parsed order. This ordered two-pass encoding is linear in input bytes and does not depend on map iteration.

Omitted or `''` catalog options cause the synthesized `EMBED_TEXT()` to omit its third argument. An accepted object, including `{}`, produces a third-argument string such as `'{}'`. The derived string contains no `@search` members.

An ordinary explicit `EMBED_TEXT()` with raw catalog options does not apply search derivation. It follows the ordinary materialization rule: normal members are used and `@search` override members are not applied. Only convenience lowering derives the effective search argument at the provenance barrier.

Metadata consensus still compares the raw accepted options string. Different whitespace, member order, or spelling does not form consensus even when effective maps are equal.

### Execution Equivalence and Security Context

Convenience lowering is planner synthesis, not a new provider-capable execution path. The synthesized `EMBED_TEXT()` shall use the same authorization, deployment eligibility, provider selection, quota/accounting, cache, singleflight, batching, cancellation, error, and redaction behavior as the equivalent explicit expression in the same SQL context.

The planner shall not capture, promote, cache, or replace the effective principal or security context. View definer/invoker behavior, generated-column materialization, restore/import execution, and other context selection remain governed by existing SQL and expression rules. No privilege, grant migration, billing ledger, or accounting unit is introduced by this design.

Catalog model/options visibility remains the visibility of the stored generated expression. Synthesized constants and search text follow the existing plan-display and literal-redaction rules for the equivalent explicit expression. The ordinary expression's visible arguments completely determine its provider-options semantics.

### Bindings, Resolution Algebra, and Provenance Barrier

Name resolution shall assign each provenance-sensitive reference an opaque, operation-local `ValueBinding`: namespace, stable producer/output identity, vector type, and INSERT role when applicable. It is not a catalog ID, global counter, runtime slot, or cross-statement token.

| State | Meaning |
| --- | --- |
| Outside | The binding is not in this namespace. |
| Unproven | The namespace owns or may own the binding, but complete sound proof is unavailable. |
| Proven | One unambiguous consumer binding has all possible non-NULL source paths proved, vector-type compatible, and metadata-equal. |

Proven may contain multiple source paths. Unproven stops namespace fallback.

`VectorTypeCompatible(left, right)` is the single compatibility relation for every transfer and consensus edge. It compares the non-NULL vector domains: both sides must have the same vector type code, declared dimension, and every other property that affects non-NULL values or their representation. It ignores, and may ignore only, nullability or an equivalent not-NULL property. Injecting SQL NULL changes nullability but creates no additional non-NULL provenance.

A query block is one SQL name-resolution scope. A logical tree is the rooted relational structure that the planner optimizes as one unit; one query block may own independently optimized child trees for subqueries or reusable definitions. Every such tree crosses its own provenance barrier, so no statement-global optimizer ordering is required.

For each tree, the planner shall:

1. resolve names and assign semantic input/output bindings;
2. complete operator ownership, JOIN groups, SQL set-operation category, and required DML bindings;
3. bind convenience calls without resolving their provenance;
4. resolve all consumers in one shared context and freeze the tree's output summary; then
5. derive effective search options and rewrite Proven calls before any provenance-altering transformation or optimization of that tree.

A frozen output summary is operation-local and maps each exported child semantic output to its complete vector type and either Unproven or copied Proven metadata; an absent output is Outside. It contains no child plan node, optimizer field, runtime slot, persistent identifier, or reference to mutable proof state. The parent first freezes the child, then creates parent-namespace bindings by explicit child output position, and finally crosses the parent barrier. Missing, duplicate, derived, or incompatible mappings are Unproven; the parent never reopens an optimized child.

Cross-tree transfer is closed-world:

| Boundary | Phase 1 rule |
| --- | --- |
| Derived table or expanded view | Transfer a bare, value-preserving, type-compatible output by resolved position. |
| Non-recursive CTE | Freeze the definition once; each reference creates fresh parent bindings from the same immutable positional summary. |
| Scalar subquery | Transfer only its single result when it is child-owned, bare, and independent of outer bindings. |
| Correlated/lateral child output | Unproven when the result depends on an outer namespace; ownership remains with the enclosing Apply when represented in one tree. |
| Recursive CTE, incomplete child summary, or unknown boundary | Unproven. |
| `EXISTS`, `IN`, or another predicate-only subquery | Export no vector value provenance. |

Pre-barrier transformations are closed-world:

| Class | Requirement |
| --- | --- |
| Binding-preserving | Runtime value and binding stay unchanged. |
| Binding-remapping | Runtime value is preserved and a total old-to-new binding map is emitted. |
| Provenance-altering | Source erasure, merge, synthesis, or replacement must wait until the proof is frozen. |

Unknown pre-barrier transformations fail closed. After the barrier, optimizers may transform explicit expressions, but no proof may be reconstructed from memo groups, physical plans, optimizer clones, or cached plans.

### Operator and JOIN Policy

| Shape | Policy |
| --- | --- |
| Catalog root | Validate final metadata. |
| Projection | Pass only one bare compatible bound column; reject constants, casts, and scalar expressions. |
| Selection, Sort, TopN, Limit, Lock, UnionScan, MaxOneRow | Transfer the child binding; MaxOneRow may change only nullability. |
| Window | Pass input columns only; window results are derived. |
| Aggregation | Pass only a bare, type-compatible grouping key; aggregate and derived outputs are Unproven. |
| Expand/rollup, recursive CTE, table dual, cycles, unknown operators | Reject. |
| SQL set operation | Apply the SQL-level matrix below before considering its logical lowering. |
| Partition expansion | Require every positional branch to be Proven, type-compatible, and metadata-equal. |
| Child-tree boundary | Consume only the frozen output summary defined above; sequence uses only its result child. |
| JOIN/Apply | Use ordinary ownership or the common-column rule below. |

SQL-level set and duplicate-elimination policy is:

| SQL shape | Phase 1 decision | Value-source reasoning |
| --- | --- | --- |
| `UNION ALL` | Allow positional consensus. | Each result comes from one branch, so every branch must be Proven with equal metadata and compatible type. |
| `UNION DISTINCT` | Reject as a conservative product limit. | It composes branch consensus with duplicate elimination; Phase 1 does not carry value ownership through both merge stages. |
| `INTERSECT` or `INTERSECT ALL` | Reject as a conservative product limit. | Result membership depends on both inputs and Phase 1 does not define cross-input value ownership. |
| `EXCEPT` or `EXCEPT ALL` | Reject as a conservative product limit. | Although surviving values originate from the left input, Phase 1 does not prove ownership across set subtraction. |
| `SELECT DISTINCT` | Allow a bare compatible output. | Duplicate elimination filters equal values without changing the selected value. |
| `GROUP BY` | Allow a bare compatible grouping key only. | A grouping key is selected from input values; aggregates and derived expressions are new values. |

If lowering erases the SQL category, it shall attach an abstract origin discriminator that preserves this decision until the barrier. Its lifetime ends at the barrier; no particular plan field, clone rule, or optimizer hash/equality behavior is part of this design.

For a non-common JOIN output, exactly one child must own the binding. SQL common-column value sources are:

| Join type | Unqualified common column | Left-qualified | Right-qualified |
| --- | --- | --- | --- |
| INNER | Canonical left value; matched values satisfy equality. | Left value. | Right value. |
| LEFT OUTER | Canonical left value. | Left value. | Right value or NULL. |
| RIGHT OUTER | Canonical right value. | Left value or NULL. | Right value. |

Phase 1 nevertheless requires complete metadata/type consensus across the entire `USING`/`NATURAL` group for canonical and qualified references. This is a conservative product restriction, not SQL value-source semantics. It prevents qualification from bypassing a conflict. Users can fall back to explicit ordinary distance plus effective `EMBED_TEXT()` options.

Group evidence must identify one canonical binding and every qualified member. Missing evidence, an ordinary vector member, conflicting metadata, or dimension mismatch is Unproven. SQL NULL from an outer JOIN is not another vector provenance.

Apply follows the same ownership/group rules. An inner namespace cannot serve as fallback for an outer-owned or Apply-owned binding.

### INSERT Binding and Snapshot

INSERT name resolution shall create four disjoint binding domains:

| Domain | Input domain and ownership |
| --- | --- |
| Statement result output | A visible output of the INSERT source query, identified by its resolved semantic output position. |
| Resolution-only extra | A source value exposed only so an ON DUPLICATE expression can resolve a name; it is not a visible source result or target assignment. |
| Target-column assignment | One target column paired by the INSERT column/assignment relation with its resolved input expression, default, or generated expression. |
| Inserted/new-row value | The post-assignment value that SQL exposes as the proposed row in ON DUPLICATE evaluation. |

Statement result outputs and resolution-only extras exist only when the INSERT has a logical source tree. Before optimizing an INSERT SELECT source, that tree crosses its barrier and freezes separate summary entries for those two domains. VALUES, SET, DEFAULT, and empty-row forms have no source logical tree, so source-plan provenance and frozen source summaries do not apply.

The SQL-shape policy is:

| INSERT shape or reference | Provenance source |
| --- | --- |
| INSERT SELECT | Map the frozen source result and resolution-only-extra domains through explicit resolved positions. A source value can reach a target assignment and new-row value only through the transfer rules below. |
| Single-row or multi-row VALUES | Explicit or ordinary vector assignments are Unproven. An omitted target value generated by a valid auto-embedding root uses that one target definition for every row. |
| INSERT SET | Treat each assignment like one VALUES row; explicit or ordinary vector assignments are Unproven, while an auto-generated target value may use its valid target definition. |
| DEFAULT or empty row | An ordinary default vector value is Unproven. A value generated from a valid auto-embedding target definition uses that definition. |
| ON DUPLICATE existing-target reference | Resolve the current target value independently through the target catalog binding; never fall back to a source or new-row binding. |
| ON DUPLICATE inserted/new-row reference | Use the shape-specific new-row binding: an eligible INSERT SELECT transfer or target-generated value may be Proven; an explicit, ordinary, default, ambiguous, or incompatible assignment is Unproven. |

Normal generated-column assignability remains authoritative; this design does not make a generated column directly assignable. In multi-row VALUES, every auto-generated target value has the same metadata because one target generated definition owns all rows. Full-row consensus is required only if a shape exposes multiple independent Proven producers for one inserted-value binding; Phase 1 VALUES, SET, DEFAULT, and empty-row forms expose no such producers.

The INSERT transfer relation shall be total: every binding in every applicable domain maps to exactly one semantic producer or to explicit Unproven.

- Child summaries map to source outputs and extras only through explicit resolved positions; extras never participate in target ordinal matching.
- Source results map to target assignments only through the resolved INSERT column/assignment relation.
- Target assignments map to inserted/new-row values only through the corresponding assignment relation.
- A target column used as the existing row resolves independently through the catalog-root rule and never as fallback for a source-owned binding.

A transfer preserves Proven only when it is a bare value-preserving reference and `VectorTypeCompatible` holds. An implicit cast, coercion, dimension change, derived or ordinary default expression, ambiguous producer, missing map entry, or incompatible type is Unproven. A target-generated assignment may establish provenance only through its valid catalog-root definition, not by source fallback.

Optimization may reorder, clone, or eliminate runtime slots but cannot alter these domains or transfers. Resolution-only extras cannot become visible results, target assignments, or inserted values. No binding or summary survives the statement.

### Rewrite and NULL Semantics

At the barrier, each convenience call shall require two arguments and one unambiguous vector binding. For a Proven call, the planner derives the effective search options from the copied static catalog metadata and builds the corresponding ordinary distance with `EMBED_TEXT(canonical_model, original_search_expression, effective_options_literal)`. Omitted or `''` catalog options omit the third argument; an accepted object supplies its deterministic derived JSON string. Outside or Unproven returns the common first-argument error without fallback.

The convenience expression must equal a user-written explicit expression using transformed effective search options. Once synthesized, it follows the ordinary expression path without a convenience-specific authorization, quota, provider, cache, singleflight, batching, or accounting branch.

| Input/event | SQL result/error | Provider observability |
| --- | --- | --- |
| Vector is NULL | Distance is NULL. | Query embedding may still run; argument order and batching are not guaranteed. |
| Search text is or converts to NULL | Embedding and distance are NULL. | That NULL value is not sent; other batch rows may run. |
| Search conversion succeeds | Provider receives the converted string. | Scalar/vectorized batching may differ. |
| Search conversion fails | Return conversion error. | Failing value is not sent; prior work follows cancellation rules. |
| Explicit options omitted, SQL NULL, or `''`; catalog options omitted or `''` | No provider options. | No convenience-specific difference. |
| Provider/runtime validation fails | Return the same error contract as explicit `EMBED_TEXT()`. | Error may be observable even when vector is NULL. |

Scalar and vectorized evaluation must agree on values, NULLs, and error categories, not provider call count/order/cache hits. Provider calls occur only during evaluation, never provenance resolution.

### Optimizer, Cache, and Errors

Proof state, frozen summaries, and INSERT transfers never enter prepared, non-prepared, or instance caches. The rewrite result is an ordinary expression whose function name, type, and arguments fully determine semantics. Clone, equality, hash, serialization, plan identity, session isolation, and schema dependency therefore follow existing expression and plan-cache contracts without extending expression identity. Cache reuse must not skip execution-time checks that the equivalent explicit expression performs.

An explicit raw-options call and a convenience-derived effective-options call are different expressions when their visible third arguments differ. Calls with identical visible arguments have identical provider-options semantics. Byte-identical derived option literals may share expression identity; byte-different literals need not compare equal even when their JSON objects are provider-equivalent.

This design reuses the existing Starter deployment-mode eligibility for auto-embedding. It introduces no system variable, dynamic kill switch, feature epoch, cache-invalidation protocol, or cross-node `SET GLOBAL` synchronization. Any configuration dependency of a rewritten plan is handled by the same existing plan-cache rules as its explicit equivalent.

Unproven first arguments use one public template for all four convenience functions:

```text
<convenience-function>() first argument must be a vector embedding column generated by EMBED_TEXT(), got <argument>
```

`<convenience-function>` is the invoked SQL function name. `<argument>` is a stable resolved user-facing column name when one is safely available; otherwise it is the fixed word `expression`. The error shall not expose optimizer identifiers, internal plan structure, or serialized expression internals.

Wrong arity uses normal function errors. Invalid static metadata is rejected when an operation creates or actually modifies an auto-embedding root candidate; a grandfathered root that cannot satisfy the contract is Unproven for convenience only. Provider registration, credentials, model existence, cancellation, quota, and output validation follow the existing explicit-expression runtime errors.

## Compatibility

Catalog/DDL validity and resolver eligibility are separate layers. The auto-embedding column capability owns whether a table definition, table kind, generated expression, materialization, or DDL operation is legal. Within that capability, passing this design's model/options parser is a necessary validity condition for every newly created or actually modified observable root candidate. The resolver separately decides whether a convenience first argument can be proved from a legal or grandfathered root.

An unsupported root or plan shape is Unproven for convenience. That result shall not disable generated-column materialization, ordinary stored-vector reads, explicit vector operations, or unrelated DDL.

| Operation class | Metadata responsibility | Resolver consequence |
| --- | --- | --- |
| CREATE or ALTER that creates or actually changes a root candidate | Existing capability rules decide other legality; before publication, the candidate's static model/options must pass this design's deterministic parser. | Newly admitted metadata can be a root; rejected metadata is not published. |
| CREATE TABLE LIKE or logical restore | Preserve raw metadata according to the operation contract; each newly created root candidate must pass the same creation-time parser. | Eligibility depends on the resulting metadata, not on syntax-specific planner state. |
| Metadata-preserving physical restore | Preserve catalog metadata without provider calls or provenance backfill. | Treat restored roots like grandfathered roots and validate only when resolving convenience. |
| IMPORT or another data-recomputing path | Use existing generated-expression execution and operation security context. If the operation also creates or modifies a root candidate, validate that candidate before publication. | Pure data recomputation does not reinterpret metadata. |
| Unrelated ALTER, temporary table, or partitioned table | No unrelated prohibition or blanket revalidation from this design; an operation that does not actually modify the root keeps it grandfathered. | A later convenience query proves or rejects its particular root and plan normally. |
| Legacy root | Keep existing DDL, materialization, and reads grandfathered. | If static metadata cannot satisfy the model/options contract, return Unproven without backfill or catalog mutation. |

Static metadata validation excludes provider registration, credentials, model existence, and returned-vector values. No persistent provenance or wire field is added. BR, TiCDC, Dumpling, PD, TiKV, and TiFlash protocols are unaffected by provenance.

## Performance

Before resolution begins, enumerate the distinct reachable entities for one barrier: `P` plan nodes, `K` semantic bindings and inspected schema/summary entries, `T` operator, boundary, snapshot, and INSERT transfer edges, `Q` coalescence and equality/name evidence entries, `S` set-operation branch entries, and `C` convenience consumers. Let `N = P + K + T + Q + S + C`. Recursive depth is bounded by reachable plan nodes and does not redefine the input after execution.

Before resolution, let `B` be the sum of byte lengths of all distinct reachable metadata values, including canonical model identifiers and raw options. One shared context shall index evidence once and memoize each reachable state. Each distinct entity may be visited only a constant number of times, and bytes parsed, copied, encoded, hashed, or compared in each category must be bounded by a constant multiple of `B`. Time and worst-case auxiliary space, including memo state, schema and evidence indexes, summaries, transfer maps, derived option literals, metadata copies, and recursion stack, shall be `O(N + B)`. An implementation may reduce auxiliary bytes by referencing immutable input metadata, but all constructed indexes and copies still count toward the bound. Recursive-call count is not an input-size measure.

Before implementation begins, the Tracking Issue shall freeze a benchmark gate containing:

- workloads independently scaling plan nodes, consumers, bindings/schema entries, transfer edges, set branches, JOIN/coalescence/equality evidence, child summaries, INSERT transfers, and model/options byte length;
- the semantically equivalent explicit-expression baseline;
- hardware, runtime configuration, planner mode, warmup, repetitions, measured statistics, and numerical time/allocation budgets; and
- a no-provider-I/O setup plus counters for visits to every entity class and for metadata bytes parsed, copied, encoded, hashed, and compared.

Enablement passes only when every frozen workload meets its recorded numerical budget, each distinct-entity counter grows linearly with its precomputed input count, and metadata-byte counters grow linearly with `B`. The design does not claim a measured percentage before that protocol exists.

## Test Design

Tests are prospective acceptance criteria.

| Contract layer | Property | Required positive/negative inputs | Pass criteria |
| --- | --- | --- | --- |
| Static metadata/options | Catalog search derivation is deterministic without changing explicit provider options. | Model whitespace/case/slashes; omitted/empty/object options; malformed/non-object JSON; duplicate nested/top-level keys; empty keys/bases; base+override; suffix chains; derived collisions; member-order and whitespace permutations; repeated derivation. Feed the same option cases to explicit `EMBED_TEXT()` as a literal, parameter, and column/dynamic expression. | Every new or modified root candidate follows the strict parser; accepted maps are order-independent and idempotent; the same accepted catalog string always emits the same compact effective literal; key-order variants produce equivalent provider objects even if their literals differ. Literal, parameter, and dynamic explicit calls retain ordinary non-search behavior. |
| Catalog/DDL boundary | Root validity and resolver eligibility remain separate. | CREATE or actually root-changing ALTER, LIKE, logical/physical restore, recomputing import, unrelated ALTER, temporary/partitioned tables, and valid/invalid legacy roots. | Every newly created or modified observable candidate passes strict parsing before publication; grandfathering and metadata preservation match the compatibility table; unsupported provenance adds no backfill, provider call, unrelated-DDL failure, table-kind ban, or stored-vector read failure. |
| Barrier and tree boundary | Each independently optimized tree freezes complete semantic outputs before parent binding. | Outside/Unproven/Proven; derived/view/CTE/scalar boundaries; multiple CTE references; correlated/lateral, recursive, predicate-only, missing, duplicate, and incompatible summaries; source-altering transforms. | Child summary is immutable and operation-local; parent maps by resolved output position; opaque cases are Unproven; proof freezes before optimization; no parent inspection or post-optimizer reconstruction occurs. |
| Vector compatibility | One relation governs every transfer and consensus edge. | Equal vector domains with equal/different nullability; different vector type codes, dimensions, or other non-NULL value-domain/representation properties; outer-JOIN NULL injection. | Only nullability differences are ignored; every other difference is incompatible; NULL injection creates no non-NULL provenance; all operator, boundary, JOIN, set, and INSERT paths produce the same decision. |
| Operators and SQL set policy | Value-source composition follows the closed operator and SQL-level matrices. | All operator rows; UNION ALL/DISTINCT, INTERSECT [ALL], EXCEPT [ALL], SELECT DISTINCT, GROUP BY; compatible/conflicting branches and lowering shapes. | Allowed shapes require complete type/metadata consensus; conservative product limits reject consistently before lowering details can change the decision; unknown shapes fail closed. |
| Rewrite, NULL, errors, and execution context | Convenience equals explicit effective-options SQL in the same context. | All four functions in SELECT/WHERE/ORDER/DML/view/subquery/CTE/prepared contexts; nullable vector; NULL/converting/error text; empty/NULL explicit options; named columns and non-column first arguments; existing security contexts, quota failures, provider errors, cache/singleflight/batching, scalar/vectorized modes. | Model, effective-options literal, original search expression, rows, NULLs, errors, authorization result, and existing execution facilities match explicit effective-options SQL; all first-argument errors use the public template and stable `expression` fallback; NULL text is never submitted; no vector-NULL short-circuit promise is made. |
| Consensus and JOIN | Every possible non-NULL source reaches consensus. | INNER/LEFT/RIGHT common columns, canonical/left/right-qualified references, matched/unmatched rows, incomplete groups, ordinary vector members, metadata/type/dimension conflicts. | Multi-path Proven works; JOIN value sources match the matrix; Phase 1 group restriction cannot be bypassed; explicit fallback works. |
| INSERT lifecycle | Every INSERT shape uses the four domains only when applicable. | INSERT SELECT; single/multi-row VALUES; SET; DEFAULT/empty row; ON DUPLICATE existing-target and new-row references; reordered columns/assignments, source extras, generated targets, explicit/ordinary/default vectors, bare transfer, implicit cast/coercion, ambiguity, and type/dimension conflicts. | Only SELECT builds a frozen source summary; every binding has one producer or explicit Unproven; target-generated rows share one definition; independent Proven producers require consensus; explicit/ordinary/default assignments remain Unproven; target and new-row references cannot fall through across domains. |
| Expression identity | Visible ordinary arguments fully determine embedding semantics. | In one statement combine explicit raw catalog options and convenience calls; repeat equal and unequal effective literals; vary catalog key order; clone and serialize expressions; reuse prepared, session, and instance caches. | Explicit raw calls never apply search overrides; convenience calls contain the derived literal; equal visible arguments have equal hash/equality identity and results; different literals are not conflated and reach the provider as their corresponding options; key-order variants remain deterministic; clone/serialization/cache reuse preserves visible arguments and results. |
| Planner and cache | Planner mode and cache reuse cannot create or retain proof state. | Traditional/cascades, prepared/non-prepared/instance caches, schema changes, Starter/non-Starter eligibility, repeated executions, child summaries, set-operation origin. | Results and errors agree across modes; cached plans contain only rewritten ordinary expressions; existing execution checks still run; no proof, summary, new gate, or epoch enters cache identity. |
| Performance | The `O(N+B)` contract and implementation-specific release budget are decidable. | Frozen workload series scaling every distinct entity class independently, model/options byte length independently, equivalent explicit baselines, both planner modes, and no-provider-I/O fixture. | Tracking Issue records environment, method, and numerical budgets before implementation; pre-resolution entity counts and metadata bytes are reported; each entity and byte counter grows only linearly; measured time and auxiliary space satisfy `O(N+B)`. |

A deterministic fake provider may be used only as a test fixture to observe provider calls, existing quota/cache/singleflight/batching behavior, errors, and vector dimensions.

## Observability and Maintenance

Resolution has no remote side effect. Plan display, logging, redaction, provider metrics, and errors follow the equivalent explicit expression. Benchmark diagnostics shall expose pre-resolution counts and distinct visits for plan nodes, bindings/schema entries, transfer edges, coalescence/equality evidence, set branches, and consumers, plus metadata bytes parsed, copied, encoded, hashed, and compared. Production diagnostics may aggregate these counts, elapsed time, and allocations, but this design adds no provider billing or payload telemetry.

Maintainers must keep operators, tree boundaries, and pre-barrier transforms closed-world; preserve operation-local summaries, INSERT shape/domains, and the Outside/Unproven distinction; use `VectorTypeCompatible` on every transfer and consensus edge; complete JOIN groups and SQL set-operation classification before the barrier; avoid optimizer/cache proof reconstruction; and keep static search-option derivation deterministic without changing dynamic explicit options.

## Impacts and Risks

### Impacts

- Users avoid duplicating catalog model/options; planner acceptance becomes deterministic and binding-local.
- Nested queries and INSERT retain provenance through semantic summaries and transfers rather than runtime layout.
- No persistent metadata, wire change, new privilege, billing mechanism, unrelated DDL/table-kind prohibition, or deployment protocol is introduced.

### Risks

- Tree-local barriers, frozen child summaries, a shared context, and semantic INSERT transfers require coordinated planner integration.
- Strict parsing can reject newly created or modified static metadata whose search derivation would be ambiguous; dynamic explicit options and grandfathered roots remain unchanged.
- Raw JSON equality and full-group JOIN consensus are intentionally conservative.
- INTERSECT/EXCEPT and outer-dependent child summaries are conservative Phase 1 rejections.
- A missing operator, boundary, set category, transfer entry, or type check must fail closed rather than infer provenance.
- Linear time/space may require shared indexes or memoization whose implementation must still remain operation-local.

## Investigation and Alternatives

- **In-place map rewriting:** rejected because duplicate keys and suffix chains can be order-dependent. Immutable one-level transformation retains intended base+override behavior.
- **Applying strict options parsing to every explicit call:** rejected because search derivation is a static catalog contract, not a provider-contract redesign.
- **Column payload or runtime slot identity:** rejected because clones, merges, INSERT layout, and optimization require fragile propagation. Operation-local bindings separate semantics from execution layout.
- **Persistent child-output identity:** rejected because an operation-local frozen positional summary closes nested trees without widening cache or optimizer identity.
- **First resolving branch:** rejected because multi-source values require complete consensus.
- **Post-optimizer resolution:** rejected because it would expand clone/hash/equality and cache identity contracts.
- **Provenance-required set-origin hash/equality:** unnecessary because origin is consumed before the barrier; independent optimizer reasons are out of scope.
- **New privilege, billing ledger, dynamic gate, or feature epoch:** rejected because provenance lowering must reuse the explicit expression and existing deployment contracts.
- **Canonical options JSON:** deferred because number, duplicate-key, ordering, and serialization compatibility need a separate contract.

## Rollout

This design adds no rollout switch. Convenience rewriting is available only where the existing auto-embedding Starter deployment capability is available.

1. After design acceptance, create or select the Tracking Issue and freeze its benchmark workloads, environment, method, and numerical budgets before implementation begins.
2. Implement deterministic static metadata parsing, tree-local barriers and frozen summaries, the resolution algebra, SQL operator/set/JOIN policy, and semantic INSERT transfers.
3. Pass every prospective acceptance class and the frozen performance gate in both planner modes and applicable cache modes.
4. Ship through the normal version rollout process for auto-embedding; do not claim cluster-wide synchronous enablement or `SET GLOBAL` behavior.

Rollback of this planner rewrite uses only a compatible binary/version deployment procedure. The rewrite adds no persistent metadata, so it requires no catalog or stored-vector rollback by itself and defines no dynamic disable, cache epoch, or distributed rollback protocol. Before rollback, operators must verify that the target binary can understand the existing auto-embedding catalog metadata and execution capability. If it cannot, downgrading the entire auto-embedding feature is a separate compatibility problem and must not be represented as rollback of this planner-only rewrite.

## Unresolved Questions

No unresolved question blocks Phase 1. JSON canonicalization, INTERSECT/EXCEPT provenance, outer-dependent child summaries, cross-statement proof caching, and post-barrier set-origin use require separate design changes.
