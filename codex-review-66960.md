# Review of PR #66960: Sample-Based Global Partition Statistics Design

## Overall Assessment

The direction is correct: building global stats from samples instead of merging partition histograms/TopN is the right architecture for V2 statistics, especially for partitioned tables with many partitions.

The motivation section is strong and the proposal addresses real weaknesses in the current merge-based path:

- poor TopN accuracy from count inflation
- histogram distortion from boundary mismatch
- inability to discover globally frequent values that are never locally TopN
- excessive memory and CPU cost during partition-to-global merge
- unnecessarily expensive single-partition re-analyze

That said, I would not approve the design as currently written without tightening the scope or fixing two design-level correctness gaps.

## Findings

### 1. Major: the pruning rule is not correct for A-Res / `WITH N SAMPLES` mode

The design says the same threshold-based pruning works for both Bernoulli and A-Res:

- keep samples with lower weights after applying a tighter threshold
- use simple concatenation after pruning

That is valid for Bernoulli-style inclusion weights, but it is not correct for the current A-Res implementation.

In the current code:

- reservoir sampling keeps the highest keys, not the lowest ones
- merge also preserves items with larger weights/keys

Relevant code:

- `pkg/statistics/row_sampler.go`
  - `ReservoirRowSampleCollector.sampleZippedRow()`
  - `ReservoirRowSampleCollector.sampleRow()`

So the proposed rule:

```text
keep sample if Weight <= threshold
```

is consistent with Bernoulli inclusion weights, but inconsistent with reservoir keys as implemented today.

This needs one of the following:

1. Explicitly scope v1 to Bernoulli / V2 sample-rate mode only.
2. Specify the correct A-Res downsampling and merge semantics separately.

Without that, the design is internally inconsistent and would mislead implementation.

### 2. Major: saved-sample compatibility validation is too weak for a safe v1

The design stores one partition-scoped sample blob and notes that:

- samples are position-based
- each blob only contains the columns included in that ANALYZE request
- validation currently compares only FMSketch array length

That is not sufficient.

It misses at least these cases:

- add one column and drop one column, keeping the same total count
- type changes with the same column count
- collation changes with the same column count
- different analyzed column sets across runs
- different ordering of analyzed columns or index/group definitions across runs

This is not just a future improvement. It is required for correctness if saved samples are reused across analyzes.

The persisted blob should carry an explicit compatibility signature, at minimum:

- ordered column IDs
- ordered index/group metadata
- column types
- collations where relevant
- analyze mode / request shape metadata

Then rebuild should refuse reuse unless the signature matches exactly.

### 3. Medium: `mysql.stats_fm_sketch` does not become obsolete yet

The design says `mysql.stats_fm_sketch` becomes obsolete.

That is too strong for the current proposal.

The same document still relies on the merge-based path for:

- `OFF`
- `SAVE`
- upgrade transition
- missing saved samples
- other fallback cases

And the current merge path still reads from `mysql.stats_fm_sketch`.

So the correct statement is closer to:

- the sample-based path can eliminate the need to read `stats_fm_sketch` in the steady state
- `stats_fm_sketch` may become removable later, after the fallback merge path is retired

I would change the wording to avoid overclaiming.

### 4. Medium: interaction with async global-stats merge should be resolved in the design

The document leaves the interaction with `tidb_enable_async_merge_global_stats` as an unresolved question.

I do not think that should remain unresolved at design-review time, because it affects:

- user-visible ANALYZE latency
- when sample persistence happens
- whether warnings are surfaced synchronously
- operational rollout behavior
- how this integrates with existing global-stats execution paths

At minimum the design should state one of:

- sample-based global stats replace both blocking and async merge paths
- sample-based build only runs synchronously in v1
- sample persistence and global build have separate sync/async semantics

Leaving this unspecified creates avoidable implementation ambiguity.

## What Looks Good

Several parts of the proposal are directionally strong and should be kept:

- Reusing the same sample-to-histogram construction path as non-partitioned V2 stats is the right architectural move.
- Persisting partition-scoped pruned samples for incremental rebuild is a sound way to avoid re-scanning unchanged partitions.
- The gradual-transition policy after upgrade / new partitions is pragmatic and avoids regressing global stats quality during rollout.
- Returning fallback reasons as SQL warnings is good operational design.
- Treating global indexes as out of scope is correct.

## Suggested Design Changes Before Approval

I would want the design updated in these ways before approving it:

1. Narrow the initial scope explicitly to Bernoulli/V2 sample-rate mode, unless the author is ready to specify correct A-Res handling.
2. Replace the current schema compatibility check with a required persisted compatibility signature.
3. Reword the `stats_fm_sketch` section so it reflects fallback reality.
4. Resolve the async-merge interaction in the main design, not in unresolved questions.

## PR Comment / Review Handling

I did not find substantive human review comments that clearly still require response in the PR discussion.

The visible bot comments were mostly documentation hygiene and appear to have been addressed by follow-up commits. The remaining visible items are administrative rather than technical:

- approval is still needed
- Pantheon had an infrastructure failure and could be rerun if desired

I was not able to query GitHub review-thread resolution state via GraphQL because `api.github.com` failed from the sandbox, so the “handled” assessment is based on the visible PR history and subsequent commits.
