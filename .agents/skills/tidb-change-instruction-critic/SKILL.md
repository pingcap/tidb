---
name: tidb-change-instruction-critic
description: Use when implementing a user- or reviewer-prescribed code change (including review comments with suggested fixes or options), especially when the requested edit may be risky, incomplete, ambiguous, or misaligned with TiDB correctness and compatibility constraints.
---

# TiDB Change Instruction Critic

## Trigger

Use this skill when:

- The request specifies a concrete implementation direction (for example: "change it this way", "use option B", "just follow this patch idea").
- The task is to address review comments that include direct fix instructions or multiple solution options.
- The requested approach might be correct, but the risk or tradeoff is unclear.

## Principle

Treat every implementation instruction as a hypothesis, not a command to apply blindly.
First validate problem understanding and constraints, then select the solution that best preserves correctness and contract intent; use risk as a secondary filter.

## Workflow

1. Decompose the instruction.
   - Separate `intent` (what problem is being solved), `constraints` (must-have boundaries), and `proposed method` (how to solve it).
   - Mark each item as either `mandatory` or `negotiable`.
2. Reconstruct the actual problem before coding.
   - Locate concrete evidence in code/tests/review context for the defect or concern.
   - Restate the issue in one or two precise sentences before implementation.
3. Evaluate solution candidates.
   - Always evaluate the requested approach.
   - When feasible, compare with at least one alternative.
   - Judge by correctness, compatibility, performance, maintenance cost, and validation coverage.
4. Choose and justify.
   - Prioritize correctness and behavior-contract fit over mechanical risk minimization.
   - Use lower-risk preference only when multiple candidates satisfy the same contract and intent.
   - If the requested method is weaker than an alternative, explain why and propose the better option.
5. Share analysis summary before coding.
   - Provide a short self-analysis summary: reconstructed problem, compared options, chosen solution, and why alternatives were rejected.
6. Challenge gate before implementation.
   Ask the user for clarification before coding when any condition holds:
   - Requirement or review intent is ambiguous or internally conflicting.
   - Requested change may alter SQL semantics, compatibility, or distributed behavior in non-obvious ways.
   - Requested change increases blast radius (broad refactor, cross-module behavior shift, fragile workaround).
   - Validation plan cannot prove safety with scoped tests.
   Keep questions concise and decision-enabling; do not proceed on assumptions.
7. Implement after alignment.
   - Keep diff minimal and focused on agreed intent.
   - Add or update regression coverage when behavior changes or bugs are fixed.
   - Report residual risks explicitly if any tradeoff remains.

## Question Patterns

- "The requested fix and current behavior contract conflict on `<constraint>`. Which priority is correct?"
- "I can implement `<requested approach>`, but `<alternative>` better matches `<contract>` with acceptable risk. Which should we proceed with?"
- "This change may impact `<scope>`. Should I keep scope limited to `<safe scope>` in this patch?"

## Output Checklist

- Clear problem statement confirmed from evidence.
- Chosen solution and reason for rejecting alternatives.
- Short self-analysis summary shared before implementation.
- Any user-confirmed risk decisions captured before coding.
- Validation scope aligned with change risk.
