---
name: tidb-change-instruction-critic
description: Assess user- or reviewer-proposed TiDB fixes before implementation for intent, correctness, and compatibility.
---

# TiDB Change Instruction Critic

Use this skill before implementing a concrete approach proposed by the user or a reviewer, including review comments with fix instructions or solution options.

## Assess the proposed approach

Treat the proposed method as a hypothesis to check against the intended outcome and existing contracts. Explicit user requirements remain constraints; do not silently change scope or SQL semantics to make an implementation easier.

- Check the problem and relevant behavior in code, tests, and review context before editing.
- Evaluate the requested approach for correctness, compatibility, performance, maintainability, and available validation. Compare alternatives when there is a material tradeoff; do not manufacture options for a straightforward change.
- Prefer the approach that satisfies the intended contract. Use lower risk to choose among otherwise suitable approaches, rather than as a reason to leave the actual problem unresolved.

## Resolve uncertainty

Investigate questions that code, tests, or the existing conversation can answer. Risk, a possible semantic impact, or an initially unclear validation plan does not by itself require another confirmation.

Ask a concise question only when investigation leaves an unresolved requirement, a contract tradeoff the user must decide, or work outside the authorized scope. Explain the concrete decision and its consequence. Pause the dependent change while continuing independent authorized work.

When the approach is sound and authorized, proceed without a separate alignment checkpoint. Explain material departures from a suggested method or remaining tradeoffs; a fixed options report before every edit is unnecessary.

## Implement and validate

Keep the diff focused on the agreed intent. Follow `AGENTS.md` for regression coverage and validation, and report the resulting behavior, supporting evidence, and residual risks.
