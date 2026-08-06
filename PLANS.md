# Codex Execution Plans (ExecPlans)

This document defines an execution plan (an "ExecPlan"): a design-and-execution document that a coding agent can follow to deliver a working feature or system change.

Assume the reader is new to this repository. They only have the current working tree and the ExecPlan file you provide. There is no memory of earlier plans and no external context.

## How to use ExecPlans and PLANS.md

When authoring an ExecPlan, follow this file exactly. If this file is not in your immediate context, re-read the entire file before editing or implementing a plan.

When implementing an ExecPlan, keep moving milestone by milestone. Do not ask for "next steps" if the plan already defines them. Keep the plan current at every stopping point so another contributor can resume from the ExecPlan alone.

When discussing an ExecPlan, record decisions directly in the plan (Decision Log). ExecPlans are living documents: updates must preserve self-containment.

When requirements are uncertain or risky, include prototyping milestones (small proofs of concept) that validate feasibility before full implementation. Capture what was learned and how it affects subsequent milestones.

If repository policy (for example `AGENTS.md`) imposes stricter rules than this document, follow the stricter policy and record the rule in the plan.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

- Every ExecPlan must be fully self-contained: it includes all context and instructions needed for a novice to succeed.
- Every ExecPlan is a living document and must be updated as progress happens, discoveries are made, and decisions are finalized.
- Every ExecPlan must enable end-to-end implementation by a novice with no prior repository knowledge.
- Every ExecPlan must produce demonstrably working behavior, not only source edits.
- Every ExecPlan must define non-obvious terms in plain language.

Purpose and outcome come first. Begin by explaining why the work matters from a user's perspective: what they can do after the change that they could not do before, and how to observe it.

Then describe exactly what to edit, what to run, and what to expect.

Assume the executor can list files, read files, search, run commands, and run tests, but has no hidden context. Repeat assumptions explicitly. Do not rely on external blogs or docs; include required knowledge directly in the plan.

## Formatting

Formatting is strict:

- Each ExecPlan must be one fenced code block labeled `md` when shared inline.
- If writing to a standalone `.md` file whose entire content is the ExecPlan, omit outer fences.
- Do not nest triple-backtick code fences inside an ExecPlan. Use indented blocks for commands, transcripts, diffs, or snippets.
- Use two newlines after every heading.
- Use valid Markdown headings and list syntax.

Write prose-first narrative. Prefer sentences over long bullet/checklist/table-heavy sections.

Checklists are required only in `Progress`.

## Guidelines

Self-containment and plain language are mandatory. If you use a technical term, define it immediately and ground it in this repository (files, packages, commands).

Avoid common failure modes:

- Do not rely on undefined jargon.
- Do not define success as "code compiles" when behavior is unchanged.
- Do not leave key decisions unresolved for the next reader.

When ambiguity exists, resolve it in the plan and explain why.

Anchor the plan in observable outcomes:

- State exactly what someone can do after implementation.
- List commands to verify behavior.
- Describe expected output or state changes.

Acceptance criteria should be human-verifiable behavior, not only internal structure.

Specify repository context explicitly:

- Use repository-relative file paths.
- Name functions, methods, and packages precisely.
- Explain how touched areas connect.
- For commands, include working directory and exact command line.

Be idempotent and safe:

- Steps should be repeatable without causing drift.
- If a step can fail midway, include a recovery path.
- For risky/destructive steps, provide rollback or backup guidance.

Validation is mandatory:

- Include test commands and expected outcomes.
- If relevant, include an end-to-end scenario that proves behavior.
- State how to interpret failures.

Capture evidence:

- Include concise terminal output, diffs, or logs that prove correctness.
- Keep evidence focused and minimal.

## Milestones

Milestones are narrative checkpoints, not bureaucracy.

For each milestone, briefly describe:

- scope,
- what newly exists after this milestone,
- commands to run,
- expected acceptance result.

Each milestone must be independently verifiable and must incrementally advance the overall goal.

Progress and milestones are distinct:

- Milestones tell the implementation story.
- Progress tracks granular completion state.

Both are required.

## Living plans and design decisions

ExecPlans must always include and maintain:

- `Progress`
- `Surprises & Discoveries`
- `Decision Log`
- `Outcomes & Retrospective`

As discoveries happen (behavioral quirks, performance trade-offs, bugs), capture them with short evidence snippets.

If implementation direction changes, record why in `Decision Log` and update impacted sections.

At major milestones (or full completion), add an `Outcomes & Retrospective` entry summarizing outcomes, remaining gaps, and lessons learned.

## Prototyping milestones and parallel implementations

Prototyping milestones are encouraged when they de-risk larger changes.

Examples:

- validate an external dependency in isolation,
- test two competing integration approaches,
- benchmark an uncertain performance path.

Keep prototypes additive and testable. Define clear promotion/discard criteria.

Parallel implementations (temporary old/new paths) are acceptable when they reduce migration risk and keep tests passing. Describe validation for each path and the retirement strategy for temporary code.

## Skeleton of a good ExecPlan

    # <Short, action-oriented description>

    This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

    Reference: `PLANS.md` (this file) at repository root; this plan must be maintained according to it.

    ## Purpose / Big Picture

    Explain in a few sentences what someone gains after this change and how to observe it working.

    ## Progress

    Use a checkbox list for granular progress. Every stopping point must be reflected.

    - [x] (2026-03-16 10:30Z) Example completed step.
    - [ ] Example incomplete step.
    - [ ] Example partially completed step (completed: X; remaining: Y).

    ## Surprises & Discoveries

    Document unexpected behavior and concise evidence.

    - Observation: ...
      Evidence: ...

    ## Decision Log

    Record every key decision in this format:

    - Decision: ...
      Rationale: ...
      Date/Author: ...

    ## Outcomes & Retrospective

    Summarize outcomes, remaining gaps, and lessons learned; compare against the original purpose.

    ## Context and Orientation

    Describe the relevant current state for a novice reader. Name key files/packages by full repository-relative path. Define non-obvious terms.

    ## Plan of Work

    Describe, in prose, the sequence of edits/additions. For each edit, name the file and the precise location (function/type/module), and what changes.

    ## Concrete Steps

    Provide exact commands and working directories. Include short expected output snippets so a novice can compare.

    ## Validation and Acceptance

    Describe how to exercise the system and what to observe. Phrase acceptance as behavior with concrete inputs/outputs.

    Example: run the project test command and expect the new test to fail before the change and pass after the change.

    ## Idempotence and Recovery

    Explain which steps are safe to rerun and provide rollback/retry guidance for risky steps.

    ## Artifacts and Notes

    Include concise transcripts, diffs, or snippets that prove success.

    ## Interfaces and Dependencies

    Be prescriptive. Name packages/modules/services and why they are used. Specify interface/type/function signatures expected at milestone completion.

    Prefer stable names and paths such as `pkg/planner/core.(*PlanBuilder).Build` or `package/submodule.Interface`.

    Example (Go):

    In `pkg/example/planner/planner.go`, define:

        type Planner interface {
            Plan(observed *Observed) ([]Action, error)
        }

A good ExecPlan is self-contained, self-sufficient, novice-guiding, and outcome-focused. A stateless agent or new human contributor should be able to execute it from top to bottom and produce a working result.

When revising a plan, ensure changes are reflected across all sections and append a short note at the bottom describing what changed and why.
