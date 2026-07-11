# Agent Docs

This directory is the single top-level home for agent-facing documentation.

## Layout

- Cross-cutting runbooks and checklists stay at `docs/agents/*.md`
  (for example `testing-flow.md`, `architecture-index.md`, and
  `agents-review-guide.md`).
- Component-specific notes live under `docs/agents/<component>/`.
  Current component folders include:
  - `docs/agents/ddl/`
  - `docs/agents/dxf/`
  - `docs/agents/executor/`
  - `docs/agents/import-into/`
  - `docs/agents/planner/`

## Maintenance

- Keep policy-level requirements in root `AGENTS.md`.
- Keep operational details in docs under `docs/agents/`.
