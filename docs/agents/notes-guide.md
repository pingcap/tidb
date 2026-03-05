# TiDB Notes Flow for Agents

This document provides operational guidance for writing and maintaining notes under `docs/note/`.
Policy-level requirements remain in the repository root `AGENTS.md`.

## Location and Layout

- Notes live under `docs/note/<component>/`.
- Keep notes close to the owning component and reuse existing folders before creating new ones.
- If you add a new `docs/note/<component>/` folder, add a short entry to this document so the new location is discoverable.

## Update Rules

- Update existing sections when the topic/root cause overlaps.
- Append a new dated section only for a genuinely new topic.

## Splitting Large Notes

- If a notes file grows beyond 2000 lines, split it by functionality and update any references that pointed to the old path.

## Planner Rule Notes

- Planner rule notes live at `docs/note/planner/rule/rule_ai_notes.md`.
