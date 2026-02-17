# TiDB Codebase Refactoring Analysis

Generated: 2026-02-17

This folder contains a comprehensive analysis of the TiDB codebase identifying design issues,
performance problems, and refactoring opportunities. It is intended as a reference for AI tools
and developers working on refactoring tasks.

## File Index

| File | Description |
|------|-------------|
| [PROGRESS.md](PROGRESS.md) | Refactoring progress tracker - update after each step |
| [01-god-files.md](01-god-files.md) | Files that are too large and need splitting |
| [02-architecture.md](02-architecture.md) | Architectural design issues (dependencies, coupling, abstractions) |
| [03-performance.md](03-performance.md) | Performance issues (hot-path allocations, memory, concurrency) |
| [04-code-quality.md](04-code-quality.md) | Technical debt (TODOs, panics, context propagation, nolint) |
| [05-planner.md](05-planner.md) | Planner-specific design and performance issues |
| [06-executor.md](06-executor.md) | Executor-specific design and performance issues |
| [07-expression-types.md](07-expression-types.md) | Expression and types package issues |
| [08-session-ddl-server.md](08-session-ddl-server.md) | Session, DDL, server, and domain package issues |
| [09-priority-plan.md](09-priority-plan.md) | Prioritized refactoring plan with action items |

## How to Use

1. Check [PROGRESS.md](PROGRESS.md) for current status before starting any refactoring work.
2. Read the relevant analysis file for the component you're working on.
3. Follow the priority plan in [09-priority-plan.md](09-priority-plan.md).
4. Update [PROGRESS.md](PROGRESS.md) after completing each step.

## Summary Statistics

| Metric | Count |
|--------|-------|
| God files (>2000 lines) | 15+ |
| TODO/FIXME comments | 3,697+ |
| Panics in non-test code | 81 |
| context.Background()/TODO() | 5,266 |
| nolint directives | 738 |
| Global var files | 945 |
| init() functions | 121 |
