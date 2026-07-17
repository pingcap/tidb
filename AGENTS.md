# Repository Guidelines

## Code Organization

See `docs/agents/architecture-index.md`.

## Building

```
make build
```

## Testing

See `docs/agents/test-guide.md`.

## Workflow Principles

- When working with each components, lookup and fully read `MAINTAINER_GUIDE.md` inside (if exists) first, e.g. `pkg/ddl/MAINTAINER_GUIDE.md`.
  It helps you understand the whole codebase architecture.
  It is critical to learn design decisions and invariants of each component before you plan or start changes to ensure you are on the right track.
- Update the MAINTAINER_GUIDE.md after your changes if necessary.
- Don't run all tests in the workspace as it is extremely slow. Run targeted tests.
- Always ensure format is ok (`make fmt`) before you finish your work.
- Run `make check` before you finish your work when the change touches Go-related files such as `*.go`. If there are no Go-related file changes, skip `make check`.
- **NEVER** use git to discard or revert any changes not made by you - they are proposely changed by user and you should live with that.

## Engineering Rules

This project requires extremely high code quality and maintainability. Best engineering practices must be followed at all times.

The rules below are some typical principles. They are not exhaustive, and you must always use your best judgment to **write the cleanest code possible**.

### Core Principles: Simplicity & Readability

- Boring Code - Obvious, self-explanatory > clever, minimize cognitive load
- Single Responsibility - One function, one job
- Explicit over Implicit - Clear is better than concise
- Meaningful Abstractions - Only when they reduce cognitive load
- Keep DRY - Only if it does not conflict with the above principles

### Better Maintainability

- Keep the public API surface minimal; prefer reusing existing methods
- Don't treat "looks similar" as "equivalent"
- Abstractions must be meaningful
- Keep certainty, single source of truth - e.g. don't introduce "optional" unless absolutely necessary
- Always add clear, concise and explicit doc comments for public APIs, complex logic, and non-obvious code

For non-refactor tasks:

- Prefer minimal, necessary, localized changes
- Follow existing code patterns, even if they are not ideal or party against our principles
  - Do your best to trade off between "ideal code", "consistent codebase", "only change what is necessary"
  - Point out any refactor chance you discovered after finishing your work, avoid changing them in the same work. Let human decides later.

### Naming Symbols

- Choosing the name that needs the least explanation, consider: verb clarity, noun specificity, context
- Name tests by behavior and expectation.

### Conflict Management

To minimize conflicts that arise when merging commits that are ahead of upstream TiDB, reduce code maintenance costs, and alleviate the mental burden of resolving conflicts, the following guidelines should be followed when developing PingKaiDB-specific features:
- Minimize intrusions into the TiDB codebase. For existing TiDB code files, make as few modifications as possible and insert only necessary entry functions.
- The main logic should be written in separate code files and named with the prefix `pkdb_`.
- If they exist as a separate package, the package name should begin with `pkdb`, such as `pkdblicense`.

## Pull Request Guide

- See `docs/agents/pull-request-guide.md`.

## Agent Output Contract

When finishing a coding task, report:

1. Files changed.
2. Risks: correctness, compatibility, performance.
3. Exact commands run for validation.
4. What was not verified locally.
