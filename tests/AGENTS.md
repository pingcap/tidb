# Repository Guidelines

## Project Structure & Module Organization
- `/pkg/` contains most TiDB server code (executors, planner, storage, session, etc.). Use the package subfolders listed in `AGENTS.md` instructions when locating logic.
- `/cmd/tidb-server/` is the main service entry point.
- `/tests/integrationtest/` holds integration tests: inputs in `tests/integrationtest/t` and expected results in `tests/integrationtest/r`.
- `testdata/` directories within packages store JSON-based test fixtures (e.g., `*_in.json`, `*_out.json`).

## Build, Test, and Development Commands
- `make bazel_prepare` sets up Bazel prerequisites.
- `make` builds the repository.
- `make gogenerate` runs Go code generation.
- `go mod tidy` syncs module dependencies.
- Unit tests:
  - `make failpoint-enable`
  - `pushd pkg/<package_name> && go test -v -run <TestName> -record --tags=intest && popd`
  - `make failpoint-disable` (always run, even after failures)
- Integration tests:
  - `pushd tests/integrationtest && ./run-tests.sh -run <TestName> && popd`

## Coding Style & Naming Conventions
- Follow standard Go formatting (`gofmt`).
- Prefer descriptive, package-scoped naming; keep exported identifiers in `CamelCase` and unexported in `camelCase`.
- Use existing test table structures and fixtures instead of creating new ones.

## Testing Guidelines
- Unit tests are standard Go tests under `/pkg/`.
- Within a package, keep the number of unit tests consistent with `shard_count` in the package `BUILD.bazel` (no more than 50).
- If tests use JSON fixtures, update inputs before running, then verify outputs were updated correctly.
- For integration tests, modify inputs in `tests/integrationtest/t` and validate results in `tests/integrationtest/r`.

## Commit & Pull Request Guidelines
- Commit messages: no repository-specific convention is documented; use clear, imperative summaries.
- PR title format must be either `pkg[, pkg2]: what is changed` or `*: what is changed`.
- PR description must follow `.github/pull_request_template.md` and keep all HTML comments intact.

## Configuration & Security Notes
- Running tests may enable failpoints; always disable them after test runs.
- Avoid editing generated files unless explicitly required by a task.
