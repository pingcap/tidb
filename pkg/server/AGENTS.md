# AGENTS.md

This file adds path-specific guidance for TiDB server code under `pkg/server/**`.
The repository root `AGENTS.md` still applies.

## HTTP API Documentation

- When adding, removing, renaming, or changing the behavior, parameters, or
  response shape of an HTTP API registered by `pkg/server`, MUST update
  `docs/tidb_http_api.md` in the same change.
- Keep the documentation human-readable and include the endpoint purpose,
  supported method, parameters, usage, and example request or response.
- If the API is registered only under a runtime gate, such as SYSTEM keyspace,
  NextGen, failpoint, or test-only registration, document that availability
  condition explicitly.
