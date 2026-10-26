# AGENTS.md

This file adds guidance for system variables under `pkg/sessionctx/variable/`.
The repository root `AGENTS.md` still applies.

## Sensitive Variables

- For every new system variable, MUST assess whether its value can contain
  secrets, such as passwords, API keys, tokens, credential-bearing URLs, or
  configuration/SQL text containing credentials. This applies to both built-in
  and extension-registered variables.
- Variables containing secrets MUST set `SysVar.IsSensitive` so diagnostics such
  as `GET /variables/global` mask non-empty values. If relying on an existing
  getter's redaction instead, MUST verify and document that redaction contract.
  This metadata does not change SQL getter semantics.
- When adding or changing a sensitive variable, MUST add or extend regression
  coverage for diagnostic masking or getter redaction, including empty values.
  Existing HTTP coverage is in `pkg/server/handler/tests/global_variables_test.go`.
