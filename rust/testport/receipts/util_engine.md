# `pkg/util/engine` — retired disconnected adapter seed

The adapter classifiers and their two private test cases were removed on
2026-10-08 after a caller search across both maintained repositories found no
production consumers. This withdraws the earlier complete-package claim.
The former receipt, implementation and tests remain in immutable Git archives
listed in [cleanup evidence](../../docs/parity/current-audit/pd-seed-cleanup-validation.json).

Go master `ab37692e9ebef44a736cd7243deef6020067b743` still owns
`pkg/util/engine/{BUILD.bazel,engine.go,engine_test.go}`. All three classifiers
and both five-case HTTP test matrices remain obligations if that package is
integrated again. Do not count the removed tests as passing or infer HTTP
classification acceptance from the native client's different endpoint/filter
APIs. Native routing and SQL cluster-discovery consumers and tests are retained.
