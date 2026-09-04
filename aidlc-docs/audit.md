# User input audit

2026-09-04T00:00:00+08:00

> /goal working on a seperate worktree indepently.
> Walk through the whole repo: file by file, package by package, file by file, function by function, test by tes, line by line, don't stop looping: Read and inventory every Go production file, test, fixture, generated/platform variant, and build artifact in that package before editing. Remove Rust-only behavior, implement missing Go behavior, and update the package parity receipt and ExecPlan. Skip go code, focus on the rust code alignment and fix.
>
> For each fix: Add focused regression tests, run the Ready validation profile, and produce one meaningful batch commit.
>
> rust: [https://github.com/pingcap/tidb/commits/hparser-integration/](https://github.com/pingcap/tidb/commits/hparser-integration/)
> go: master
> let agent loop without stopping，fix should be push by go package, push to [https://github.com/pingcap/tidb/commits/hparser-integration/▍](https://github.com/pingcap/tidb/commits/hparser-integration/▍)
