# Proposal: Move parser back to pingcap/tidb

- Author(s): [xhebox](http://github.com/xhebox)
- Discussion: [Move parser back to pingcap/tidb](https://internals.tidb.io/t/topic/385)
- Tracking Issue: [Tracking issue for moving parser back to TiDB](https://github.com/pingcap/tidb/issues/28257)

## Table of Contents

- [Introduction](#introduction)
- [Background](#background)
- [Detailed Plan](#detailed-plan)
- [Questions](#questions)

## Introduction

This is an refined version of the draft thread [Move parser back to pingcap/tidb](https://internals.tidb.io/t/topic/385).

In short, I want to move pingcap/parser back to the pingcap/tidb repository in the form of go sub-module.

## Background

I will explain why and how to migrate the parser back to the TiDB repository.

### Why did we move parser into a separate repository?

The original PR moving out parser is [tidb#7923: Move TiDB parser to a separate repository](https://github.com/pingcap/tidb/issues/7923). When we migrated to go module 111, @tiancaiamao decided to do so, because:

> We can put the generated `parser.go` in the repository directly, rather than generate it using Makefile script. The drawback of this way is that every time `parser.y` is touched, there will be many lines change in `parser.go` . Then the TiDB repo will be inflate quickly.
>
> A better way is moving parser to a separated repo, so every time parser is changed, only go.mod need to change accordingly.

Personally, that is a weird argument for me. Indeed, pingcap/tidb does not store the diff of `parser.go` anymore, but pingcap/parser does. How is moving the inflation from one repo to another better?

Another argument is about the messy dependency. The separation does improve the case.

### What is wrong with a separate repository?

In fact, it isn't a big problem. However, the development becomes inconvenient. You could argue that we have workarounds, but why not make the development easier?

1. We always need to ensure the version of two go modules. Cherry-picking becomes troublesome since the import version must be a specific branch of pingcap/parser.
2. It is a mysql parser, but is closely related to TiDB (basically everything except ast/mysql packages). You must code TiDB logic into the "standalone mysql parser" first before your real implementation, which means at least two PRs, even for small changes.
3. Before the PR of parser merged, the PR of TiDB must use module directive to redirect a specific version of parser to fix CI.

Problems above have defeated the previous argument `only go.mod need to change accordingly`. We only separated code into two repositories, actual work did not decrease. However, our development experience does getting worse.

## Detailed Plan

Obviously, we could move back to the old good way. But it must be a go sub-module, because:

1. To keep compatibility. Parser has been imported by other tools and other users.
2. To keep the clean dependency. That is why it is separated. We don't want to make things worse again.

The detailed plan:

1. Create parser directory, a sub-module, in pingcap/tidb, which is synchronized with pingcap/parser, with `replace github.com/pingcap/parser => ./parser`.
2. Stop sending new PRs and issues to pingcap/parser, new PRs should be directly sent to pingcap/tidb. And merge recent PRs in pingcap/parser as much as possible. Changes will be synchronized from pingcap/parser to pingcap/tidb manually, of course.
3. Announce the deprecation of pingcap/parser to possible users. Clean up old PRs, reopen it in pingcap/tidb, close it, or just forget about it. As for issues, by @kennytm, we could directly transfer all issues to pingcap/tidb according to the [document](https://docs.github.com/en/issues/tracking-your-work-with-issues/transferring-an-issue-to-another-repository). As for tests, we need to migrate the verify CI tests on jenkins. Since TiDB may migrate to verify CI eventually, it is not a good idea if we migrate to things other than verify CI. Rewriting the import path is enough for migrating verify CI.
4. We can create wrapper packages for pingcap/parser, which re-exports things from `pingcap/tidb/parser`. I mean something like `import ( . github.com/pingcap/tidb/parser/xxx)`, create a dummy function will eliminate the error of `not used import`. All code of pingcap/parser will be removed in this step. This will delevery new updates without the need of migrating importing paths.
5. After another one or two dev cycles, we could archive pingcap/parser. It mainly depends on users of pingcap/parser.

There will be a tracking issue, and the whole progress is public. Step 1 or 2 will likely take one or two weeks. Internally we could do the migration for internal tools while doing step 3.

Step 4 and 5 are long-running tasks to smoothly end the support of the old parser.

## Questions

- What about projects importing parser?

Nothing needs to be done before step 5. However, one will need to migrate the import path eventually, before the final deprecation.

- What about some complex dependency chain? For example, `pkg -> forked tidb -> other pingcap/parser`.

Nothing different for the complex dependencies. Just do the migration before the deprecation. And we don't actually remove the old commits for not breaking old apps.

- What about CI and tests?

It is not a problem. pingcap/parser only has unit tests, and integration tests `make gotest` of `pingcap/tidb`. We could run integration tests of tidb, and only run unit tests.

- How about release and versioning?

Since sub-module is merely another module inside another module, we need to release it separately from the main module. That means our release team needs to tag `parser/vX.X.X`, which can be solved by a script: thanks to the identical release cycle/version between parser and tidb.

- What about the bloated parser.go file? Nobody wants it, and it increases the size of tidb repo.

We can not simply remove it. Importing pingcap/parser to build applications needs parser.go file, but go module doesn't support something like build scripts in rust or `postbuild` in nodejs to auto-generate parser.go.

One solution is to only include the file for the released version. But TiDB does not support package semver, `go get xxx/parser` won't simply give a buildable package. That will be too frustrating for users.
