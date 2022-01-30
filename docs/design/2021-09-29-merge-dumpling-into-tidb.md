# Proposal: Merge Dumpling repo into TiDB

- Author(s): [okJiang](http://github.com/okJiang)
- Discussion: [Merge Dumpling repo into TiDB](https://internals.tidb.io/t/topic/434)
- Tracking Issue: [Tracking issue for merge Dumpling repo into TiDB](https://github.com/pingcap/tidb/issues/28775)

## Table of Contents

- [Introduction](#introduction)
- [Motivation](#motivation)
- [Milestones](#milestones)
   - [Milestone 1](#milestone-1)
   - [Milestone 2](#milestone-2)
   - [Milestone 3](#milestone-3)

## Introduction

[**Dumpling**](https://github.com/pingcap/dumpling) is a tool and a Go library for creating SQL dump from a MySQL-compatible database. It is intended to replace `mysqldump` and `mydumper` when targeting TiDB.

## Motivation

There are three primary reasons to merge:

1. DM suffers from dependency problem about TiDB and Dumpling. Concretely, DM depends on TiDB and Dumpling, while Dumpling depends on TiDB too. Every time DM updates Dumpling's version, TiDB's version will be updated along with Dumpling, which is unexpected.

2. Regression during a release. Dumpling releases along with TiDB even though they are in two repos. This results in conflicts are only exposed when testing failed during a release. For example, TiDB adds a new feature, which may cause a fail in Dumpling, which Dumpling isn't aware of. This issue cannot be found on their own tests, but the integration test of Dumpling and TiDB. This is very troublesome!

3. Reduce development costs. In TiDB, there is a function `export SQL` of TiDB similar with Dumpling.

After merged, we will:

1. Get rid of the DM's dependency problem;
2. Test in a monorepo and thus expose problem earlier;
3. Reduce duplicated code such as implementing `export SQL` for TiDB with Dumpling.

## Milestones

* Milestone 1: Merge **master** branch of Dumpling into TiDB repo.
* Milestone 2: Keep maintaining released branches until they are **end of maintenance**; e.g., release-4.0.
* Milestone 3: Migrate all useful content from Dumpling to TiDB repo, e.g. issues. And then **archives** Dumpling repo.

### Milestone 1

> This section refers to https://internals.tidb.io/t/topic/256.

To achieve milestone 1, we should do as follow:

1. `git checkout -b clone-dumpling && git subtree add --prefix=dumpling https://github.com/pingcap/dumpling.git master --squash`
2. `git checkout -b merge-dumpling` (we will update code in this branch)
3. Do necessary merging:
   1. merge go.mod, go.sum;
   2. update Makefile;
   3. update the import path for DM and Dumpling;
   4. merge CI scripts.
4. Create a MR from `okJiang:merge-dumpling` to `okJiang:clone-dumpling` for reviewing locally: (link if create)
5. Create true PR from `okJiang/merge-dumpling` to `pingcap:master` (link if create)
6. After reviewing, merge it to master

### Milestone 2

In this stage, we should do lots of maintenance in both repos:

* All increment activities happened in TiDB repo and give some corresponding guidance.
* Release new version (>= 5.3) from TiDB repo.
* Maintain existing active branches released for TiDB repo and Dumpling repo.

#### What increment activities should happen in TiDB repo?

* All created PRs, issues about Dumpling;
* When we want to fix an existing issue, we can create a new issue in TiDB repo with a link to original issue.

#### What guidance should we give?

* Post migrated notice on README.md like https://github.com/pingcap/br/blob/master/README.md
* When contributors create issues or PRs, we should tell them how to re-create in TiDB repo.

#### How do we release new version from TiDB repo?

* Follow TiDB's rule: https://pingcap.github.io/tidb-dev-guide/project-management/release-train-model.html

#### How do we maintain former active released branches?

> This section refers to https://internals.tidb.io/t/topic/256

If we want to cherry-pick the specific commit <COMMIT_SHA> to Dumpling repo. DO THE FOLLOWING THINGS

1. if the <COMMIT_SHA> not in <SPLIT_BRANCH>:
    1. In TiDB repo:
       1. git subtree split --prefix=dumpling -b <SPLIT_BRANCH>
       2. git checkout <SPLIT_BRANCH>
       3. git push <DUMPLING_REPO_REMOTE> <COMMIT_SHA>:refs/heads/<SPLIT_BRANCH>
2. In Dumpling repo:
   1. git fetch origin <SPLIT_BRANCH>
   2. git checkout master
   3. git checkout -b <pick_from_tidb>
   4. git cherry-pick <COMMIT_SHA>
3. Give a PR of merge <pick_from_tidb> to master.

> We will maintain release 4.0, 5.0, 5.1, 5.2 in Dumpling repo.

### Milestone 3

After all releases(<= 5.2) end of maintenance, it is time to archive Dumpling repo. But before the truly end, we should do some closing work:

* Migrate issues from Dumpling to TiDB
* ...