# Proposal: Restructure TiDB Tests

- Author(s): [tison](http://github.com/tison)
- Discussion PR: https://github.com/pingcap/tidb/pull/XXX
- Tracking Issue: https://github.com/pingcap/tidb/issues/XXX

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This document describe the proposal to restructure TiDB tests, focusing on unit tests and integration tests, especially on separating `integration_test.go` as well as unifying test infrastructure.

## Motivation or Background

On the current codebase of TiDB, tests suffer from several major troubles.

1. There are a number of unstable tests mainly about concurrency logics, which impacts a lot the development experience during the project.
2. The main test framework ([pingcap/check](http://github.com/pingcap/check)) is lack of maintenance. Not only the integration with modern IDE such as GoLand is terrible so that every newcomer asks how to run a unit test, but also nobody is responsible for the test framework and the development on upstream ([go-check/check](https://github.com/go-check/check)) seems halt.
3. Logs created by passed tests are noisy and redundant, see also [discussion on TiDB Internals forum](https://internals.tidb.io/t/topic/48).
4. There are a lot of huge test files contains many cases of "integration tests" which make the tests subtle and brittle, as well as hard to be run in parallel.
    * `expression/integration_test.go` has 9801 lines.
    * `executor/executor_test.go` has 8726 lines.
    * `ddl/db_test.go` has 6930 lines.
    * `session/session_test.go` has 4825.
    * `planner/core/integration_test.go` has 3900 lines.
5. The satellite tests is disordered. Where they are? How can we fix failure reported by them or write new cases?
    * idc-jenkins-ci-tidb/common-test
    * idc-jenkins-ci-tidb/integration-common-test 
    * idc-jenkins-ci-tidb/integration-br-test
    * idc-jenkins-ci-tidb/integration-compatibility-test
    * idc-jenkins-ci-tidb/integration-copr-test
    * idc-jenkins-ci-tidb/integration-ddl-test
    * idc-jenkins-ci-tidb/mybatis-test
    * idc-jenkins-ci-tidb/sqllogic-test-1
    * idc-jenkins-ci-tidb/sqllogic-test-2
    * idc-jenkins-ci-tidb/tics-test

In this proposal, we are focus on dealing with the first four troubles, and leave the last trouble as a start of the discussion.

## Detailed Design

In order to overcome the first four troubles listed above, I propose the following changes.

1. Migrate the main test framework from [pingcap/check](http://github.com/pingcap/check) to [stretchr/testify](https://github.com/stretchr/testify), which has well maintenance, fruitful test kits, and better integration with IDE.
2. Leverage test logger implemented at [pingcap/log#16](https://github.com/pingcap/log/pull/16) to reduce the redundant testing logs.
2. During the migrations, rethink how to test the original tests want to test. For example, for tests about concurrency logics, avoid using `sleep` but prefer determinate happens before chain. For example, separate huge test files into small test files and thus generate more little parallel units.

Let's give a closer looks on the proposal.

### Migrate from pingcap/check to stretchr/testify

As described above, the main test framework of TiDB, i.e., [pingcap/check](http://github.com/pingcap/check), is lack of maintenance. Not only the integration with modern IDE such as GoLand is terrible so that every newcomer asks how to run a unit test, but also nobody is responsible for the test framework. What's worse, the development on upstream ([go-check/check](https://github.com/go-check/check)) seems halt.

[stretchr/testify](https://github.com/stretchr/testify) is a widely adopted test framework for Golang project. We can make a quick comparison between these projects.

| Dimension    | pingcap/check               | go-check/check               | stretchr/testify              |
| ------------ | :-------------------------- | :--------------------------- | :---------------------------- |
| stars        | 16                          | 617                          | 13.6k                         |
| contributors | 17                          | 13                           | 188                           |
| development  | little, inactive for a year | little, inactive for a while | active                        |
| integration  | poor                        | out of go testing            | go testing, GoLand, and so on |
| checkers     | poor                        | even poor than pingcap/check | fruitful                      |

There is no doubt stretchr/testify is a better adoption then pingcap/check or go-check/check. You can see also complaint on TiDB Internals forum, named [Better way to debug single unit test in tidb](https://internals.tidb.io/t/topic/141).

Many of Golang projects previous using pingcap/check, such as [pingcap/errors](https://github.com/pingcap/errors), [pingcap/log](https://github.com/pingcap/log), and [tikv/client-go](https://github.com/tikv/client-go), migrate to stretchr/testify and gain all benefits.

### Leverage test logger implemented at pingcap/log#16

This section continues from the previous discussion on TiDB Internals forum, named [Turn off redundant logging in TiDB tests](https://internals.tidb.io/t/topic/48).

### Rethink each tests during the migration

#### Prefer determinate tests for concurrency logics

#### Separate huge test files into small test units that can be run in parallel

## Implementation Plan

The implementation of detailed design is separated into three phases.

### Phase 0: Start the migration to stretchr/testify, but new tests can be still written in pingcap/check

### Phase 1: Once test kits in stretchr/testify version implemented and there is adequate migration examples, prevent new tests written in pingcap/check

### Phase 2: Finalize the migration and drop the dependency to pingcap/check

## Test Design

This proposal is basically about tests itself. And what we design to do with tests are covered by the detailed design section.

## Impacts & Risks

### Projects depend on github.com/pingcap/tidb/util/testkit or other test utils may break

For example, [pingcap/br](http://github.com/pingcap/br), which has an unfortunate circle dependency with pingcap/tidb for historical reason, depends on `github.com/pingcap/tidb/util/testkit` and will be broken after we reach phase 2.

However, those projects can copy and paste the missing kits when implementation comes to phase 2. Also we can take care of these cases and help them get rid of pingcap/check later.

Comment here if there are other projects suffer from this issue.

* [pingcap/br](http://github.com/pingcap/br)

## Investigation & Alternatives

How do other systems solve this issue? What other designs have been considered and what is the rationale for not choosing them?

**To be done.**

## Unresolved Questions

What parts of the design are still to be determined?
