# Proposal: Restructure TiDB Tests

- Author(s): [tison](http://github.com/tisonkun)
- Discussion PR: https://github.com/pingcap/tidb/pull/26024
- Tracking Issue: https://github.com/pingcap/tidb/issues/26022

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
* [Implementation Plan](#implementation-plan)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This document describes the proposal to restructure TiDB tests, focusing on unit tests and integration tests, especially on separating `integration_test.go` as well as unifying test infrastructure.

## Motivation or Background

On the current codebase of TiDB, tests suffer from several major troubles.

1. There are many unstable tests mainly about concurrency logic, which impacts a lot of the development experience during the project.
2. The main test framework ([pingcap/check](http://github.com/pingcap/check)) is lack of maintenance. Not only the integration with modern IDE such as GoLand is terrible so that every newcomer asks how to run a unit test, but also nobody is responsible for the test framework and the development on upstream ([go-check/check](https://github.com/go-check/check)) seems to halt.
3. Logs created by passed tests are noisy and redundant, see also [discussion on TiDB Internals forum](https://internals.tidb.io/t/topic/48).
4. There are a lot of huge test files contains many cases of "integration tests" which make the tests subtle and brittle, as well as hard to be run in parallel.
    * `expression/integration_test.go` has 9801 lines.
    * `executor/executor_test.go` has 8726 lines.
    * `ddl/db_test.go` has 6930 lines.
    * `session/session_test.go` has 4825.
    * `planner/core/integration_test.go` has 3900 lines.
5. The satellite tests are disordered. Where they are? How can we fix failures reported by them or write new cases?
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

To overcome the first four troubles listed above, I propose to start with migrating the main test framework from [pingcap/check](http://github.com/pingcap/check) to [stretchr/testify](https://github.com/stretchr/testify).

Besides, we refer to a series of suggestions for better tests in the appendix sections so that we can spawn another tracking issue to handle the problem caused by large test files, redundant logs, and unstable tests.

As described above, the main test framework of TiDB, i.e., pingcap/check, is lack of maintenance. Not only the integration with modern IDE such as GoLand is terrible so that every newcomer asks how to run a unit test, but also nobody is responsible for the test framework. What's worse, the development on upstream (go-check/check) seems to halt.

stretchr/testify is a widely adopted test framework for Golang project. We can make a quick comparison between these projects.

| Dimension    | pingcap/check               | go-check/check               | stretchr/testify              |
| ------------ | :-------------------------- | :--------------------------- | :---------------------------- |
| stars        | 16                          | 617                          | 13.6k                         |
| contributors | 17                          | 13                           | 188                           |
| development  | little, inactive for a year | little, inactive for a while | active                        |
| integration  | poor                        | out of go testing            | go testing, GoLand, and so on |
| checkers     | poor                        | even poor than pingcap/check | fruitful                      |

There is no doubt stretchr/testify is a better adoption than pingcap/check or go-check/check. You can see also the complaint on TiDB Internals forum, named [Better way to debug single unit test in tidb](https://internals.tidb.io/t/topic/141).

Many of Golang projects previous using pingcap/check, such as [pingcap/errors](https://github.com/pingcap/errors), [pingcap/log](https://github.com/pingcap/log), and [tikv/client-go](https://github.com/tikv/client-go), migrate to stretchr/testify and gain all benefits.

## Implementation Plan

The implementation of detailed design is separated into three phases.

* **Phase 0**: Start the migration to stretchr/testify by introducing the dependency, but new tests can be still written in pingcap/check.
* **Phase 1**: Once test kits in stretchr/testify version are implemented and there are adequate migration examples, prevent new tests written in pingcap/check.
* **Phase 2**: Finalize the migration and drop the dependency to pingcap/check.

It is planned to finish the migration in two sprints or three, i.e., four months or by the end of this year.

I will moderate the proposal and call for a vote for phase 1 on the forum and reflect the result in the tracking issue. The prerequisites should include implement all test kits necessary and have copyable migration samples. We may do it packages by packages, though.

## Test Design

This proposal is basically about tests themselves. And what we design to do with tests is covered by the detailed design section.

## Impacts & Risks

### Projects depend on github.com/pingcap/tidb/util/testkit or other test utils may break

For example, [pingcap/br](http://github.com/pingcap/br), which has an unfortunate circle dependency with pingcap/tidb for historical reasons, depends on `github.com/pingcap/tidb/util/testkit` and will be broken after we reach phase 2.

However, those projects can copy and paste the missing kits when implementation comes to phase 2. Also, we can take care of these cases and help them get rid of pingcap/check later.

Comment here if other projects suffer from this issue.

* [pingcap/br](http://github.com/pingcap/br)

## Investigation & Alternatives

To be honest, go testing already provides a test framework with parallel supports, children tests supports, and logging supports.

[kubernetes](https://github.com/kubernetes/kubernetes), [coredns](https://github.com/coredns/coredns), and [etcd](https://github.com/etcd-io/etcd) all make use of go testing, and only kubenetes uses stretchr/testify for its fruitful checkers.

This proposal keeps the same preference that we stay as friendly with go testing as possible, make use of checkers provided by stretchr/testify, and generate our own test kits.

### Test Improvement Suggestions

#### Leverage test logger implemented at pingcap/log#16

This section continues from the previous discussion on the TiDB Internals forum, named [Turn off redundant logging in TiDB tests](https://internals.tidb.io/t/topic/48).

Logs created by passed tests are generally less interesting to developers. go.uber.org/zap provides a module named `zaptest` to redirect output to `testing.TB`. It only caches the output per test case, and only print the output to the console if the test failed.

[pingcap/log#16](https://github.com/pingcap/log/pull/16) brings this function to pingcap/log and we'd better make use of it to prevent redundant logs output. In detail, we will introduce a method with the signature `logutil.InitTestLogger(t zaptest.TestingT, cfg *LogConfig)` and enable in every test related to logs.

Note that the test logger only prevents logs when go test without `-v` flag. But since we keep printing logs on failure case, we can safely remove the flag.

#### Prefer determinate tests for concurrency logics

Make use of `sync.Mutex`, `sync.WaitGroup`, or other concurrent utils like latches, and get rid of "long enough" sleep, which is brittle and causes unnecessary time for tests.

#### Separate huge test files into small test units that can be run in parallel

* `expression/integration_test.go` has 9801 lines.
* `executor/executor_test.go` has 8726 lines.
* `ddl/db_test.go` has 6930 lines.
* `session/session_test.go` has 4825.
* `planner/core/integration_test.go` has 3900 lines.

We should break these test files into small test units and try to perform white-box tests instead of just testing the end-to-end behavior.

## Unresolved Questions

What parts of the design are still to be determined?
