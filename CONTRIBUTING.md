# Contribution Guide

TiDB is a community-driven open source project and we welcome any contributor.  Contributions to the TiDB project are expected to adhere to our [Code of Conduct](https://github.com/pingcap/community/blob/master/CODE_OF_CONDUCT.md).

This document outlines some conventions about development workflow, commit message formatting, contact points and other resources to make it easier to get your contribution accepted. This document is the canonical source of truth for things like supported toolchain versions for building and testing TiDB.

<!-- TOC -->

- [Contribution Guide](#contribution-guide)
    - [Before you get started](#before-you-get-started)
        - [Sign the CLA](#sign-the-cla)
        - [Setting up your development environment](#setting-up-your-development-environment)
    - [Your First Contribution](#your-first-contribution)
        - [Hacktoberfest](#hacktoberfest)
    - [Before you open your PR](#before-you-open-your-pr)
    - [TiDB Contribution Workflow](#tidb-contribution-workflow)
    - [Get a code review](#get-a-code-review)
        - [Style reference](#style-reference)
    - [Bot Commands](#bot-commands)
    - [Benchmark](#benchmark)

<!-- /TOC -->

## Before you get started

### Sign the CLA

Click the **Sign in with Github to agree** button to sign the CLA. See an example [here](https://cla-assistant.io/pingcap/tidb?pullRequest=16303).

What is [CLA](https://www.clahub.com/pages/why_cla)?

### Setting up your development environment

TiDB is written in GO. Before you start contributing code to TiDB, you need to
set up your GO development environment.

1. Install `Go` version **1.13** or above. Refer to [How to Write Go Code](http://golang.org/doc/code.html) for more information.
2. Define `GOPATH` environment variable and modify `PATH` to access your Go binaries. A common setup is as follows. You could always specify it based on your own flavor.

    ```sh
    export GOPATH=$HOME/go
    export PATH=$PATH:$GOPATH/bin
    ```

>**Note:** TiDB uses [`Go Modules`](https://github.com/golang/go/wiki/Modules)
to manage dependencies.

Now you should be able to use the `make` command to build TiDB.

> **Note:**
>
>For TiKV development environment setup, See [Building and setting up a development
workspace](https://github.com/tikv/tikv/blob/master/CONTRIBUTING.md#building-and-setting-up-a-development-workspace).

## Your First Contribution

All set to contribute? You can start by finding an existing issue with the
[help-wanted](https://github.com/pingcap/tidb/issues?q=is%3Aissue+is%3Aopen+label%3Astatus%2Fhelp-wanted) label in the tidb repository. These issues are well suited for new contributors.

### Hacktoberfest

TiDB is in Hacktoberfest 2020! [Hacktoberfest](https://hacktoberfest.digitalocean.com/) is a month-long celebration & challenge of open source projects organized by DigitalOcean, Intel, and DEV. Whether you are a seasoned contributor or looking for projects to contribute to for the first time, you’re welcome to participate. Open four or more high-quality pull requests between **October 1 and 31** to any public repositories to get a limited edition Hacktoberfest T-shirt!

To participate, sign up for Hacktoberfest using your GitHub, and pick up the following issues specially tailored for you:

<-TO BE UPDATED with ISSUE LINKS-->

## Before you open your PR

Before you move on, please make sure what your issue and/or pull request is, a
simple bug fix or an architecture change.

In order to save reviewers' time, each issue should be filed with template and
should be sanity-checkable in under 5 minutes.

- **Is this a simple bug fix?**

    Bug fixes usually come with tests. With the help of continuous integration
    test, patches can be easy to review. Please update the unit tests so that they
    catch the bug! Please check example
    [here](https://github.com/pingcap/tidb/pull/2808).

- **Is this an architecture improvement?**

    Some examples of "Architecture" improvements:

    - Converting structs to interfaces.
    - Improving test coverage.
    - Decoupling logic or creation of new utilities.
    - Making code more resilient (sleeps, backoffs, reducing flakiness, etc).

    If you are improving the quality of code, then justify/state exactly what you
    are 'cleaning up' in your Pull Request so as to save reviewers' time. An
    example will be this [pull request](https://github.com/pingcap/tidb/pull/3113).

    If you're making code more resilient, test it locally to demonstrate how
    exactly your patch changes things.

    > **Tip:**
    >
    >To improve the efficiency of other contributors and avoid
    duplicated working, it's better to leave a comment in the issue that you are
    working on.

## TiDB Contribution Workflow

To contribute to the TiDB code base, please follow the workflow as defined in this section.

1. Create a topic branch from where you want to base your work. This is usually master.
2. Make commits of logical units and add test case if the change fixes a bug or adds new functionality.
3. Run tests and make sure all the tests are passed.
4. Make sure your commit messages are in the proper format (see below).
5. Push your changes to a topic branch in your fork of the repository.
6. Submit a pull request.

This is a rough outline of what a contributor's workflow looks like. For more details, see [GitHub workflow](https://github.com/pingcap/community/blob/master/contributors/workflow.md).

Thanks for your contributions!

## Get a code review

If your pull request (PR) is opened, it will be assigned to reviewers within the relevant Special Interest Group (SIG). Normally each PR requires at least 2 LGTMs (Looks Good To Me) from eligible reviewers. Those reviewers will do a thorough code review, looking at correctness, bugs, opportunities for improvement, documentation and comments,
and style.

To address review comments, you should commit the changes to the same branch of
the PR on your fork.

### Style reference

Keeping a consistent style for code, code comments, commit messages, and pull requests is very important for a project like TiDB. We highly recommend you refer to and comply to the following style guides when you put together your pull requests:

- **Coding Style**: See [PingCAP coding style guide](https://github.com/pingcap/style-guide) for details.

- **Code Comment Style**: See [Code Comment Style](https://github.com/pingcap/community/blob/master/contributors/code-comment-style.md) for details.

- **Commit Message and Pull Request Style**: See [Commit Message and Pull Request Style](https://github.com/pingcap/community/blob/master/contributors/commit-message-pr-style.md) for details.

## Bot Commands

The TiDB projects uses Continuous Integration(CI) Commands to improve efficiency.

See [SRE-BOT Command Help](https://github.com/pingcap/community/blob/master/contributors/command-help.md) for details.

## Benchmark

If the change affects TIDB's performance, the benchmark data is normally required in the description. You can use the [benchstat](https://godoc.org/golang.org/x/perf/cmd/benchstat) tool to format benchmark data for change descriptions.

The following script runs benchmark multiple times (10)

```bash
go test -run=^$ -bench=BenchmarkXXX -count=10
```

**Tip**: To make the result more readable, you can copy and paste the output to both the old.txt and new.txt, and then run:

```
benchstat old.txt new.txt
```

Paste the result into your pull request description. Here is a good [example](https://github.com/pingcap/tidb/pull/12903#issue-331440170).

