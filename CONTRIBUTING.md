# Contribution Guide

TiDB is a community driven open source project and we welcome any contributor. The process of contributing to the TiDB project
may be different than many other projects you have been involved in. This document outlines some conventions about development workflow, commit message formatting, contact points and other resources to make it easier to get your contribution accepted. This document is the canonical source of truth for things like supported toolchain versions for building and testing TiDB.

## What is a Contributor?

A Contributor refers to the person who contributes to the following projects:
- TiDB: https://github.com/pingcap/tidb 
- TiKV: https://github.com/tikv/tikv 
- TiSpark: https://github.com/pingcap/tispark 
- PD: https://github.com/pingcap/pd 
- Docs: https://github.com/pingcap/docs 
- Docs-cn: https://github.com/pingcap/docs-cn 

## How to become a TiDB Contributor?

If a PR (Pull Request) submitted to the TiDB / TiKV / TiSpark / PD / Docs／Docs-cn projects by you is approved and merged, then you become a TiDB Contributor. 

You are also encouraged to participate in the projects in the following ways:
- Actively answer technical questions asked by community users.
- Help to test the projects.
- Help to review the pull requests (PRs) submitted by others.
- Help to improve technical documents.
- Submit valuable issues.
- Report or fix known and unknown bugs.
- Participate in the existing discussion about features in the roadmap, and have interest in implementing a certain feature independently.
- Write articles about the source code analysis and usage cases for the projects.

## Pre submit pull request/issue flight checks

Before you move on, please make sure what your issue and/or pull request is, a simple bug fix or an architecture change.

In order to save reviewers' time, each issue should be filed with template and should be sanity-checkable in under 5 minutes.

### Is this a simple bug fix?

Bug fixes usually come with tests. With the help of continuous integration test, patches can be easy to review. Please update the unit tests so that they catch the bug! Please check example [here](https://github.com/pingcap/tidb/pull/2808).

### Is this an architecture improvement?

Some examples of "Architecture" improvements:

- Converting structs to interfaces.
- Improving test coverage.
- Decoupling logic or creation of new utilities.
- Making code more resilient (sleeps, backoffs, reducing flakiness, etc).

If you are improving the quality of code, then justify/state exactly what you are 'cleaning up' in your Pull Request so as to save reviewers' time. An example will be this [pull request](https://github.com/pingcap/tidb/pull/3113).

If you're making code more resilient, test it locally to demonstrate how exactly your patch changes
things.

## Building TiDB on a local OS/shell environment

TiDB development only requires `go` set-up. If you already have, simply type `make` from terminal.

### Go

TiDB is written in [Go](http://golang.org).
If you don't have a Go development environment,
please [set one up](http://golang.org/doc/code.html).

The version of GO should be **1.10** or above.

After installation, you'll need `GOPATH` defined,
and `PATH` modified to access your Go binaries.

A common setup is the following but you could always google a setup for your own flavor.

```sh
export GOPATH=$HOME/go
export PATH=$PATH:$GOPATH/bin
```

#### Dependency management

TiDB uses [`dep`](https://github.com/golang/dep) to manage dependencies.

```sh
go get -u github.com/golang/dep/cmd/dep
```

## Workflow

### Step 1: Fork in the cloud

1. Visit https://github.com/pingcap/tidb
2. Click `Fork` button (top right) to establish a cloud-based fork.

### Step 2: Clone fork to local storage

Per Go's [workspace instructions][go-workspace], place TiDB's code on your
`GOPATH` using the following cloning procedure.

Define a local working directory:

```sh
# If your GOPATH has multiple paths, pick
# just one and use it instead of $GOPATH here.
working_dir=$GOPATH/src/github.com/pingcap
```

> If you already worked with Go development on github before, the `pingcap` directory
> will be a sibling to your existing `github.com` directory.

Set `user` to match your github profile name:

```sh
user={your github profile name}
```

Create your clone:

```sh
mkdir -p $working_dir
cd $working_dir
git clone https://github.com/$user/tidb.git
# the following is recommended
# or: git clone git@github.com:$user/tidb.git

cd $working_dir/tidb
git remote add upstream https://github.com/pingcap/tidb.git
# or: git remote add upstream git@github.com:pingcap/tidb.git

# Never push to upstream master since you do not have write access.
git remote set-url --push upstream no_push

# Confirm that your remotes make sense:
# It should look like:
# origin    git@github.com:$(user)/tidb.git (fetch)
# origin    git@github.com:$(user)/tidb.git (push)
# upstream  https://github.com/pingcap/tidb (fetch)
# upstream  no_push (push)
git remote -v
```

#### Define a pre-commit hook

Please link the TiDB pre-commit hook into your `.git` directory.

This hook checks your commits for formatting, building, doc generation, etc.

```sh
cd $working_dir/tidb/.git/hooks
ln -s ../../hooks/pre-commit .
```
Sometime, pre-commit hook can not be executable. In such case, you have to make it executable manually.

```sh
cd $working_dir/tidb/.git/hooks
chmod +x pre-commit
```

### Step 3: Branch

Get your local master up to date:

```sh
cd $working_dir/tidb
git fetch upstream
git checkout master
git rebase upstream/master
```

Branch from master:

```sh
git checkout -b myfeature
```

### Step 4: Develop

#### Edit the code

You can now edit the code on the `myfeature` branch.

#### Run stand-alone mode

If you want to reproduce and investigate an issue, you may need
to run TiDB in stand-alone mode.

```sh
# Build the binary.
make server

# Run in stand-alone mode. The data is stored in `/tmp/tidb`.
bin/tidb-server
```

Then you can connect to TiDB with mysql client.

```sh
mysql -h127.0.0.1 -P4000 -uroot test
```

If you use MySQL client 8, you may get the `ERROR 1105 (HY000): Unknown charset id 255` error. To solve it, you can add `--default-character-set utf8` in MySQL client 8's arguments.

```sh
mysql -h127.0.0.1 -P4000 -uroot test --default-character-set utf8
```

#### Run Test

```sh
# Run unit test to make sure all test passed.
make dev

# Check checklist before you move on.
make checklist
```

### Step 5: Keep your branch in sync

```sh
# While on your myfeature branch.
git fetch upstream
git rebase upstream/master
```

### Step 6: Commit

Commit your changes.

```sh
git commit
```

Likely you'll go back and edit/build/test some more than `commit --amend`
in a few cycles.

### Step 7: Push

When ready to review (or just to establish an offsite backup or your work),
push your branch to your fork on `github.com`:

```sh
git push -f origin myfeature
```

### Step 8: Create a pull request

1. Visit your fork at https://github.com/$user/tidb (replace `$user` obviously).
2. Click the `Compare & pull request` button next to your `myfeature` branch.

### Step 9: Get a code review

Once your pull request has been opened, it will be assigned to at least two
reviewers. Those reviewers will do a thorough code review, looking for
correctness, bugs, opportunities for improvement, documentation and comments,
and style.

Commit changes made in response to review comments to the same branch on your
fork.

Very small PRs are easy to review. Very large PRs are very difficult to
review.

## Code style

The coding style suggested by the Golang community is used in TiDB. See the [style doc](https://github.com/golang/go/wiki/CodeReviewComments) for details.

## Commit message style

Please follow this style to make TiDB easy to review, maintain and develop.

```
<subsystem>: <what changed>
<BLANK LINE>
<why this change was made>
<BLANK LINE>
<footer>(optional)
```

The first line is the subject and should be no longer than 70 characters, the
second line is always blank, and other lines should be wrapped at 80 characters.
This allows the message to be easier to read on GitHub as well as in various
git tools.

If the change affects more than one subsystem, you can use comma to separate them like `util/codec,util/types:`.

If the change affects many subsystems, you can use ```*``` instead, like ```*:```.

For the why part, if no specific reason for the change,
you can use one of some generic reasons like "Improve documentation.",
"Improve performance.", "Improve robustness.", "Improve test coverage."

[Os X GNU tools]: https://www.topbug.net/blog/2013/04/14/install-and-use-gnu-command-line-tools-in-mac-os-x
[go-1.8]: https://blog.golang.org/go1.8
[go-workspace]: https://golang.org/doc/code.html#Workspaces
[issue]: https://github.com/pingcap/tidb/issues
[mercurial]: http://mercurial.selenic.com/wiki/Download
