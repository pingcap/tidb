# Proposal: Standardize error codes and messages

- Author(s):     Joshua
- Last updated:  July 22
- Discussion at: https://docs.google.com/document/d/1beoa5xyuToboSx6e6J02tjLLX5SWzmqvqHuAPEk-I58/edit?usp=sharing

## Abstract

This issue proposes that TiDB components maintain standard error codes and error messages according to a consistent specification.

## Motivation

When certain errors occur in TiDB components, users are often unaware of the meaning of the corresponding error message, and we plan the following two initiatives to alleviate this problem

- Specification of error codes for TiDB components
- Provide a program that allows users to quickly access information corresponding to error codes

## Proposal

### The Metafiles

In order to let TiUP know the the errors every component may throw, the components developers should
keep a metafile in the code repository. The metafile should be a toml file which looks like:

```toml
[error.8005]
error = '''Write Conflict, txnStartTS is stale'''
description = '''Transactions in TiDB encounter write conflicts.'''
workaround = '''
Check whether `tidb_disable_txn_auto_retry` is set to `on`. If so, set it to `off`; if it is already `off`, increase the value of `tidb_retry_limit` until the error no longer occurs.
'''

[error.9005]
error = '''Region is unavailable'''
description = '''
A certain Raft Group is not available, such as the number of replicas is not enough.
This error usually occurs when the TiKV server is busy or the TiKV node is down.
'''
workaround = '''Check the status, monitoring data and log of the TiKV server.'''
```

Benefit:
- It's easy to write.
- It handles multiple lines well.

Tradeoff:
- The markdown content can't be previewed on writing.

Except toml, we also considered json and markdown format, see `rationale` section.

### Rationale

This section introduce two candidate formats, which is deprecated.

#### metafile in JSON

The json format of metafile is like:

```json
[
    {
        "code": 8005,
        "error": "Write Conflict, txnStartTS is stale",
        "description": "Transactions in TiDB encounter write conflicts.",
        "workaround": "Check whether `tidb_disable_txn_auto_retry` is set to `on`. If so, set it to `off`; if it is already `off`, increase the value of `tidb_retry_limit` until the error no longer occurs."
    },
    {
        "code": 9005,
        "error": "Region is unavailable",
        "description": "A certain Raft Group is not available, such as the number of replicas is not enough.\nThis error usually occurs when the TiKV server is busy or the TiKV node is down.",
        "workaround": "Check the status, monitoring data and log of the TiKV server."
    }
]
```

Benefit:
- It's easy to write (every developer knows json).
- It's easy to parse (every programming language can handle json).

Tradeoff:
- Poor readability when multiple lines are involved.
- The markdown content can't be previewed on writing.

#### metafile in markdown

```md
## Code: 8005
### Error
Write Conflict, txnStartTS is stale
### Description
A certain Raft Group is not available, such as the number of replicas is not enough.
This error usually occurs when the TiKV server is busy or the TiKV node is down.
### Workaround
Check whether `tidb_disable_txn_auto_retry` is set to `on`. If so, set it to `off`; if it is already `off`, increase the value of `tidb_retry_limit` until the error no longer occurs.

## Code: 9005
### Error
Region is unavailable
### Description
A certain Raft Group is not available, such as the number of replicas is not enough.
This error usually occurs when the TiKV server is busy or the TiKV node is down.
### Workaround
Check the status, monitoring data and log of the TiKV server.
```

Benefit:
- It's easy to write.
- It handles multiple lines well.
- The markdown content can be previewed on writing.

Tradeoff:
- It's hard to implement (see example below)
- It's not self-consistent (see example below)

Tradeoff Example:

```md
## Code: 8005
### Error
Write Conflict, txnStartTS is stale
### Description
A certain Raft Group is not available, such as the number of replicas is not enough.
This error usually occurs when the TiKV server is busy or the TiKV node is down.
### Workaround
## Code: 9005
### Error
Region is unavailable
### Description
A certain Raft Group is not available, such as the number of replicas is not enough.
This error usually occurs when the TiKV server is busy or the TiKV node is down.
### Workaround
Check the status, monitoring data and log of the TiKV server.
```

As the syntax above, the 9005 block is the message part of 8005 block, so we expect it's result is the same as this toml:

```toml
[error.8005]
error = '''Write Conflict, txnStartTS is stale'''
description = '''Transactions in TiDB encounter write conflicts.'''
workaround = '''
## Code: 9005
### Error
Region is unavailable
### Description
A certain Raft Group is not available, such as the number of replicas is not enough.
This error usually occurs when the TiKV server is busy or the TiKV node is down.
### Workaround
Check the status, monitoring data and log of the TiKV server.
'''
```

In this case, we must define the grammar and write a parser. Writing parser increase much complexity. What's worse
is that I found no grammar that fits it.

#### Summary

Through the above discussion, we recommend the toml version of metafile. 

In addition, the error codes should be append only in case of conflict between versions.

### The Error Definition

In the discussion above, an error has at least 4 parts:
- The error code: it's the identity of an error.
- The error field: it's the error itself the user can view in TiDB system. Like `err.Error()`.
- The description field: the expanded detail of why this error occurred. This could be written by developer outside the code, and the more detail this field explaining the better, even some guess of the cause could be included.
- The workaround filed: how to work around this error. It's used to teach the users how to solve the error if occurring in the real environment.

Besides, we can append a optional tags field to it:
```toml
[error.9005]
error = ""
description = ""
workaround = ""
tags = ["tikv"]
```

The tags is used to classify errors (e.g. the level of seriousness). At the very beginning, we can ignore it since we don't have enough errors listed. Once we have enough data, we need to classify all errors by different dimensions. Then we will make out a standard about how to classify errors.

#### The Error Code Range

The error code is a 3-tuple of abbreviated component name, error class and error code, joined by a colon like `{Component}:{ErrorClass}:{InnerErrorCode}`.

Where `Component` field is the abbreviated component name of the error source, wrote as upper case, component names are mapped as below:

- TiKV: KV
- PD: PD
- DM: DM
- BR: BR
- TiCDC: CDC
- Lightning: LN
- TiFlash: FLASH
- Dumpling: DP

The `ErrorClass` is the name of the `ErrClass` the error belongs to, which defined by [`errors.RegisterErrorClass`](https://github.com/pingcap/errors/blob/f9054262e67a3704a936a6ea216e73287486490d/terror/terror.go#L41) or someway likewise. If this is unacceptable (for projects not written with golang), anything that can classify the "type" of this error (e.g., package name.) would also be good. 

The `InnerErrorCode` is the identity of this error internally, note that this error code can be duplicated in different component or `ErrorClass`. Both numeric and textual code are acceptable, but it would be better to provide textual code, which should be one or two short words with PascalCase to identity the error.

The content of `ErrorClass` and `InnerErrorCode` must matches `[0-9a-zA-Z]+`.

Here are some examples:

- BR:Internal:FileNotFound
- KV:Region:EpochNotMatch
- KV:Region:NotLeader
- DB:BRIE:BackupFailed

When logging, the format `[ErrorCode] message` should be used, for example:

```
[2020/07/17 18:38:06.461 +08:00] [ERROR] [import.go:259] ["failed to download file"] [error="[BR:Internal:DownloadFileFailed] failed to download foo.sst : File not found"] [errVerbose="..."]
```

For compatibility with MySQL protocol, the code transmitted through the mysql protocol should be number only, others can be a number with a prefix string.

The code of each components looks like:

```
TiDB: {class}:{code}
TiKV: KV:{class}:{code}
PD: PD:{class}:{code}
TiFlash: FLASH:{class}:{code}
DM: DM:{class}:{code}
BR: BR:{class}:{code}
CDC: CDC:{class}:{code}
Lightning: LN:{class}:{code}
Dumpling: DP:{class}:{code}

{class}, {code} ~= [A-Za-z0-9]+
```

For mysql protocol compatible components, table below shows the available purely numeric codes for each component.

| MySQL error code range  | TiDB Family Component |
| ----------------------- | ------- |
| [0, 9000)               | TiDB |
| [8124, 8200)            | Ecosystem Productions in TiDB |
| [9000, 9010)            | TiKV / PD / TiFlash |

### How It Works

In every build, the pipeline should fetch all these metafiles from all repositories:

```bash
mkdir -p errors
curl https://raw.githubusercontent.com/pingcap/tidb/master/errors.toml -o errors/tidb.toml
curl https://raw.githubusercontent.com/tikv/tikv/master/errors.toml -o errors/tikv.toml
curl https://raw.githubusercontent.com/tikv/pd/master/errors.toml -o errors/pd.toml
```

Then there are two tasks will be execute on the errors directory:
- Build a program that embed these errors, the program is used to quickly access information corresponding to error codes.
- Build the [error code document](https://pingcap.com/docs/stable/reference/error-codes/) from these errors.
