# Proposal: Standardize error codes and messages

- Author(s):     Joshua
- Last updated:  May 8
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
message = '''Write Conflict, txnStartTS is stale'''
description = '''Transactions in TiDB encounter write conflicts.'''
workaround = '''
Check whether `tidb_disable_txn_auto_retry` is set to `on`. If so, set it to `off`; if it is already `off`, increase the value of `tidb_retry_limit` until the error no longer occurs.
'''

[error.9005]
message = '''Region is unavailable'''
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
        "message": "Write Conflict, txnStartTS is stale",
        "description": "Transactions in TiDB encounter write conflicts.",
        "workaround": "Check whether `tidb_disable_txn_auto_retry` is set to `on`. If so, set it to `off`; if it is already `off`, increase the value of `tidb_retry_limit` until the error no longer occurs."
    },
    {
        "code": 9005,
        "message": "Region is unavailable",
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
### Message
A certain Raft Group is not available, such as the number of replicas is not enough.
This error usually occurs when the TiKV server is busy or the TiKV node is down.
### Workaround
Check whether `tidb_disable_txn_auto_retry` is set to `on`. If so, set it to `off`; if it is already `off`, increase the value of `tidb_retry_limit` until the error no longer occurs.

## Code: 9005
### Error
Region is unavailable
### Message
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
### Message
A certain Raft Group is not available, such as the number of replicas is not enough.
This error usually occurs when the TiKV server is busy or the TiKV node is down.
### Workaround
## Code: 9005
### Error
Region is unavailable
### Message
A certain Raft Group is not available, such as the number of replicas is not enough.
This error usually occurs when the TiKV server is busy or the TiKV node is down.
### Workaround
Check the status, monitoring data and log of the TiKV server.
```

As the syntax above, the 9005 block is the message part of 8005 block, so we expect it's result is the same as this toml:

```toml
[error.8005]
message = '''Write Conflict, txnStartTS is stale'''
description = '''Transactions in TiDB encounter write conflicts.'''
workaround = '''
## Code: 9005
### Error
Region is unavailable
### Message
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
- The error code: it's the identity of an error
- The error field: it's the error itself the user can view in TiDB system
- the message field: the description of the error, what happened and why happend?
- the workaround filed: how to workaround this error

Besides, we can append a optional tags field to it:
```toml
[error.9005]
message = ""
description = ""
workaround = ""
tags = ["tikv"]
```

The tags is used to classify errors (e.g. the level of seriousness). At the very beginning, we can ignore it since we don't have enough errors listed. Once we have enough data, we need to classify all errors by different dimensions. Then we will make out a standard abount how to classify errors.

#### The Error Code Range

For compatibility with MySQL protocol, the code transmitted through the mysql protocol should be number only, others can be a number with a prefix string.

The code of each components looks like:
- TiDB: [0, 9000), DB-([A-Z]+-)?[0-9]{3,}
- TiKV: [9000, 9010), KV-([A-Z]+-)?[0-9]{3,}
- PD: [9000, 9010), PD-([A-Z]+-)?[0-9]{3,}
- DM: DM-([A-Z]+-)?[0-9]{3,}
- BR: BR-([A-Z]+-)?[0-9]{3,}
- CDC: CDC-([A-Z]+-)?[0-9]{3,}
- Lightning: LN-([A-Z]+-)?[0-9]{3,}
- Dumpling: DP-([A-Z]+-)?[0-9]{3,}

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
