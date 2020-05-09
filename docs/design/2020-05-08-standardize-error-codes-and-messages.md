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

### Requirement

In order to let TiUP know the the errors every component may throw, the components developers should
keep a meta file in the code repository. The meta file can be a json file, a toml file or a markdown file.

#### Meta file in JSON

The json format of meta file is like:

```json
[
    {
        "code": 8005,
        "error": "Transactions in TiDB encounter write conflicts.",
        "message": "See [the Troubleshoot section](https://pingcap.com/docs/stable/faq/tidb#troubleshoot) for the cause and solution.",
    },
    {
        "code": 8110,
        "error": "The Cartesian product operation cannot be executed.",
        "message": "Set cross-join in the configuration to true.\nThe second line."
    }
]
```

Benefit:
- It's easy to write (I think every developer knows json).
- It's easy to parse (I think every programming language can handle json).
- It's easy to implement (parse json and render markdown content)

Tradeoff:
- Poor readability when multiple lines are involved.
- The markdown content can't be previewed on writing.

#### Meta file in toml

```toml
[error.8005]
error = '''Transactions in TiDB encounter write conflicts.'''
message = '''See [the Troubleshoot section](https://pingcap.com/docs/stable/faq/tidb#troubleshoot) for the cause and solution.'''

[error.8110]
error = '''The Cartesian product operation cannot be executed.'''
message = '''
Set cross-join in the configuration to true.
The second line.
'''
```

Benefit:
- It's easy to write.
- It handles multiple lines well.
- It's easy to implement (parse toml and render markdown content)

Tradeoff:
- The markdown content can't be previewed on writing.

#### Meta file in markdown

```md
## Code: 8005
### Error
Transactions in TiDB encounter write conflicts.
### Message
See [the Troubleshoot section](https://pingcap.com/docs/stable/faq/tidb#troubleshoot) for the cause and solution.

## Code: 8110
### Error
The Cartesian product operation cannot be executed.
### Message
Set cross-join in the configuration to true.
The second line.
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
## Code: 8110
### Error
The Cartesian product operation cannot be executed.
### Message
## Code: 8005
### Error
Transactions in TiDB encounter write conflicts.
### Message
See [the Troubleshoot section](https://pingcap.com/docs/stable/faq/tidb#troubleshoot) for the cause and solution.
```

As the syntax above, the 8005 block is the message part of 8110 block, so we expect it's result is the same as this toml:

```toml
[error.8110]
error = '''The Cartesian product operation cannot be executed.'''
message = '''
## Code: 8005
### Error
Transactions in TiDB encounter write conflicts.
### Message
See [the Troubleshoot section](https://pingcap.com/docs/stable/faq/tidb#troubleshoot) for the cause and solution.
'''
```

In this case, we must define the grammar and write a parser. Writing parser increase much complexity. What's worse
is that I found no grammar that fits it.

### Principle

In every build, the pipeline should fetch all these metafiles from all repositories:

```bash
mkdir -p errors
curl https://raw.githubusercontent.com/pingcap/tidb/master/errors.toml -o errors/tidb.json
curl https://raw.githubusercontent.com/tikv/tikv/master/errors.toml -o errors/tikv.json
curl https://raw.githubusercontent.com/tikv/pd/master/errors.toml -o errors/pd.json
```

Then there are two tasks will be execute on the errors directory:
- Build a program that embed these errors, the program is used to quickly access information corresponding to error codes.
- Build the [error code document](https://pingcap.com/docs/stable/reference/error-codes/) from these errors.