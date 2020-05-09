# Proposal: Standardize error codes and messages

- Author(s):     Joshua
- Last updated:  May 8
- Discussion at: https://docs.google.com/document/d/1beoa5xyuToboSx6e6J02tjLLX5SWzmqvqHuAPEk-I58/edit?usp=sharing

## Abstract

This issue proposes that TiDB components maintain standard error codes and error messages according to a consistent specification.

## Motivation

When certain errors occur in the TiDB system, an error message with an error code will be thrown. 
In many cases, users don't know what the error message is saying. So we need a manual to locate 
errors and solutions. In order to allow users to locate the cause of the error faster, we can 
integrate the manual to the `doc` component of [TiUP](https://github.com/pingcap-incubator/tiup),
so that users can locate error with the command:

```bash
tiup doc desc-error <code>
```

The `code` is the error code thrown by TiDB system. Another use case is that users can search error
with some key:

```bash
tiup doc search-error <key>
```

The `key` can be any keyword from an error message.

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
[[errors]]
code = 8005
error = '''Transactions in TiDB encounter write conflicts.'''
message = '''See [the Troubleshoot section](https://pingcap.com/docs/stable/faq/tidb#troubleshoot) for the cause and solution.'''

[[errors]]
code = 8110
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
[[errors]]
code = 8110
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
curl https://raw.githubusercontent.com/pingcap/tidb/master/errors.json -o errors/tidb.json
curl https://raw.githubusercontent.com/tikv/tikv/master/errors.json -o errors/tikv.json
curl https://raw.githubusercontent.com/tikv/pd/master/errors.json -o errors/pd.json
```

And the errors directory should be packed with the binary of doc component. When user try to
locate a certain error, the doc component will search in these error files. Once found, print
out the result.

By the way, the doc component can provide a sub command called `doc-error`. It will generate
a markdown file which list all errors and we can upload it to the [error code document](https://pingcap.com/docs/stable/reference/error-codes/).
