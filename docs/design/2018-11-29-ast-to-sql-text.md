# Proposal: Support restoring SQL text from an AST tree.

- Author(s):     [Yilin Zhao](https://github.com/leoppro)
- Last updated:  2018-12-05
- Discussion at: https://github.com/pingcap/tidb/issues/8532

## Abstract

This proposal aims to support restoring SQL text from any `ast.Node`.

## Background

Some new features in TiDB need all the `ast.Node` to be restorable to SQL text. 
For example, we parse the `create view v as select * from t;` SQL statement to an AST, 
and then expand `select` of the AST to `select test.t.col0, test.t.col1 from test.t;`.

The structure of an `ast.Node` is shown in the following picture (taking `ast.CreateUserStmt` as an example).

![create-user-stmt](./imgs/create-user-stmt.png)

We know there is a `Text()` method of `ast.Node`, 
in which the parser calls the `SetText()` method during the parsing process, 
and then we can call `Text()` to get the original input SQL statement. 
But the `Text()` method is incomplete, and only `Text()` of root nodes can work well. 
We should not use it when implementing this proposal.

## Proposal

I recommend adding a `Restore()` method to ast.Node. This is the function defined:

```go
type Node interface {
	// Restore AST to SQL text and append them to `sb`.
	// return error when the AST is invalid.
	Restore(sb *strings.Builder) error
	
	...
}
```

## Rationale

We know the AST is a kind of tree whose child node is corresponding to SQL text. 
So we can walk through the AST layer by layer, call `Restore()` of each child node recursively and 
splice SQL text according to AST node. 
There is the AST parsed from `SELECT column0 FROM table0 UNION SELECT column1 FROM table1 WHERE a = 1`. 
We can splice SQL text as the picture shows.

![ast tree](./imgs/ast-tree.png)

## Compatibility

The AST and SQL text are in a one-to-many relationship, so we can't restore completely equal SQL text. 
We only need to ensure that the ASTs parsed from the input SQL statement and the restored SQL statement are equal.

## Implementation

### Stage

Considering that some ast.Node depends on another ast.Node, we divide sub-tasks into four stages.  
Detailed list at [pingcap/tidb#8532](https://github.com/pingcap/tidb/issues/8532).

### Example

I implemented the `Restore` function of [ast.CreateDatabasesStmt](https://github.com/pingcap/parser/blob/ce4d755a8937ee6bc0e851fafdcd042ab5b1a1c1/ast/ddl.go#L69) 
and [ast.DropDatabaseStmt](https://github.com/pingcap/parser/blob/ce4d755a8937ee6bc0e851fafdcd042ab5b1a1c1/ast/ddl.go#L130) for examples.

```go
// Restore implements Recoverable interface.
func (n *DatabaseOption) Restore(sb *strings.Builder) error {
	switch n.Tp {
	case DatabaseOptionCharset:
		sb.WriteString("CHARACTER SET = ")
		sb.WriteString(n.Value)
	case DatabaseOptionCollate:
		sb.WriteString("COLLATE = ")
		sb.WriteString(n.Value)
	default:
		return errors.Errorf("invalid DatabaseOptionType: %d", n.Tp)
	}
	return nil
}
```

```go
// Restore implements Recoverable interface.
func (n *DropDatabaseStmt) Restore(sb *strings.Builder) error {
	sb.WriteString("DROP DATABASE ")
	if n.IfExists {
		sb.WriteString("IF EXISTS ")
	}
	WriteName(sb, n.Name)
	return nil
}
```

**There are other examples which include complete implementation and test:  
[parser#62](https://github.com/pingcap/parser/pull/62) [parser#63](https://github.com/pingcap/parser/pull/63)**

### Note

* Table name, column name, etc... use back quotes to wrap

We can use `WriteName(sb, n.Name)` to append name to strings.Builder

* Don't depend on `node.Text()`

## Open issues

https://github.com/pingcap/tidb/issues/8532
