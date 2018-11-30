# Proposal: Support to restore SQL text from a AST tree.

- Author(s):     [Yilin Zhao](https://github.com/leoppro)
- Last updated:  2018-11-29
- Discussion at: https://github.com/pingcap/tidb/issues/8532

## Abstract

This proposal aims to support to restore SQL text from any `ast.Node`.

## Background

We know there is a `Text()` function of `ast.Node`, 
the parser calls `SetText()` function during the parsing process, then we can call `Text()` to get original input sql. 
But the `Text()` function is incomplete, only the `Text()` of root nodes can work well. 

Now some features(eg. Database name and table name mapping) need us to make any `ast.Node` could restore to SQL text.

## Proposal

We know the AST is a type of tree structure of which child node corresponding to SQL text. 
So we can walk through AST tree layer by layer and splice SQL text according to AST node. 
There is a AST node parsed from `SELECT column0 FROM table0 UNION SELECT column1 FROM table1 WHERE a = 1`, 
we can splice SQL text as the picture shows.

![ast tree](./imgs/ast-tree.png)

## Rationale

See: [Rationale (Chinese)](https://docs.google.com/document/d/1KF4OvObvAzT0YW_KwiMRrsGZQMNVE03RdqShhStM_D4/edit?usp=sharing)

## Compatibility

None

## Implementation

### Code

There is a `Recoverable` interface in [pingcap/parser/ast.go](https://github.com/pingcap/parser/blob/master/ast/ast.go).  

```go
// Recoverable can be restored to sql text
type Recoverable interface {
	// Restore append sql test to `sb`
	Restore(sb *strings.Builder) error
}
```

We need implement it for all `ast.Node()`.

### Example

I implemented the `Restore` function of `ast.CreateDatabasesStmt` and `ast.DropDatabaseStmt` for examples.

```go
// Restore implements Recoverable interface.
func (n *CreateDatabaseStmt) Restore(sb *strings.Builder) error {
	sb.WriteString("CREATE DATABASE ")
	if n.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	WriteName(sb, n.Name)
	for _, option := range n.Options {
		sb.WriteString(" ")
		err := option.Restore(sb)
		if err != nil {
			return err
		}
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

### Test

For `ast.StmtNode`:

```go
var sb strings.Builder
err := stmt.Restore(&sb)
c.Assert(err, IsNil)
restoreSQL := sb.String()
comment := Commentf("source %v \nrestore %v", t.src, restoreSQL)
restoreStmt, err := parser.ParseOneStmt(restoreSQL, "", "")
c.Assert(err, IsNil, comment)
stmt.Accept(&cleaner)
restoreStmt.Accept(&cleaner)
c.Assert(restoreStmt, DeepEquals, stmt, comment)
```

We believe that a `Restore()` function is correct when it outputs sql text 
can be parsed successfully and the AST correspond to output sql equals with input sql.

For other `ast.Node`:

Some `ast.Node` can't be restored to a complete SQL text, 
to test them we can construct a complete SQL text.

```go
var cleaner NodeTextCleaner
parser := parser.New()
testNodes := []string{"select ++1", "select -+1", "select --1", "select -1"}
for _, node := range testNodes {
    stmt, err := parser.ParseOneStmt(node, "", "")
    comment := Commentf("source %v", node)
    c.Assert(err, IsNil, comment)
    var sb strings.Builder
    sb.WriteString("SELECT ")
    err = stmt.(*SelectStmt).Fields.Fields[0].Expr.Restore(&sb)
    c.Assert(err, IsNil, comment)
    restoreSql := sb.String()
    comment = Commentf("source %v ; restore %v", node, restoreSql)
    stmt2, err := parser.ParseOneStmt(restoreSql, "", "")
    c.Assert(err, IsNil, comment)
    stmt.Accept(&cleaner)
    stmt2.Accept(&cleaner)
    c.Assert(stmt2, DeepEquals, stmt, comment)
}
```

### Note

* Table name, column name, etc... use back quotes to wrap

We can use `WriteName(sb, n.Name)` to append name to strings.Builder

* Don't depend on `node.Text()`

## Open issues

https://github.com/pingcap/tidb/issues/8532
