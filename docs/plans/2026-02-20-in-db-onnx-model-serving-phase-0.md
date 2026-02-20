# In-DB ONNX Model Serving Phase 0 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add parser/AST/system-table scaffolding for `CREATE/ALTER/DROP MODEL` and `SHOW CREATE MODEL`.

**Architecture:** Extend the TiDB parser grammar and AST to represent model DDL, then introduce system tables and infoschema exposure for model metadata.

**Tech Stack:** Go, TiDB parser (goyacc), infoschema/DDL bootstrap.

---

### Task 1: Add parser tests for model DDL

**Files:**
- Modify: `/Users/brian.w/projects/tidb/pkg/parser/parser_test.go`
- Test: `/Users/brian.w/projects/tidb/pkg/parser/parser_test.go`

**Step 1: Write the failing test**

```go
func TestModelStatements(t *testing.T) {
	cases := []testCase{
		{
			"create model m (input (a int) output (score double)) using onnx location 's3://models/m/v1.onnx' checksum 'sha256:abc'",
			true,
			"CREATE MODEL `m` (INPUT (`a` INT) OUTPUT (`score` DOUBLE)) USING ONNX LOCATION 's3://models/m/v1.onnx' CHECKSUM 'sha256:abc'",
		},
		{
			"alter model m set location 's3://models/m/v2.onnx' checksum 'sha256:def'",
			true,
			"ALTER MODEL `m` SET LOCATION 's3://models/m/v2.onnx' CHECKSUM 'sha256:def'",
		},
		{
			"drop model m",
			true,
			"DROP MODEL `m`",
		},
		{
			"show create model m",
			true,
			"SHOW CREATE MODEL `m`",
		},
	}
	RunTest(t, cases, false)
}
```

**Step 2: Run test to verify it fails**

Run: `/bin/zsh -lc "cd /Users/brian.w/projects/tidb/pkg/parser && go test -run TestModelStatements --tags=intest"`

Expected: FAIL with a parse error (missing grammar).

### Task 2: Add AST nodes and restore logic

**Files:**
- Modify: `/Users/brian.w/projects/tidb/pkg/parser/ast/ddl.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/parser/ast/ast.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/parser/ast/sem.go`

**Step 1: Add new AST structs and Restore/Accept methods**

```go
type CreateModelStmt struct {
	ddlNode
	IfNotExists bool
	Name        *TableName
	InputCols   []*ColumnDef
	OutputCols  []*ColumnDef
	Engine      string // "ONNX" for v1
	Location    string
	Checksum    string
}

func (n *CreateModelStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE MODEL ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return errors.Annotate(err, "restore CreateModelStmt Name")
	}
	ctx.WritePlain(" (")
	ctx.WriteKeyWord("INPUT (")
	for i, col := range n.InputCols {
		if i > 0 {
			ctx.WritePlain(",")
		}
		if err := col.Restore(ctx); err != nil {
			return errors.Annotatef(err, "restore CreateModelStmt InputCols[%d]", i)
		}
	}
	ctx.WritePlain(") ")
	ctx.WriteKeyWord("OUTPUT (")
	for i, col := range n.OutputCols {
		if i > 0 {
			ctx.WritePlain(",")
		}
		if err := col.Restore(ctx); err != nil {
			return errors.Annotatef(err, "restore CreateModelStmt OutputCols[%d]", i)
		}
	}
	ctx.WritePlain(")")
	ctx.WritePlain(") ")
	ctx.WriteKeyWord("USING ")
	ctx.WriteKeyWord(n.Engine)
	ctx.WriteKeyWord(" LOCATION ")
	ctx.WriteString(n.Location)
	ctx.WriteKeyWord(" CHECKSUM ")
	ctx.WriteString(n.Checksum)
	return nil
}
```

Add corresponding `AlterModelStmt`, `DropModelStmt`, and `ShowCreateModelStmt` with `Restore` and `Accept` methods, and register them in the DDL node list. Update `GetStmtLabel` and `SEMCommand` with new entries.

**Step 2: Run parser tests (still expected to fail until grammar is added)**

Run: `/bin/zsh -lc "cd /Users/brian.w/projects/tidb/pkg/parser && go test -run TestModelStatements --tags=intest"`

Expected: FAIL due to parser grammar, but AST build should succeed.

### Task 3: Add parser grammar and keywords

**Files:**
- Modify: `/Users/brian.w/projects/tidb/pkg/parser/parser.y`

**Step 1: Add keywords**

Add `MODEL` and `ONNX` to the UnReservedKeyword section in `parser.y`.

**Step 2: Add grammar rules**

Add DDL productions:

```yacc
CreateModelStmt:
	"CREATE" "MODEL" ModelIfNotExistsOpt TableName ModelSpec
	{
		stmt := $5.(*ast.CreateModelStmt)
		stmt.Name = $4.(*ast.TableName)
		stmt.IfNotExists = $3.(bool)
		$$ = stmt
	}
;

ModelIfNotExistsOpt:
	"IF" "NOT" "EXISTS" { $$ = true }
| /* empty */ { $$ = false }
;

ModelSpec:
	'(' "INPUT" '(' ColumnDefList ')' "OUTPUT" '(' ColumnDefList ')' ')' "USING" "ONNX" "LOCATION" stringLit "CHECKSUM" stringLit
	{
		$$ = &ast.CreateModelStmt{
			InputCols: $4.([]*ast.ColumnDef),
			OutputCols: $8.([]*ast.ColumnDef),
			Engine: "ONNX",
			Location: $14.(string),
			Checksum: $16.(string),
		}
	}
;

AlterModelStmt:
	"ALTER" "MODEL" TableName "SET" "LOCATION" stringLit "CHECKSUM" stringLit
	{
		$$ = &ast.AlterModelStmt{Name: $3.(*ast.TableName), Location: $6.(string), Checksum: $8.(string)}
	}
;

DropModelStmt:
	"DROP" "MODEL" IfExists TableName
	{
		$$ = &ast.DropModelStmt{IfExists: $3.(bool), Name: $4.(*ast.TableName)}
	}
;

ShowCreateModelStmt:
	"SHOW" "CREATE" "MODEL" TableName
	{
		$$ = &ast.ShowCreateModelStmt{Name: $4.(*ast.TableName)}
	}
;
```

Wire these into the statement list (`Statement` or `DDLStmt`) where other CREATE/ALTER/DROP statements live.

**Step 3: Regenerate parser**

Run: `/bin/zsh -lc "cd /Users/brian.w/projects/tidb/pkg/parser && make parser"`

**Step 4: Run tests to verify they pass**

Run: `/bin/zsh -lc "cd /Users/brian.w/projects/tidb/pkg/parser && go test -run TestModelStatements --tags=intest"`

Expected: PASS.

### Task 4: Add system tables and infoschema exposure

**Files:**
- Modify: `/Users/brian.w/projects/tidb/pkg/infoschema/tables.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/infoschema/infoschema.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/meta/autoid` (if new IDs required)
- Modify: `/Users/brian.w/projects/tidb/pkg/ddl` bootstrap definitions

**Step 1: Write failing infoschema test**

```go
func TestModelSystemTables(t *testing.T) {
	tk := testkit.NewTestKit(t, store)
	rows := tk.MustQuery("SHOW TABLES FROM mysql LIKE 'tidb_model%'").Rows()
	require.Len(t, rows, 2)
}
```

**Step 2: Run test to verify it fails**

Run: `/bin/zsh -lc "cd /Users/brian.w/projects/tidb && go test -run TestModelSystemTables --tags=intest ./pkg/infoschema"`

Expected: FAIL (tables not present).

**Step 3: Add system table definitions and bootstrap wiring**

Add table definitions for `mysql.tidb_model` and `mysql.tidb_model_version` in infoschema tables and ensure they are created during bootstrap.

**Step 4: Run tests to verify they pass**

Run: `/bin/zsh -lc "cd /Users/brian.w/projects/tidb && go test -run TestModelSystemTables --tags=intest ./pkg/infoschema"`

Expected: PASS.

