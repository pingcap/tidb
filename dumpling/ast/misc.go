package ast

var (
	_ Node = &ExplainStmt{}
	_ Node = &PreparedStmt{}
	_ Node = &DeallocateStmt{}
	_ Node = &ExecuteStmt{}
	_ Node = &BeginStmt{}
	_ Node = &CommitStmt{}
	_ Node = &RollbackStmt{}
	_ Node = &VariableAssignment{}
	_ Node = &SetStmt{}
)

// AuthOption is used for parsing create use statement.
type AuthOption struct {
	// AuthString/HashString can be empty, so we need to decide which one to use.
	ByAuthString bool
	AuthString   string
	HashString   string
	// TODO: support auth_plugin
}

// ExplainStmt is a statement to provide information about how is SQL statement executed
// or get columns information in a table.
// See: https://dev.mysql.com/doc/refman/5.7/en/explain.html
type ExplainStmt struct {
	txtNode

	S Node
}

// PreparedStmt is a statement to prepares a SQL statement which contains placeholders,
// and it is executed with ExecuteStmt and released with DeallocateStmt.
// See: https://dev.mysql.com/doc/refman/5.7/en/prepare.html
type PreparedStmt struct {
	txtNode

	InPrepare bool // true for prepare mode, false for use mode
	Name      string
	ID        uint32 // For binary protocol, there is no Name but only ID
	SQLVar    *Variable
	SQLStmt   Node // The parsed statement from sql text with placeholder
	Params    []*ParamMarker
}

// DeallocateStmt is a statement to release PreparedStmt.
// See: https://dev.mysql.com/doc/refman/5.7/en/deallocate-prepare.html
type DeallocateStmt struct {
	txtNode

	Name string
	ID   uint32 // For binary protocol, there is no Name but only ID.
}

// ExecuteStmt is a statement to execute PreparedStmt.
// See: https://dev.mysql.com/doc/refman/5.7/en/execute.html
type ExecuteStmt struct {
	txtNode

	Name      string
	ID        uint32 // For binary protocol, there is no Name but only ID
	UsingVars []Expression
}

// ShowStmt is a statement to provide information about databases, tables, columns and so on.
// See: https://dev.mysql.com/doc/refman/5.7/en/show.html
type ShowStmt struct {
	txtNode

	Target     int // Databases/Tables/Columns/....
	DBName     string
	Table      *TableRef // Used for showing columns.
	ColumnName string    // Used for `desc table column`.
	Flag       int       // Some flag parsed from sql, such as FULL.
	Full       bool

	// Used by show variables
	GlobalScope bool
	Pattern     *PatternLike
	Where       Expression
}

// BeginStmt is a statement to start a new transaction.
// See: https://dev.mysql.com/doc/refman/5.7/en/commit.html
type BeginStmt struct {
	txtNode
}

// CommitStmt is a statement to commit the current transaction.
// See: https://dev.mysql.com/doc/refman/5.7/en/commit.html
type CommitStmt struct {
	txtNode
}

// RollbackStmt is a statement to roll back the current transaction.
// See: https://dev.mysql.com/doc/refman/5.7/en/commit.html
type RollbackStmt struct {
	txtNode
}

// UseStmt is a statement to use the DBName database as the current database.
// See: https://dev.mysql.com/doc/refman/5.7/en/use.html
type UseStmt struct {
	txtNode

	DBName string
}

// VariableAssignment is a variable assignment struct.
type VariableAssignment struct {
	txtNode
	Name     string
	Value    Expression
	IsGlobal bool
	IsSystem bool
}

// Accept implements Node interface.
func (va *VariableAssignment) Accept(v Visitor) (Node, bool) {
	if !v.Enter(va) {
		return va, false
	}
	node, ok := va.Value.Accept(v)
	if !ok {
		return va, false
	}
	va.Value = node.(Expression)
	return v.Leave(va)
}

// SetStmt is the statement to set variables.
type SetStmt struct {
	txtNode
	// Variables is the list of variable assignment.
	Variables []*VariableAssignment
}

// Accept implements Node Accept interface.
func (set *SetStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(set) {
		return set, false
	}
	for i, val := range set.Variables {
		node, ok := val.Accept(v)
		if !ok {
			return set, false
		}
		set.Variables[i] = node.(*VariableAssignment)
	}
	return v.Leave(set)
}
