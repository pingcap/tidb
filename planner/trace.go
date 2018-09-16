package planner

import (
	"github.com/pingcap/tidb/ast"
)

// Trace represents a trace planner.
type Trace struct {
	baseSchemaProducer

	StmtNode ast.StmtNode
}
