package core

import (
	"github.com/pingcap/parser/ast"
)

// Trace represents a trace plan.
type Trace struct {
	baseSchemaProducer

	StmtNode ast.StmtNode
}
