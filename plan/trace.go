package plan

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
)

// Trace represents a trace plan.
type Trace struct {
	baseSchemaProducer

	StmtPlan Plan
}
