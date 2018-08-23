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

// buildTrace builds a trace plan. Inside this method, it first optimize the
// underlying query and then constructs a schema, which will be used to constructs
// rows result.
func (b *planBuilder) buildTrace(trace *ast.TraceStmt) (Plan, error) {
	if _, ok := trace.Stmt.(*ast.SelectStmt); !ok {
		return nil, errors.New("trace only supports select query")
	}

	optimizedP, err := Optimize(b.ctx, trace.Stmt, b.is)
	if err != nil {
		return nil, errors.New("fail to optimize during build trace")
	}
	p := &Trace{StmtPlan: optimizedP}

	retFields := []string{"operation", "duration", "spanID"}
	schema := expression.NewSchema(make([]*expression.Column, 0, len(retFields))...)
	schema.Append(buildColumn("", "operation", mysql.TypeString, mysql.MaxBlobWidth))

	schema.Append(buildColumn("", "startTS", mysql.TypeString, mysql.MaxBlobWidth))
	schema.Append(buildColumn("", "duration", mysql.TypeString, mysql.MaxBlobWidth))
	p.SetSchema(schema)
	return p, nil
}
