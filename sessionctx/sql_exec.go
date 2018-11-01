package sessionctx

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

// Statement is an interface for SQL execution.
// NOTE: all Statement implementations must be safe for
// concurrent using by multiple goroutines.
// If the Exec method requires any Execution domain local data,
// they must be held out of the implementing instance.
type Statement interface {
	// OriginText gets the origin SQL text.
	OriginText() string

	// Exec executes SQL and gets a Recordset.
	Exec(ctx context.Context) (RecordSet, error)

	// IsPrepared returns whether this statement is prepared statement.
	IsPrepared() bool

	// IsReadOnly returns if the statement is read only. For example: SelectStmt without lock.
	IsReadOnly() bool

	// RebuildPlan rebuilds the plan of the statement.
	RebuildPlan() (schemaVersion int64, err error)
}

// RecordSet is an abstract result set interface to help get data from Plan.
type RecordSet interface {
	// Fields gets result fields.
	Fields() []*ast.ResultField

	// Next reads records into chunk.
	Next(ctx context.Context, chk *chunk.Chunk) error

	// NewChunk creates a new chunk with initial capacity.
	NewChunk() *chunk.Chunk

	// Close closes the underlying iterator, call Next after Close will
	// restart the iteration.
	Close() error
}
