package task

import (
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/internal"
	"github.com/pingcap/tidb/pkg/util/size"
)

// ************************************* RootTask Start ******************************************

// RootTask is the final sink node of a plan graph. It should be a single goroutine on tidb.
type RootTask struct {
	p base.PhysicalPlan

	// For copTask and rootTask, when we compose physical tree bottom-up, index join need some special info
	// fetched from underlying ds which built index range or table range based on these runtime constant.
	IndexJoinInfo *IndexJoinInfo

	// warnings passed through different task copy attached with more upper operator specific warnings. (not concurrent safe)
	warnings internal.SimpleWarnings
}

// GetPlan returns the root task's plan.
func (t *RootTask) GetPlan() base.PhysicalPlan {
	return t.p
}

// SetPlan sets the root task' plan.
func (t *RootTask) SetPlan(p base.PhysicalPlan) {
	t.p = p
}

// Copy implements Task interface.
func (t *RootTask) Copy() base.Task {
	nt := &RootTask{
		p: t.p,

		// when copying, just copy it out.
		IndexJoinInfo: t.IndexJoinInfo,
	}
	// since *t will reuse the same warnings slice, we need to copy it out.
	// because different task instance should have different warning slice.
	nt.warnings.Copy(&t.warnings)
	return nt
}

// ConvertToRootTask implements Task interface.
func (t *RootTask) ConvertToRootTask(_ base.PlanContext) base.Task {
	// root -> root, only copy another one instance.
	// *p: a new pointer to pointer current task's physic plan
	// warnings: a new slice to store current task-bound(p-bound) warnings.
	// *indexInfo: a new pointer to inherit the index join info upward if necessary.
	return t.Copy().(*RootTask)
}

// Invalid implements Task interface.
func (t *RootTask) Invalid() bool {
	return t.p == nil
}

// Count implements Task interface.
func (t *RootTask) Count() float64 {
	return t.p.StatsInfo().RowCount
}

// Plan implements Task interface.
func (t *RootTask) Plan() base.PhysicalPlan {
	return t.p
}

// MemoryUsage return the memory usage of rootTask
func (t *RootTask) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}
	sum = size.SizeOfInterface + size.SizeOfBool
	if t.p != nil {
		sum += t.p.MemoryUsage()
	}
	return sum
}

// AppendWarning appends a warning
func (t *RootTask) AppendWarning(err error) {
	t.warnings.AppendWarning(err)
}

// ************************************* RootTask End ******************************************
