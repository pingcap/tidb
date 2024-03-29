package core

import (
	"github.com/pingcap/tidb/pkg/util/size"
)

var (
	_ Task = &RootTask{}
)

// Task is a new version of `PhysicalPlanInfo`. It stores cost information for a task.
// A task may be CopTask, RootTask, MPPTaskMeta or a ParallelTask.
type Task interface {
	Count() float64
	Copy() Task
	Plan() PhysicalPlan
	Invalid() bool
	ConvertToRootTask(ctx PlanContext) *RootTask
	MemoryUsage() int64
}

// rootTask is the final sink node of a plan graph. It should be a single goroutine on tidb.
type RootTask struct {
	p       PhysicalPlan
	isEmpty bool // isEmpty indicates if this task contains a dual table and returns empty data.
	// TODO: The flag 'isEmpty' is only checked by Projection and UnionAll. We should support more cases in the future.
}

func (t *RootTask) GetPlan() PhysicalPlan {
	return t.p
}

func (t *RootTask) SetPlan(p PhysicalPlan) {
	t.p = p
}

func (t *RootTask) IsEmpty() bool {
	return t.isEmpty
}

func (t *RootTask) SetEmpty(x bool) {
	t.isEmpty = x
}

func (t *RootTask) Copy() Task {
	return &RootTask{
		p: t.p,
	}
}

func (t *RootTask) ConvertToRootTask(_ PlanContext) *RootTask {
	return t.Copy().(*RootTask)
}

func (t *RootTask) Invalid() bool {
	return t.p == nil
}

func (t *RootTask) Count() float64 {
	return t.p.StatsInfo().RowCount
}

func (t *RootTask) Plan() PhysicalPlan {
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
