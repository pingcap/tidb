package logicalop

import "github.com/pingcap/tidb/pkg/planner/core/base"

// MockDataSource is used for test only.
type MockDataSource struct {
	BaseLogicalPlan
}

// Init initializes MockDataSource
func (ds MockDataSource) Init(ctx base.PlanContext) *MockDataSource {
	ds.BaseLogicalPlan = NewBaseLogicalPlan(ctx, "mockDS", &ds, 0)
	return &ds
}
