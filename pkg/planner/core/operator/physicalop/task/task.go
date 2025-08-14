package task

import "github.com/pingcap/tidb/pkg/planner/core/base"

var (
	_ base.Task = &RootTask{}
)
