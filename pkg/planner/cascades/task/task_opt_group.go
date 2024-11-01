// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"io"

	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
)

type OptGroupTask struct {
	BaseTask

	group *memo.Group
}

// NewOptGroupTask returns a new optimizing group task.
func NewOptGroupTask(mctx *MemoContext, g *memo.Group) Task {
	return &OptGroupTask{BaseTask: BaseTask{
		mctx: mctx,
	}, group: g}
}

// Execute implements the task.Execute interface.
func (g *OptGroupTask) Execute() error {
	if g.group.IsExplored() {
		return nil
	}
	g.group.ForEachGE(func(ge *memo.GroupExpression) {
		g.Push(NewOptGroupExpressionTask(g.mctx, ge))
	})
	g.group.SetExplored()
	return nil
}

// Desc implements the task.Desc interface.
func (g *OptGroupTask) Desc(w io.StringWriter) {
	w.WriteString("OptGroupTask{group:")
	g.group.String(w)
	w.WriteString("}")
}
