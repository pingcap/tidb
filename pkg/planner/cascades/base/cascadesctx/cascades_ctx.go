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

package cascadesctx

import (
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
)

// CascadesContext define the cascades context as interface, since it will be defined
// in cascades pkg, which ref task pkg with no doubt.
// while in the task pkg, the concrete task need receive cascades context as its
// constructing args, which will lead an import cycle.
// so that's why we separate it out of base pkg.
type CascadesContext interface {
	Destroy()
	GetScheduler() base.Scheduler
	PushTask(task base.Task)
	GetMemo() *memo.Memo
}
