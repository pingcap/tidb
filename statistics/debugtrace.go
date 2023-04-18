// Copyright 2023 PingCAP, Inc.
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

package statistics

import (
	"github.com/pingcap/tidb/planner/util/debugtrace"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/ranger"
)

/*
 Below is debug trace for GetRowCountByXXX().
*/

type getRowCountInput struct {
	ID     int64
	Ranges []string
}

func debugTraceGetRowCountInput(
	s sessionctx.Context,
	id int64,
	ranges ranger.Ranges,
) {
	root := debugtrace.GetOrInitDebugTraceRoot(s)
	newCtx := &getRowCountInput{
		ID:     id,
		Ranges: make([]string, len(ranges)),
	}
	for i, r := range ranges {
		newCtx.Ranges[i] = r.String()
	}
	root.AppendStepToCurrentContext(newCtx)
}
