// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/types"
)

var _ Executor = &ShowDDLJobsExec{}

// ShowDDLJobsExec represent a show DDL jobs executor.
type ShowDDLJobsExec struct {
	baseExecutor

	cursor int
	jobs   []*model.Job
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobsExec) Next() (Row, error) {
	if e.cursor >= len(e.jobs) {
		return nil, nil
	}

	job := e.jobs[e.cursor]
	row := types.MakeDatums(job.String(), job.State.String())
	e.cursor++

	return row, nil
}
