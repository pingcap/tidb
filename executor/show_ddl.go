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
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/types"
)

var _ Executor = &ShowDDLExec{}

// ShowDDLExec represents a show DDL executor.
type ShowDDLExec struct {
	baseExecutor

	ddlOwnerID string
	selfID     string
	ddlInfo    *admin.DDLInfo
	done       bool
}

// Next implements the Executor Next interface.
func (e *ShowDDLExec) Next() (Row, error) {
	if e.done {
		return nil, nil
	}

	var ddlJob string
	if e.ddlInfo.Job != nil {
		ddlJob = e.ddlInfo.Job.String()
	}

	row := types.MakeDatums(
		e.ddlInfo.SchemaVer,
		e.ddlOwnerID,
		ddlJob,
		e.selfID,
	)
	e.done = true

	return row, nil
}
