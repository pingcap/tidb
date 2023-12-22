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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/parser/model"
)

type runningJobs struct {
	sync.RWMutex
	ids           map[int64]runningJobInfo
	runningJobIDs string
}

type runningJobInfo struct {
	schemaNames []string
	tableNames  []string
}

func newRunningJobs() *runningJobs {
	return &runningJobs{
		ids: make(map[int64]runningJobInfo),
	}
}

func (j *runningJobs) add(job *model.Job) {
	j.Lock()
	defer j.Unlock()
	schemaNames, tableNames := getJobSchemaAndTableNames(job)
	j.ids[job.ID] = runningJobInfo{
		schemaNames: schemaNames,
		tableNames:  tableNames,
	}
	j.updateInternalRunningJobIDs()
}

func getJobSchemaAndTableNames(job *model.Job) (schemaNames []string, tableNames []string) {
	if len(job.AffectedSchemaNames) == 0 {
		schemaNames = []string{job.SchemaName}
	} else {
		schemaNames = job.AffectedSchemaNames
	}
	if len(job.AffectedTableNames) == 0 {
		tableNames = []string{job.TableName}
	} else {
		tableNames = job.AffectedTableNames
	}
	return
}

func (j *runningJobs) remove(job *model.Job) {
	j.Lock()
	defer j.Unlock()
	delete(j.ids, job.ID)
	j.updateInternalRunningJobIDs()
}

func (j *runningJobs) checkRunnable(job *model.Job) bool {
	j.RLock()
	defer j.RUnlock()
	for _, info := range j.ids {
		for i := 0; i < len(info.schemaNames); i++ {
			runningSchema := info.schemaNames[i]
			runningTable := info.tableNames[i]
			jobSchemas, jobTables := getJobSchemaAndTableNames(job)
			for j, jobSchema := range jobSchemas {
				jobTable := jobTables[j]
				if checkConflict(runningSchema, runningTable, jobSchema, jobTable) {
					return false
				}
			}
		}
	}
	return true
}

func checkConflict(schemaName, tableName, jobSchemaName, jobTableName string) bool {
	if schemaName == "*" {
		return jobSchemaName != ""
	}
	if schemaName == "" {
		return false
	}
	if schemaName != jobSchemaName {
		return false
	}
	if tableName == "*" {
		return jobTableName != ""
	}
	if tableName == "" {
		return false
	}
	return tableName == jobTableName
}

func (j *runningJobs) allIDs() string {
	j.RLock()
	defer j.RUnlock()
	return j.runningJobIDs
}

func (j *runningJobs) updateInternalRunningJobIDs() {
	var sb strings.Builder
	i := 0
	for id := range j.ids {
		sb.WriteString(strconv.Itoa(int(id)))
		if i != len(j.ids)-1 {
			sb.WriteString(",")
		}
		i++
	}
	j.runningJobIDs = sb.String()
}
