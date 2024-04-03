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
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func mkJob(id int64, schemaTableNames ...string) *model.Job {
	schemaInfos := make([]model.InvolvingSchemaInfo, len(schemaTableNames))
	for _, schemaTableName := range schemaTableNames {
		ss := strings.Split(schemaTableName, ".")
		schemaInfos = append(schemaInfos, model.InvolvingSchemaInfo{
			Database: ss[0],
			Table:    ss[1],
		})
	}
	return &model.Job{
		ID:                  id,
		InvolvingSchemaInfo: schemaInfos,
	}
}

func TestRunningJobs(t *testing.T) {
	orderedAllIDs := func(ids string) string {
		ss := strings.Split(ids, ",")
		ssid := make([]int, len(ss))
		for i := range ss {
			id, err := strconv.Atoi(ss[i])
			require.NoError(t, err)
			ssid[i] = id
		}
		sort.Ints(ssid)
		for i := range ssid {
			ss[i] = strconv.Itoa(ssid[i])
		}
		return strings.Join(ss, ",")
	}

	j := newRunningJobs()
	require.Equal(t, "", j.allIDs())

	runnable := j.checkRunnable(mkJob(0, "db1.t1"))
	require.True(t, runnable)
	job1 := mkJob(1, "db1.t1", "db1.t2")
	job2 := mkJob(2, "db2.t3")
	j.add(job1)
	j.add(job2)
	require.Equal(t, "1,2", orderedAllIDs(j.allIDs()))
	runnable = j.checkRunnable(mkJob(0, "db1.t1"))
	require.False(t, runnable)
	runnable = j.checkRunnable(mkJob(0, "db1.t2"))
	require.False(t, runnable)
	runnable = j.checkRunnable(mkJob(0, "db3.t4", "db1.t1"))
	require.False(t, runnable)
	runnable = j.checkRunnable(mkJob(0, "db3.t4", "db4.t5"))
	require.True(t, runnable)

	job3 := mkJob(3, "db1.*")
	j.add(job3)
	require.Equal(t, "1,2,3", orderedAllIDs(j.allIDs()))
	runnable = j.checkRunnable(mkJob(0, "db1.t100"))
	require.False(t, runnable)

	job4 := mkJob(4, "db4.")
	j.add(job4)
	require.Equal(t, "1,2,3,4", orderedAllIDs(j.allIDs()))
	runnable = j.checkRunnable(mkJob(0, "db4.t100"))
	require.True(t, runnable)

	job5 := mkJob(5, "*.*")
	j.add(job5)
	require.Equal(t, "1,2,3,4,5", orderedAllIDs(j.allIDs()))
	runnable = j.checkRunnable(mkJob(0, "db100.t100"))
	require.False(t, runnable)

	job5.State = model.JobStateDone
	j.remove(job5)
	require.Equal(t, "1,2,3,4", orderedAllIDs(j.allIDs()))
	runnable = j.checkRunnable(mkJob(0, "db100.t100"))
	require.True(t, runnable)

	job3.State = model.JobStateDone
	j.remove(job3)
	require.Equal(t, "1,2,4", orderedAllIDs(j.allIDs()))
	runnable = j.checkRunnable(mkJob(0, "db1.t100"))
	require.True(t, runnable)

	job1.State = model.JobStateDone
	j.remove(job1)
	require.Equal(t, "2,4", orderedAllIDs(j.allIDs()))
	runnable = j.checkRunnable(mkJob(0, "db1.t1"))
	require.True(t, runnable)
}

func TestOwnerRetireThenToBeOwner(t *testing.T) {
	j := newRunningJobs()
	require.Equal(t, "", j.allIDs())
	job := mkJob(1, "test.t1")
	j.add(job)
	require.False(t, j.checkRunnable(job))
	// retire
	j.clear()
	// to be owner, try to start a new job.
	require.False(t, j.checkRunnable(job))
	// previous job removed.
	j.remove(job)
	require.True(t, j.checkRunnable(job))
	j.add(job)
	require.False(t, j.checkRunnable(job))
}
