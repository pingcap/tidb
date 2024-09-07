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

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func mkJob(id int64, schemaTableNames ...string) (int64, []model.InvolvingSchemaInfo) {
	schemaInfos := make([]model.InvolvingSchemaInfo, 0, len(schemaTableNames))
	for _, schemaTableName := range schemaTableNames {
		ss := strings.Split(schemaTableName, ".")
		schemaInfos = append(schemaInfos, model.InvolvingSchemaInfo{
			Database: ss[0],
			Table:    ss[1],
		})
	}
	return id, schemaInfos
}

func checkInvariants(t *testing.T, j *runningJobs) {
	for _, checkingObj := range []*objects{j.exclusive, j.shared, j.pending} {
		for _, tables := range checkingObj.schemas {
			// check table-level entry should not have zero length
			require.Greater(t, len(tables), 0)
			for _, v := range tables {
				require.Greater(t, v, 0)
			}
		}
		for _, v := range checkingObj.placementPolicies {
			require.Greater(t, v, 0)
		}
		for _, v := range checkingObj.resourceGroups {
			require.Greater(t, v, 0)
		}
	}
}

func orderedAllIDs(ids string) string {
	if ids == "" {
		return ""
	}

	ss := strings.Split(ids, ",")
	ssid := make([]int, len(ss))
	for i := range ss {
		id, _ := strconv.Atoi(ss[i])
		ssid[i] = id
	}
	sort.Ints(ssid)
	for i := range ssid {
		ss[i] = strconv.Itoa(ssid[i])
	}
	return strings.Join(ss, ",")
}

func TestRunningJobs(t *testing.T) {
	j := newRunningJobs()
	require.Equal(t, "", j.allIDs())
	checkInvariants(t, j)

	runnable := j.checkRunnable(mkJob(0, "db1.t1"))
	require.True(t, runnable)

	jobID1, involves1 := mkJob(1, "db1.t1", "db1.t2")
	runnable = j.checkRunnable(jobID1, involves1)
	require.True(t, runnable)
	j.addRunning(jobID1, involves1)
	jobID2, involves2 := mkJob(2, "db2.t3")
	runnable = j.checkRunnable(jobID2, involves2)
	require.True(t, runnable)
	j.addRunning(jobID2, involves2)
	require.Equal(t, "1,2", orderedAllIDs(j.allIDs()))
	checkInvariants(t, j)

	runnable = j.checkRunnable(mkJob(0, "db1.t1"))
	require.False(t, runnable)
	runnable = j.checkRunnable(mkJob(0, "db1.t2"))
	require.False(t, runnable)
	runnable = j.checkRunnable(mkJob(0, "db3.t4", "db1.t1"))
	require.False(t, runnable)
	runnable = j.checkRunnable(mkJob(0, "db3.t4", "db4.t5"))
	require.True(t, runnable)

	jobID3, involves3 := mkJob(3, "db1.*")
	runnable = j.checkRunnable(jobID3, involves3)
	require.False(t, runnable)
	j.removeRunning(jobID1, involves1)
	runnable = j.checkRunnable(jobID3, involves3)
	require.True(t, runnable)
	j.addRunning(jobID3, involves3)
	require.Equal(t, "2,3", orderedAllIDs(j.allIDs()))
	checkInvariants(t, j)

	runnable = j.checkRunnable(mkJob(0, "db1.t100"))
	require.False(t, runnable)

	jobID4, involves4 := mkJob(4, "db4.t100", "db2.t6")
	runnable = j.checkRunnable(jobID4, involves4)
	require.True(t, runnable)
	j.addRunning(jobID4, involves4)
	require.Equal(t, "2,3,4", orderedAllIDs(j.allIDs()))
	checkInvariants(t, j)

	jobID5, involves5 := mkJob(5, "*.*")
	runnable = j.checkRunnable(jobID5, involves5)
	require.False(t, runnable)

	j.removeRunning(jobID2, involves2)
	j.removeRunning(jobID3, involves3)
	j.removeRunning(jobID4, involves4)
	require.Equal(t, "", orderedAllIDs(j.allIDs()))
	checkInvariants(t, j)

	runnable = j.checkRunnable(jobID5, involves5)
	require.True(t, runnable)
	j.addRunning(jobID5, involves5)
	require.Equal(t, "5", orderedAllIDs(j.allIDs()))
	checkInvariants(t, j)

	runnable = j.checkRunnable(mkJob(0, "db1.t1"))
	require.False(t, runnable)
}

func TestSchemaPolicyAndResourceGroup(t *testing.T) {
	j := newRunningJobs()

	jobID1, involves1 := mkJob(1, "db1.t1", "db1.t2")
	runnable := j.checkRunnable(jobID1, involves1)
	require.True(t, runnable)
	j.addRunning(jobID1, involves1)

	failedInvolves := []model.InvolvingSchemaInfo{
		{Policy: "p0"},
		{Database: "db1", Table: model.InvolvingAll},
	}
	runnable = j.checkRunnable(0, failedInvolves)
	require.False(t, runnable)

	failedInvolves = []model.InvolvingSchemaInfo{
		{Database: model.InvolvingAll, Table: model.InvolvingAll},
		{ResourceGroup: "g0"},
	}
	runnable = j.checkRunnable(0, failedInvolves)
	require.False(t, runnable)

	jobID2 := int64(2)
	involves2 := []model.InvolvingSchemaInfo{
		{Database: "db2", Table: model.InvolvingAll},
		{Policy: "p0"},
		{ResourceGroup: "g0"},
	}
	runnable = j.checkRunnable(jobID2, involves2)
	require.True(t, runnable)
	j.addRunning(jobID2, involves2)

	jobID3 := int64(3)
	involves3 := []model.InvolvingSchemaInfo{
		{Policy: "p1"},
		{ResourceGroup: "g1"},
	}
	runnable = j.checkRunnable(jobID3, involves3)
	require.True(t, runnable)
	j.addRunning(jobID3, involves3)
	require.Equal(t, "1,2,3", orderedAllIDs(j.allIDs()))
	checkInvariants(t, j)

	failedInvolves = []model.InvolvingSchemaInfo{
		{ResourceGroup: "g0"},
	}
	runnable = j.checkRunnable(0, failedInvolves)
	require.False(t, runnable)

	j.removeRunning(jobID2, involves2)
	require.Equal(t, "1,3", orderedAllIDs(j.allIDs()))
	checkInvariants(t, j)

	jobID4 := int64(4)
	involves4 := []model.InvolvingSchemaInfo{
		{Policy: "p0"},
		{Database: "db3", Table: "t3"},
	}
	runnable = j.checkRunnable(jobID4, involves4)
	require.True(t, runnable)
	j.addRunning(jobID4, involves4)
	require.Equal(t, "1,3,4", orderedAllIDs(j.allIDs()))
	checkInvariants(t, j)

	failedInvolves = []model.InvolvingSchemaInfo{
		{Database: "db3", Table: "t3"},
	}
	runnable = j.checkRunnable(0, failedInvolves)
	require.False(t, runnable)
	failedInvolves = []model.InvolvingSchemaInfo{
		{Policy: "p1"},
	}
	runnable = j.checkRunnable(0, failedInvolves)
	require.False(t, runnable)
}

func TestExclusiveShared(t *testing.T) {
	j := newRunningJobs()

	jobID1, involves1 := mkJob(1, "db1.t1", "db1.t2")
	runnable := j.checkRunnable(jobID1, involves1)
	require.True(t, runnable)
	j.addRunning(jobID1, involves1)

	failedInvolves := []model.InvolvingSchemaInfo{
		{Database: "db2", Table: model.InvolvingAll},
		{Database: "db1", Table: "t1", Mode: model.SharedInvolving},
	}
	runnable = j.checkRunnable(0, failedInvolves)
	require.False(t, runnable)

	jobID2 := int64(2)
	involves2 := []model.InvolvingSchemaInfo{
		{Database: "db3", Table: model.InvolvingAll},
		{Database: "db2", Table: "t2", Mode: model.SharedInvolving},
	}
	runnable = j.checkRunnable(jobID2, involves2)
	require.True(t, runnable)
	j.addRunning(jobID2, involves2)

	jobID3 := int64(3)
	involves3 := []model.InvolvingSchemaInfo{
		{Database: "db4", Table: model.InvolvingAll},
		{Database: "db2", Table: "t2", Mode: model.SharedInvolving},
	}
	runnable = j.checkRunnable(jobID3, involves3)
	require.True(t, runnable)
	j.addRunning(jobID3, involves3)
	require.Equal(t, "1,2,3", orderedAllIDs(j.allIDs()))
	checkInvariants(t, j)

	pendingInvolves := []model.InvolvingSchemaInfo{
		{Database: "db2", Table: "t2"},
	}
	runnable = j.checkRunnable(0, pendingInvolves)
	require.False(t, runnable)
	j.addPending(pendingInvolves)
	require.Equal(t, "1,2,3", orderedAllIDs(j.allIDs()))
	checkInvariants(t, j)

	// because there's a pending job on db2.t2, next job on db2.t2 should be blocked
	jobID4 := int64(4)
	involves4 := []model.InvolvingSchemaInfo{
		{Database: "db100", Table: model.InvolvingAll},
		{Database: "db2", Table: "t2", Mode: model.SharedInvolving},
	}
	runnable = j.checkRunnable(jobID4, involves4)
	require.False(t, runnable)
	require.Equal(t, "1,2,3", orderedAllIDs(j.allIDs()))
	checkInvariants(t, j)

	// mimic all running job is finished and here's the next round to get jobs
	j.resetAllPending()
	j.removeRunning(jobID1, involves1)
	j.removeRunning(jobID2, involves2)
	j.removeRunning(jobID3, involves3)
	checkInvariants(t, j)

	runnable = j.checkRunnable(0, pendingInvolves)
	require.True(t, runnable)

	// new test round

	jobID5 := int64(5)
	involves5 := []model.InvolvingSchemaInfo{
		{Policy: "p1", Mode: model.SharedInvolving},
		{Policy: "p2", Mode: model.SharedInvolving},
	}
	runnable = j.checkRunnable(jobID5, involves5)
	require.True(t, runnable)
	j.addRunning(jobID5, involves5)

	jobID6 := int64(6)
	involves6 := []model.InvolvingSchemaInfo{
		{Policy: "p1", Mode: model.SharedInvolving},
		{ResourceGroup: "g1", Mode: model.SharedInvolving},
	}
	runnable = j.checkRunnable(jobID6, involves6)
	require.True(t, runnable)
	j.addRunning(jobID6, involves6)

	require.Equal(t, "5,6", orderedAllIDs(j.allIDs()))
	checkInvariants(t, j)

	pendingInvolves = []model.InvolvingSchemaInfo{
		{Policy: "p1"},
		{ResourceGroup: "g2"},
	}
	runnable = j.checkRunnable(0, pendingInvolves)
	require.False(t, runnable)
	j.addPending(pendingInvolves)

	secondPendingInvolves := []model.InvolvingSchemaInfo{
		{ResourceGroup: "g2"},
		{ResourceGroup: "g3"},
	}
	runnable = j.checkRunnable(0, secondPendingInvolves)
	require.False(t, runnable)
	j.addPending(secondPendingInvolves)

	// we have two shared p1 objects, test when one is finished and another round starts.

	j.removeRunning(jobID6, involves6)
	require.Equal(t, "5", orderedAllIDs(j.allIDs()))
	checkInvariants(t, j)
	j.resetAllPending()

	runnable = j.checkRunnable(0, pendingInvolves)
	require.False(t, runnable)
	j.addPending(pendingInvolves)
	runnable = j.checkRunnable(0, secondPendingInvolves)
	require.False(t, runnable)
	j.addPending(secondPendingInvolves)

	// all shared p1 objects is removed

	j.removeRunning(jobID5, involves5)
	require.Equal(t, "", orderedAllIDs(j.allIDs()))
	checkInvariants(t, j)

	// no p1 in exclusive and shared. But p1 exists in pending, so this job can not run
	thirdPendingInvolves := []model.InvolvingSchemaInfo{
		{Policy: "p1"},
	}
	runnable = j.checkRunnable(0, thirdPendingInvolves)
	require.False(t, runnable)
	j.addPending(thirdPendingInvolves)

	// now another round starts, the first pending job can run

	j.resetAllPending()
	runnable = j.checkRunnable(0, pendingInvolves)
	require.True(t, runnable)
}
