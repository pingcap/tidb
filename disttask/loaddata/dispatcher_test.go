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

package loaddata

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type loadDataSuite struct {
	suite.Suite
}

func TestLoadData(t *testing.T) {
	suite.Run(t, &loadDataSuite{})
}

func (s *loadDataSuite) enableFailPoint(path, term string) {
	require.NoError(s.T(), failpoint.Enable(path, term))
	s.T().Cleanup(func() {
		_ = failpoint.Disable(path)
	})
}

func (s *loadDataSuite) TestFlowHandleGetEligibleInstances() {
	makeFailpointRes := func(v interface{}) string {
		bytes, err := json.Marshal(v)
		s.NoError(err)
		return fmt.Sprintf("return(`%s`)", string(bytes))
	}
	uuids := []string{"ddl_id_1", "ddl_id_2"}
	serverInfoMap := map[string]*infosync.ServerInfo{
		uuids[0]: {
			ID: uuids[0],
		},
		uuids[1]: {
			ID: uuids[1],
		},
	}
	mockedAllServerInfos := makeFailpointRes(serverInfoMap)

	h := flowHandle{}
	gTask := &proto.Task{Meta: []byte("{}")}
	s.enableFailPoint("github.com/pingcap/tidb/domain/infosync/mockGetAllServerInfo", mockedAllServerInfos)
	eligibleInstances, err := h.GetEligibleInstances(context.Background(), gTask)
	s.NoError(err)
	// order of slice is not stable, change to map
	resultMap := map[string]*infosync.ServerInfo{}
	for _, ins := range eligibleInstances {
		resultMap[ins.ID] = ins
	}
	s.Equal(serverInfoMap, resultMap)

	gTask.Meta = []byte(`{"EligibleInstances":[{"ip": "1.1.1.1", "listening_port": 4000}]}`)
	eligibleInstances, err = h.GetEligibleInstances(context.Background(), gTask)
	s.NoError(err)
	s.Equal([]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}}, eligibleInstances)
}

func (s *loadDataSuite) TestUpdateCurrentTask() {
	taskMeta := TaskMeta{
		Plan: importer.Plan{
			DisableTiKVImportMode: true,
		},
	}
	bs, err := json.Marshal(taskMeta)
	require.NoError(s.T(), err)

	h := flowHandle{}
	require.Equal(s.T(), int64(0), h.currTaskID.Load())
	require.False(s.T(), h.disableTiKVImportMode.Load())

	h.updateCurrentTask(&proto.Task{
		ID:   1,
		Meta: bs,
	})
	require.Equal(s.T(), int64(1), h.currTaskID.Load())
	require.True(s.T(), h.disableTiKVImportMode.Load())

	h.updateCurrentTask(&proto.Task{
		ID:   1,
		Meta: bs,
	})
	require.Equal(s.T(), int64(1), h.currTaskID.Load())
	require.True(s.T(), h.disableTiKVImportMode.Load())
}
