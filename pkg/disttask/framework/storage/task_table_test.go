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

package storage

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestSplitSubtasks(t *testing.T) {
	tm := &TaskManager{}
	subtasks := make([]*proto.Subtask, 0, 10)
	metaBytes := make([]byte, 100)
	for i := 0; i < 10; i++ {
		subtasks = append(subtasks, &proto.Subtask{SubtaskBase: proto.SubtaskBase{ID: int64(i)}, Meta: metaBytes})
	}
	bak := kv.TxnTotalSizeLimit.Load()
	t.Cleanup(func() {
		kv.TxnTotalSizeLimit.Store(bak)
	})

	kv.TxnTotalSizeLimit.Store(config.SuperLargeTxnSize)
	splitSubtasks := tm.splitSubtasks(subtasks)
	require.Len(t, splitSubtasks, 1)
	require.Equal(t, subtasks, splitSubtasks[0])

	maxSubtaskBatchSize = 300
	splitSubtasks = tm.splitSubtasks(subtasks)
	require.Len(t, splitSubtasks, 4)
	require.Equal(t, subtasks[:3], splitSubtasks[0])
	require.Equal(t, subtasks[3:6], splitSubtasks[1])
	require.Equal(t, subtasks[6:9], splitSubtasks[2])
	require.Equal(t, subtasks[9:], splitSubtasks[3])
}
