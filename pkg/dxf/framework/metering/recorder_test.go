// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metering

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecorder(t *testing.T) {
	r := Recorder{taskID: 1, keyspace: "ks", taskType: "tt"}
	r.objStoreAccess.Requests.Get.Add(100)
	r.objStoreAccess.Requests.Put.Add(200)
	r.objStoreAccess.RecRead(11)
	r.objStoreAccess.RecWrite(22)
	r.IncClusterReadBytes(300)
	r.IncClusterWriteBytes(400)
	require.Equal(t, &Data{
		taskID: 1, keyspace: "ks", taskType: "tt",
		dataValues: dataValues{
			getRequests: 100, putRequests: 200,
			objStoreReadBytes: 11, objStoreWriteBytes: 22,
			clusterReadBytes: 300, clusterWriteBytes: 400,
		},
	}, r.currData())
}
