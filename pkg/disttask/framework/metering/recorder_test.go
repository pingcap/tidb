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
	r.objStoreReqs.Get.Add(100)
	r.objStoreReqs.Put.Add(200)
	r.IncReadBytes(300)
	r.IncWriteBytes(400)
	require.Equal(t, &Data{
		taskID: 1, keyspace: "ks", taskType: "tt",
		getRequests: 100, putRequests: 200,
		readBytes: 300, writeBytes: 400,
	}, r.currData())
}
