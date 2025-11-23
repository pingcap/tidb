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
	"github.com/pingcap/tidb/br/pkg/storage/recording"
)

// Recorder is used to record metering data.
type Recorder struct {
	taskID         int64
	keyspace       string
	taskType       string
	objStoreAccess recording.AccessStats
	clusterTraffic recording.Traffic
}

// MergeObjStoreAccess merges the object store requests from another Requests.
func (r *Recorder) MergeObjStoreAccess(other *recording.AccessStats) {
	r.objStoreAccess.Merge(other)
}

// IncClusterReadBytes records the read data bytes from cluster.
func (r *Recorder) IncClusterReadBytes(n uint64) {
	r.clusterTraffic.Read.Add(n)
}

// IncClusterWriteBytes records the write data bytes to cluster.
func (r *Recorder) IncClusterWriteBytes(n uint64) {
	r.clusterTraffic.Write.Add(n)
}

func (r *Recorder) currData() *Data {
	return &Data{
		taskID:   r.taskID,
		keyspace: r.keyspace,
		taskType: r.taskType,
		dataValues: dataValues{
			getRequests:        r.objStoreAccess.Requests.Get.Load(),
			putRequests:        r.objStoreAccess.Requests.Put.Load(),
			objStoreReadBytes:  r.objStoreAccess.Traffic.Read.Load(),
			objStoreWriteBytes: r.objStoreAccess.Traffic.Write.Load(),
			clusterReadBytes:   r.clusterTraffic.Read.Load(),
			clusterWriteBytes:  r.clusterTraffic.Write.Load(),
		},
	}
}
