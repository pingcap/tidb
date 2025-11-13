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
	taskID       int64
	keyspace     string
	taskType     string
	objStoreReqs recording.Requests
	traffic      recording.Traffic
}

// MergeObjStoreRequests merges the object store requests from another Requests.
func (r *Recorder) MergeObjStoreRequests(other *recording.Requests) {
	r.objStoreReqs.Merge(other)
}

// IncReadBytes records the read data bytes.
func (r *Recorder) IncReadBytes(v uint64) {
	r.traffic.Read.Add(v)
}

// IncWriteBytes records the write data bytes.
func (r *Recorder) IncWriteBytes(v uint64) {
	r.traffic.Write.Add(v)
}

func (r *Recorder) currData() *Data {
	return &Data{
		taskID:      r.taskID,
		keyspace:    r.keyspace,
		taskType:    r.taskType,
		getRequests: r.objStoreReqs.Get.Load(),
		putRequests: r.objStoreReqs.Put.Load(),
		readBytes:   r.traffic.Read.Load(),
		writeBytes:  r.traffic.Write.Load(),
	}
}
