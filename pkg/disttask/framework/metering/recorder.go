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

import "sync/atomic"

// Recorder is used to record metering data.
type Recorder struct {
	taskID      int64
	keyspace    string
	taskType    string
	getRequests atomic.Uint64
	putRequests atomic.Uint64
	readBytes   atomic.Uint64
	writeBytes  atomic.Uint64
}

// IncGetRequest records the get request count.
func (r *Recorder) IncGetRequest(v uint64) {
	r.getRequests.Add(v)
}

// IncPutRequest records the put request count.
func (r *Recorder) IncPutRequest(v uint64) {
	r.putRequests.Add(v)
}

// IncReadBytes records the read data bytes.
func (r *Recorder) IncReadBytes(v uint64) {
	r.readBytes.Add(v)
}

// IncWriteBytes records the write data bytes.
func (r *Recorder) IncWriteBytes(v uint64) {
	r.writeBytes.Add(v)
}

func (r *Recorder) currData() *Data {
	return &Data{
		taskID:      r.taskID,
		keyspace:    r.keyspace,
		taskType:    r.taskType,
		getRequests: r.getRequests.Load(),
		putRequests: r.putRequests.Load(),
		readBytes:   r.readBytes.Load(),
		writeBytes:  r.writeBytes.Load(),
	}
}
