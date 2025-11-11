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

import "fmt"

const (
	getRequestsField = "get_requests"
	putRequestsField = "put_requests"
	readBytesField   = "read_bytes"
	writeBytesField  = "write_bytes"
)

// Data represents the metering data.
// we might use this struct to store accumulated data.
type Data struct {
	getRequests uint64
	putRequests uint64
	readBytes   uint64
	writeBytes  uint64

	taskID   int64
	keyspace string
	taskType string
}

func (d *Data) equals(other *Data) bool {
	return d.getRequests == other.getRequests &&
		d.putRequests == other.putRequests &&
		d.readBytes == other.readBytes &&
		d.writeBytes == other.writeBytes
}

func (d *Data) calMeterDataItem(other *Data) map[string]any {
	// since Data item is always monotonically increasing, so don't consider
	// negative delta here.
	if d.equals(other) {
		return nil
	}
	item := map[string]any{
		"version":     "1",
		"cluster_id":  d.keyspace,
		"source_name": category,
		"task_type":   d.taskType,
		"task_id":     d.taskID,
	}
	if d.getRequests > other.getRequests {
		item[getRequestsField] = d.getRequests - other.getRequests
	}
	if d.putRequests > other.putRequests {
		item[putRequestsField] = d.putRequests - other.putRequests
	}
	if d.readBytes > other.readBytes {
		item[readBytesField] = d.readBytes - other.readBytes
	}
	if d.writeBytes > other.writeBytes {
		item[writeBytesField] = d.writeBytes - other.writeBytes
	}
	return item
}

// String implements fmt.Stringer interface.
func (d *Data) String() string {
	return fmt.Sprintf("{taskID: %d, keyspace: %s, type: %s, getReqs: %d, putReqs: %d, readBytes: %d, writeBytes: %d}",
		d.taskID,
		d.keyspace,
		d.taskType,
		d.getRequests,
		d.putRequests,
		d.readBytes,
		d.writeBytes)
}
