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
	"fmt"

	"github.com/docker/go-units"
)

const (
	getRequestsField        = "get_requests"
	putRequestsField        = "put_requests"
	objStoreReadBytesField  = "obj_store_read_bytes"
	objStoreWriteBytesField = "obj_store_write_bytes"
	clusterReadBytesField   = "cluster_read_bytes"
	clusterWriteBytesField  = "cluster_write_bytes"

	// RowCountField represents the number of rows processed.
	RowCountField = "row_count"
	// DataKVBytesField represents the bytes of data KV ingested into the cluster.
	DataKVBytesField = "data_kv_bytes"
	// IndexKVBytesField represents the bytes of index KV ingested into the cluster.
	IndexKVBytesField = "index_kv_bytes"
	// ConcurrencyField represents the concurrency of the task.
	ConcurrencyField = "concurrency"
	// MaxNodeCountField represents the maximum number of nodes used during the task.
	MaxNodeCountField = "max_node_count"
	// DurationSecondsField represents the duration of the task in seconds.
	DurationSecondsField = "duration_seconds"
)

// Data represents the metering data.
// we use this struct to store accumulated data.
type Data struct {
	dataValues
	taskID   int64
	keyspace string
	taskType string
}

type dataValues struct {
	getRequests        uint64
	putRequests        uint64
	objStoreReadBytes  uint64
	objStoreWriteBytes uint64
	clusterReadBytes   uint64
	clusterWriteBytes  uint64
}

func (d *Data) equals(other *Data) bool {
	return d.dataValues == other.dataValues
}

func (d *Data) calMeterDataItem(other *Data) map[string]any {
	// since Data item is always monotonically increasing, so don't consider
	// negative delta here.
	if d.equals(other) {
		return nil
	}
	item := GetBaseMeterItem(d.taskID, d.keyspace, d.taskType)
	if diff := d.getRequests - other.getRequests; diff > 0 {
		item[getRequestsField] = diff
	}
	if diff := d.putRequests - other.putRequests; diff > 0 {
		item[putRequestsField] = diff
	}
	if diff := d.objStoreReadBytes - other.objStoreReadBytes; diff > 0 {
		item[objStoreReadBytesField] = diff
	}
	if diff := d.objStoreWriteBytes - other.objStoreWriteBytes; diff > 0 {
		item[objStoreWriteBytesField] = diff
	}
	if diff := d.clusterReadBytes - other.clusterReadBytes; diff > 0 {
		item[clusterReadBytesField] = diff
	}
	if diff := d.clusterWriteBytes - other.clusterWriteBytes; diff > 0 {
		item[clusterWriteBytesField] = diff
	}
	return item
}

// String implements fmt.Stringer interface.
func (d *Data) String() string {
	return fmt.Sprintf("{id: %d, keyspace: %s, type: %s, requests{get: %d, put: %d}, obj_store{r: %s, w: %s}, cluster{r: %s, w: %s}",
		d.taskID,
		d.keyspace,
		d.taskType,
		d.getRequests,
		d.putRequests,
		units.BytesSize(float64(d.objStoreReadBytes)),
		units.BytesSize(float64(d.objStoreWriteBytes)),
		units.BytesSize(float64(d.clusterReadBytes)),
		units.BytesSize(float64(d.clusterWriteBytes)),
	)
}

// GetBaseMeterItem returns the base metering data item.
func GetBaseMeterItem(taskID int64, keyspace, taskType string) map[string]any {
	return map[string]any{
		"version":     "1",
		"source_name": category,
		"task_id":     taskID,
		// in nextgen, cluster_id is used as the keyspace name.
		"cluster_id": keyspace,
		"task_type":  taskType,
	}
}
