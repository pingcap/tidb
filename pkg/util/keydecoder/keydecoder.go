// Copyright 2021 PingCAP, Inc.
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

package keydecoder

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// HandleType is the type of a Handle, could be `int`(ie. kv.IntHandle) or `common`(ie. *kv.CommonHandle)
type HandleType string

// HandleType instances
const (
	IntHandle    HandleType = "int"
	CommonHandle HandleType = "common"

	UnknownHandle HandleType = "unknown"
)

// DecodedKey is a struct contains detailed information about a key, its json form should be used to fill KEY_INFO field in `DEADLOCKS` and `DATA_LOCK_WAITS`
type DecodedKey struct {
	DbName            string     `json:"db_name,omitempty"`
	TableName         string     `json:"table_name,omitempty"`
	PartitionName     string     `json:"partition_name,omitempty"`
	HandleType        HandleType `json:"handle_type,omitempty"`
	HandleValue       string     `json:"handle_value,omitempty"`
	IndexName         string     `json:"index_name,omitempty"`
	IndexValues       []string   `json:"index_values,omitempty"`
	DbID              int64      `json:"db_id,omitempty"`
	TableID           int64      `json:"table_id"`
	PartitionID       int64      `json:"partition_id,omitempty"`
	IndexID           int64      `json:"index_id,omitempty"`
	IsPartitionHandle bool       `json:"partition_handle,omitempty"`
}

func handleType(handle kv.Handle) HandleType {
	if _, ok := handle.(kv.IntHandle); ok {
		return IntHandle
	} else if _, ok := handle.(*kv.CommonHandle); ok {
		return CommonHandle
	} else if h, ok := handle.(kv.PartitionHandle); ok {
		return handleType(h.Handle)
	} else if h, ok := handle.(*kv.PartitionHandle); ok {
		return handleType(h.Handle)
	}
	logutil.BgLogger().Warn("Unexpected kv.Handle type",
		zap.String("handle Type", fmt.Sprintf("%T", handle)),
	)
	return UnknownHandle
}

// DecodeKey decodes `key` (either a Record key or an Index key) into `DecodedKey`, which is used to fill KEY_INFO field in `DEADLOCKS` and `DATA_LOCK_WAITS`
func DecodeKey(key []byte, is infoschema.InfoSchema) (DecodedKey, error) {
	var result DecodedKey
	if !tablecodec.IsRecordKey(key) && !tablecodec.IsIndexKey(key) {
		return result, errors.Errorf("Unknown key type for key %v", key)
	}
	tableOrPartitionID, indexID, isRecordKey, err := tablecodec.DecodeKeyHead(key)
	if err != nil {
		return result, err
	}
	result.TableID = tableOrPartitionID

	table, tableFound := is.TableByID(tableOrPartitionID)

	// The schema may have changed since when the key is get.
	// Then we just omit the table name and show the table ID only.
	// Otherwise, we can show the table name and table ID.
	if tableFound {
		result.TableName = table.Meta().Name.O

		schema, ok := infoschema.SchemaByTable(is, table.Meta())
		if !ok {
			logutil.BgLogger().Warn("no schema associated with table found in infoschema", zap.Int64("tableOrPartitionID", tableOrPartitionID))
			return result, nil
		}
		result.DbID = schema.ID
		result.DbName = schema.Name.O
	} else {
		// If the table of this ID is not found, try to find it as a partition.
		var schema *model.DBInfo
		var partition *model.PartitionDefinition
		table, schema, partition = is.FindTableByPartitionID(tableOrPartitionID)
		if table != nil {
			tableFound = true
			result.TableID = table.Meta().ID
			result.TableName = table.Meta().Name.O
		}
		if schema != nil {
			result.DbID = schema.ID
			result.DbName = schema.Name.O
		}
		if partition != nil {
			result.PartitionID = partition.ID
			result.PartitionName = partition.Name.O
		}
		if !tableFound {
			logutil.BgLogger().Warn("no table found in infoschema", zap.Int64("tableOrPartitionID", tableOrPartitionID))
		}
	}
	if isRecordKey {
		_, handle, err := tablecodec.DecodeRecordKey(key)
		if err != nil {
			logutil.BgLogger().Warn("decode record key failed", zap.Int64("tableOrPartitionID", tableOrPartitionID), zap.Error(err))
			return result, errors.Errorf("cannot decode record key of table %d", tableOrPartitionID)
		}
		result.HandleType = handleType(handle)
		// The PartitionHandle is used by the Global Index feature for partition tables, which is currently an
		// unfinished feature. So we don't care about it much for now.
		_, result.IsPartitionHandle = handle.(kv.PartitionHandle)
		result.HandleValue = handle.String()
	} else {
		// is index key
		_, _, indexValues, err := tablecodec.DecodeIndexKey(key)
		if err != nil {
			logutil.BgLogger().Warn("cannot decode index key", zap.ByteString("key", key), zap.Error(err))
			return result, nil
		}
		result.IndexID = indexID
		if tableFound {
			for _, index := range table.Indices() {
				if index.Meta().ID == indexID {
					result.IndexName = index.Meta().Name.O
					result.IndexValues = indexValues
					break
				}
			}
		}
	}
	return result, nil
}
