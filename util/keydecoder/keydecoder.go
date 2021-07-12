package keydecoder

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type HandleType string

const (
	IntHandle    = "int"
	CommonHandle = "common"
)

type DecodedKey struct {
	DbId            int64      `json:"db_id"`
	DbName          string     `json:"db_name"`
	TableId         int64      `json:"table_id"`
	TableName       string     `json:"table_name"`
	HandleType      HandleType `json:"handle_type"`
	PartitionHandle bool       `json:"partition_handle"`
	HandleValue     string     `json:"handle_value,omitempty"`
	IndexId         int64      `json:"index_id,omitempty"`
	IndexName       string     `json:"index_name,omitempty"`
	IndexValues     []string   `json:"index_values,omitempty"`
}

func handleType(handle kv.Handle) HandleType {
	if _, ok := handle.(kv.IntHandle); ok {
		return IntHandle
	} else if _, ok := handle.(*kv.CommonHandle); ok {
		return CommonHandle
	} else if h, ok := handle.(kv.PartitionHandle); ok {
		return handleType(h.Handle)
	} else {
		logutil.BgLogger().Warn("Unexpected kv.Handle type",
			zap.String("handle", fmt.Sprintf("%T", handle)))
	}
	return ""
}

func DecodeKey(key []byte, infoschema infoschema.InfoSchema) (DecodedKey, error) {
	var result DecodedKey
	tableId, indexId, isRecordKey, err := tablecodec.DecodeKeyHead(key)
	if err != nil {
		return result, err
	}
	table, ok := infoschema.TableByID(tableId)
	if !ok {
		return result, errors.Errorf("no table associated which id=%d found", tableId)
	}
	schema, ok := infoschema.SchemaByTable(table.Meta())
	if !ok {
		return result, errors.Errorf("no schema associated with table which tableId=%d found", tableId)
	}

	result.TableId = tableId
	result.DbId = schema.ID
	result.DbName = schema.Name.O
	result.TableName = table.Meta().Name.O

	if isRecordKey {
		_, handle, err := tablecodec.DecodeRecordKey(key)
		if err != nil {
			return result, errors.Errorf("cannot decode record key of table %d", tableId)
		}
		result.HandleType = handleType(handle)
		_, result.PartitionHandle = handle.(*kv.PartitionHandle)
		result.HandleValue = handle.String()
	} else {
		// is index key
		_, _, indexValues, err := tablecodec.DecodeIndexKey(key)
		if err != nil {
			return result, errors.Errorf("cannot decode index key of table %d", tableId)
		}
		result.IndexId = indexId
		for _, index := range table.Indices() {
			if index.Meta().ID == indexId {
				result.IndexName = index.Meta().Name.O
				result.IndexValues = indexValues
				break
			}
		}
	}
	return result, nil
}
