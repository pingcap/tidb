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

// HandleType is the type of a Handle, could be `int`(ie. kv.IntHandle) or `common`(ie. *kv.CommonHandle)
type HandleType string

// HandleType instances
const (
	IntHandle    HandleType = "int"
	CommonHandle HandleType = "common"
)

// DecodedKey is a struct contains detailed information about a key, its json form should be used to fill KEY_INFO field in `DEADLOCKS` and `DATA_LOCK_WAITS`
type DecodedKey struct {
	DbID              int64      `json:"db_id"`
	DbName            string     `json:"db_name"`
	TableID           int64      `json:"table_id"`
	TableName         string     `json:"table_name"`
	HandleType        HandleType `json:"handle_type"`
	IsPartitionHandle bool       `json:"partition_handle"`
	HandleValue       string     `json:"handle_value,omitempty"`
	IndexID           int64      `json:"index_id,omitempty"`
	IndexName         string     `json:"index_name,omitempty"`
	IndexValues       []string   `json:"index_values,omitempty"`
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
	} else {
		logutil.BgLogger().Warn("Unexpected kv.Handle type",
			zap.Any("handle", handle),
			zap.String("handle Type", fmt.Sprintf("%T", handle)),
		)
	}
	return ""
}

// DecodeKey decodes `key` into `DecodedKey`, which is used to fill KEY_INFO field in `DEADLOCKS` and `DATA_LOCK_WAITS`
func DecodeKey(key []byte, infoschema infoschema.InfoSchema) (DecodedKey, error) {
	var result DecodedKey
	tableID, indexID, isRecordKey, err := tablecodec.DecodeKeyHead(key)
	if err != nil {
		return result, err
	}
	table, ok := infoschema.TableByID(tableID)
	if !ok {
		return result, errors.Errorf("no table associated which id=%d found", tableID)
	}
	schema, ok := infoschema.SchemaByTable(table.Meta())
	if !ok {
		return result, errors.Errorf("no schema associated with table which tableID=%d found", tableID)
	}

	result.TableID = tableID
	result.DbID = schema.ID
	result.DbName = schema.Name.O
	result.TableName = table.Meta().Name.O

	if isRecordKey {
		_, handle, err := tablecodec.DecodeRecordKey(key)
		if err != nil {
			return result, errors.Errorf("cannot decode record key of table %d", tableID)
		}
		result.HandleType = handleType(handle)
		_, result.IsPartitionHandle = handle.(kv.PartitionHandle)
		result.HandleValue = handle.String()
	} else {
		// is index key
		_, _, indexValues, err := tablecodec.DecodeIndexKey(key)
		if err != nil {
			return result, errors.Errorf("cannot decode index key of table %d", tableID)
		}
		result.IndexID = indexID
		for _, index := range table.Indices() {
			if index.Meta().ID == indexID {
				result.IndexName = index.Meta().Name.O
				result.IndexValues = indexValues
				break
			}
		}
	}
	return result, nil
}
