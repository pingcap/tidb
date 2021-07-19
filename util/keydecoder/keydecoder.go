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

	UnknownHandle HandleType = "unknown"
)

// DecodedKey is a struct contains detailed information about a key, its json form should be used to fill KEY_INFO field in `DEADLOCKS` and `DATA_LOCK_WAITS`
type DecodedKey struct {
	DbID              int64      `json:"db_id,omitempty"`
	DbName            string     `json:"db_name,omitempty"`
	TableID           int64      `json:"table_id"`
	TableName         string     `json:"table_name,omitempty"`
	HandleType        HandleType `json:"handle_type,omitempty"`
	IsPartitionHandle bool       `json:"partition_handle,omitempty"`
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
	return UnknownHandle
}

// DecodeKey decodes `key` into `DecodedKey`, which is used to fill KEY_INFO field in `DEADLOCKS` and `DATA_LOCK_WAITS`
func DecodeKey(key []byte, infoschema infoschema.InfoSchema) (DecodedKey, error) {
	var result DecodedKey
	tableID, indexID, isRecordKey, err := tablecodec.DecodeKeyHead(key)
	if err != nil {
		return result, err
	}
	result.TableID = tableID

	table, ok := infoschema.TableByID(tableID)
	if !ok {
		// The schema may have changed since when the key is get.
		// In this case we just omit the table name but show the table ID.
		return result, nil
	}
	result.TableName = table.Meta().Name.O

	schema, ok := infoschema.SchemaByTable(table.Meta())
	if !ok {
		return result, errors.Errorf("no schema associated with table which tableID=%d found", tableID)
	}
	result.DbID = schema.ID
	result.DbName = schema.Name.O

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
