package keydecoder

import (
	"testing"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/stretchr/testify/assert"
)

func TestDecodeKey(t *testing.T) {
	table.MockTableFromMeta = tables.MockTableFromMeta
	tableInfo1 := &model.TableInfo{ID: 1, Name: model.NewCIStr("table1")}
	stubTableInfos := []*model.TableInfo{tableInfo1}
	stubInfoschema := infoschema.MockInfoSchema(stubTableInfos)

	decodedKey, err := DecodeKey([]byte{
		't',
		// table id = 1
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
		'_',
		'r',
		// row id = 1
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
	}, stubInfoschema)
	assert.Nil(t, err)
	assert.Equal(t, decodedKey.DbID, int64(0))
	assert.Equal(t, decodedKey.DbName, "test")
	assert.Equal(t, decodedKey.TableID, int64(1))
	assert.Equal(t, decodedKey.TableName, "table1")
	assert.Equal(t, decodedKey.HandleType, IntHandle)
	assert.Equal(t, decodedKey.PartitionHandle, false)
	assert.Equal(t, decodedKey.HandleValue, "1")
}
