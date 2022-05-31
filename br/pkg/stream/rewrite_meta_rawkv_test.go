// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream_test

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func ProduceValue(tableName string, dbID int64) ([]byte, error) {
	tableInfo := model.TableInfo{
		ID:   dbID,
		Name: model.NewCIStr(tableName),
	}

	return json.Marshal(tableInfo)
}

func TestRewriteValueForTable(t *testing.T) {
	var (
		tableName  = "person"
		tableID    = 57
		newTableID = 63
	)

	v, err := ProduceValue(tableName, int64(tableID))
	require.NoError(t, err)
	log.Info("old-value", zap.Int("value-len", len(v)), zap.ByteString("old-value", v), logutil.Key("old-value", v))

	v, err = ProduceValue(tableName, int64(newTableID))
	require.NoError(t, err)
	log.Info("new-value", zap.Int("value-len", len(v)), zap.ByteString("new-value", v), logutil.Key("new-value", v))
}
