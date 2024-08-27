// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestLogKVConvertFailed(t *testing.T) {
	tempPath := filepath.Join(t.TempDir(), "/temp.txt")
	logCfg := &log.Config{File: tempPath, FileMaxSize: 1}
	err := log.InitLogger(logCfg, "info")
	require.NoError(t, err)

	modelName := model.NewCIStr("c1")
	modelState := model.StatePublic
	modelFieldType := *types.NewFieldType(mysql.TypeTiny)
	c1 := &model.ColumnInfo{ID: 1, Name: modelName, State: modelState, Offset: 0, FieldType: modelFieldType}
	cols := []*model.ColumnInfo{c1}
	tblInfo := &model.TableInfo{ID: 1, Columns: cols, PKIsHandle: false, State: model.StatePublic}
	var tbl table.Table
	tbl, err = tables.TableFromMeta(NewPanickingAllocators(tblInfo.SepAutoInc(), 0), tblInfo)
	require.NoError(t, err)

	var baseKVEncoder *BaseKVEncoder
	baseKVEncoder, err = NewBaseKVEncoder(&encode.EncodingConfig{
		Table: tbl,
		SessionOptions: encode.SessionOptions{
			SQLMode:   mysql.ModeStrictAllTables,
			Timestamp: 1234567890,
		},
		Logger: log.L(),
	})
	var newString strings.Builder
	for i := 0; i < 100000; i++ {
		newString.WriteString("test_test_test_test_")
	}
	newDatum := types.NewStringDatum(newString.String())
	rows := []types.Datum{}
	for i := 0; i <= 10; i++ {
		rows = append(rows, newDatum)
	}
	err = baseKVEncoder.LogKVConvertFailed(rows, 6, c1, err)
	require.NoError(t, err)

	var content []byte
	content, err = os.ReadFile(tempPath)
	require.NoError(t, err)
	require.LessOrEqual(t, 500, len(string(content)))
	require.NotContains(t, content, "exceeds maximum file size")
}
