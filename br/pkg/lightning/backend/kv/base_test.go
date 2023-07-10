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

package kv_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestLogKVConvertFailed(t *testing.T) {
	dir := "/temp.txt"
	dir = fmt.Sprintf("%s%s", t.TempDir(), dir)
	logCfg := &log.Config{File: dir, FileMaxSize: 1}
	err := log.InitLogger(logCfg, "info")
	require.NoError(t, err)
	msg := "logger is initialized"
	log.L().Info(msg)

	c1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("c1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeTiny)}
	cols := []*model.ColumnInfo{c1}
	tblInfo := &model.TableInfo{ID: 1, Columns: cols, PKIsHandle: false, State: model.StatePublic}
	var tbl table.Table
	tbl, err = tables.TableFromMeta(kv.NewPanickingAllocators(0), tblInfo)
	require.NoError(t, err)

	var baseKVEncoder *kv.BaseKVEncoder
	baseKVEncoder, err = kv.NewBaseKVEncoder(&encode.EncodingConfig{
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
	content, err = os.ReadFile(dir)
	require.NoError(t, err)
	require.LessOrEqual(t, 500, len(string(content)))
}
