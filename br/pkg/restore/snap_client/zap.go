// Copyright 2024 PingCAP, Inc.
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

package snapclient

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ZapTables make zap field of table for debuging, including table names.
func zapTables(tables []CreatedTable) zapcore.Field {
	return logutil.AbbreviatedArray("tables", tables, func(input any) []string {
		tables := input.([]CreatedTable)
		names := make([]string, 0, len(tables))
		for _, t := range tables {
			names = append(names, fmt.Sprintf("%s.%s",
				utils.EncloseName(t.OldTable.DB.Name.String()),
				utils.EncloseName(t.OldTable.Info.Name.String())))
		}
		return names
	})
}

type zapTableIDWithFilesMarshaler []TableIDWithFiles

func zapTableIDWithFiles(fs []TableIDWithFiles) zap.Field {
	return zap.Object("files", zapTableIDWithFilesMarshaler(fs))
}

func (fs zapTableIDWithFilesMarshaler) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	for _, f := range fs {
		encoder.AddInt64("table-id", f.TableID)
		if err := logutil.MarshalLogObjectForFiles(f.Files, encoder); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
