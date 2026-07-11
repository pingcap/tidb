// Copyright 2026 PingCAP, Inc.
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

package parsedef

import (
	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap/zapcore"
)

// Row is the content of a row.
type Row struct {
	// RowID is the row id of the row.
	// as objects of this struct is reused, this RowID is increased when reading
	// next row.
	RowID  int64
	Row    []types.Datum
	Length int
}

// MarshalLogArray implements the zapcore.ArrayMarshaler interface
func (row Row) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for _, r := range row.Row {
		encoder.AppendString(r.String())
	}
	return nil
}
