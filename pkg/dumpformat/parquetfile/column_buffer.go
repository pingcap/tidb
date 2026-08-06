// Copyright 2026 PingCAP, Inc.
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

package parquetfile

import (
	"fmt"

	"github.com/apache/arrow-go/v18/parquet"
)

type columnBuffer struct {
	defLevels               []int16
	boolValues              []bool
	int32Values             []int32
	int64Values             []int64
	float32Values           []float32
	float64Values           []float64
	byteArrayValues         []parquet.ByteArray
	fixedLenByteArrayValues []parquet.FixedLenByteArray
}

func (buffer *columnBuffer) reset() {
	buffer.defLevels = buffer.defLevels[:0]
	buffer.boolValues = buffer.boolValues[:0]
	buffer.int32Values = buffer.int32Values[:0]
	buffer.int64Values = buffer.int64Values[:0]
	buffer.float32Values = buffer.float32Values[:0]
	buffer.float64Values = buffer.float64Values[:0]
	buffer.byteArrayValues = buffer.byteArrayValues[:0]
	buffer.fixedLenByteArrayValues = buffer.fixedLenByteArrayValues[:0]
}

func newColumnBuffers(columns []column, capacity int) ([]columnBuffer, error) {
	buffers := make([]columnBuffer, len(columns))
	for i := range columns {
		buffer, err := newColumnBuffer(columns[i], capacity)
		if err != nil {
			return nil, fmt.Errorf("init parquet buffer for column %s: %w", columns[i].Name, err)
		}
		buffers[i] = buffer
	}
	return buffers, nil
}

func newColumnBuffer(column column, capacity int) (columnBuffer, error) {
	buffer := columnBuffer{}
	if column.allowsNullEncoding {
		buffer.defLevels = make([]int16, 0, capacity)
	}

	switch column.Physical {
	case parquet.Types.Boolean:
		buffer.boolValues = make([]bool, 0, capacity)
	case parquet.Types.Int32:
		buffer.int32Values = make([]int32, 0, capacity)
	case parquet.Types.Int64:
		buffer.int64Values = make([]int64, 0, capacity)
	case parquet.Types.Float:
		buffer.float32Values = make([]float32, 0, capacity)
	case parquet.Types.Double:
		buffer.float64Values = make([]float64, 0, capacity)
	case parquet.Types.ByteArray:
		buffer.byteArrayValues = make([]parquet.ByteArray, 0, capacity)
	case parquet.Types.FixedLenByteArray:
		if column.TypeLength <= 0 {
			return columnBuffer{}, fmt.Errorf("invalid fixed-size byte width %d", column.TypeLength)
		}
		buffer.fixedLenByteArrayValues = make([]parquet.FixedLenByteArray, 0, capacity)
	default:
		return columnBuffer{}, fmt.Errorf("unsupported parquet physical type %s", column.Physical)
	}
	return buffer, nil
}
