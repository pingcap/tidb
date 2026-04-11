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

package rowcodec

import (
	"time"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// ExportedEncodeFromOldRow encodes a row from old (tablecodec) format to new (rowcodec) format.
// This is a helper function for testing.
func ExportedEncodeFromOldRow(encoder *Encoder, loc *time.Location, oldRow []byte, buf []byte) ([]byte, error) {
	if len(oldRow) > 0 && oldRow[0] == CodecVer {
		return oldRow, nil
	}
	encoder.reset()
	for len(oldRow) > 1 {
		var d types.Datum
		var err error
		oldRow, d, err = codec.DecodeOne(oldRow)
		if err != nil {
			return nil, err
		}
		colID := d.GetInt64()
		oldRow, d, err = codec.DecodeOne(oldRow)
		if err != nil {
			return nil, err
		}
		encoder.appendColVal(colID, &d)
	}
	numCols, notNullIdx := encoder.reformatCols()
	if err := encoder.encodeRowCols(loc, numCols, notNullIdx); err != nil {
		return nil, err
	}
	return encoder.row.toBytes(buf[:0]), nil
}

// ExportedGetColIDs returns the column IDs from the encoder.
// This is a helper method to access the internal column IDs.
func ExportedGetColIDs(encoder *Encoder) []int64 {
	return encoder.tempColIDs
}

// ExportedSetColIDs sets the column IDs for the encoder.
// This is a helper method to set the internal column IDs for testing.
func ExportedSetColIDs(encoder *Encoder, colIDs []int64) {
	encoder.tempColIDs = colIDs
}
