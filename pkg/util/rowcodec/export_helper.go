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
)

// ExportedEncodeFromOldRow encodes a row from old format to new format.
// This is a helper function for testing that decodes an old-format row
// and re-encodes it using the new encoder.
func ExportedEncodeFromOldRow(encoder *Encoder, loc *time.Location, oldRow []byte, buf []byte) ([]byte, error) {
	// Decode the old row to get the values
	decoder := NewDatumMapDecoder(nil, loc)
	datumMap, err := decoder.DecodeToDatumMap(oldRow, nil)
	if err != nil {
		return nil, err
	}

	// Convert datum map to slice
	colIDs := make([]int64, 0, len(datumMap))
	values := make([]types.Datum, 0, len(datumMap))
	for colID, datum := range datumMap {
		colIDs = append(colIDs, colID)
		values = append(values, datum)
	}

	// Encode using the new encoder
	return encoder.Encode(loc, colIDs, values, NoChecksum{}, buf)
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
