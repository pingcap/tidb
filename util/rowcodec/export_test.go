// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package rowcodec

import (
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// EncodeFromOldRow encodes a row from an old-format row.
// this method will be used in test.
func EncodeFromOldRow(encoder *Encoder, sc *stmtctx.StatementContext, oldRow, buf []byte) ([]byte, error) {
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
		encoder.appendColVal(colID, d)
	}
	numCols, notNullIdx := encoder.reformatCols()
	err := encoder.encodeRowCols(sc, numCols, notNullIdx)
	if err != nil {
		return nil, err
	}
	return encoder.row.toBytes(buf[:0]), nil
}
