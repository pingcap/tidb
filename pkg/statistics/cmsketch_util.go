// Copyright 2023 PingCAP, Inc.
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

package statistics

import (
	"time"

	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

func topNMetaToDatum(val TopNMeta,
	ft *types.FieldType, isIndex bool, loc *time.Location) (dat types.Datum, err error) {
	if isIndex {
		dat.SetBytes(val.Encoded)
		return dat, nil
	}
	if _, dat, err = codec.DecodeOne(val.Encoded); err != nil {
		return dat, err
	}
	// The key encodes a value in its flattened form: ENUM, SET and BIT
	// as their numeric value, times as a packed integer, TypeFloat as a
	// float64. Unflatten restores the kind the column's own values
	// carry, which matters because Datum.Compare dispatches on kind and
	// because a histogram's chunk column is typed.
	return tablecodec.Unflatten(dat, ft, loc)
}
