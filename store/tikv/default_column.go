// Copyright 2016 PingCAP, Inc.
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

package tikv

import "github.com/pingcap/kvproto/pkg/kvrpcpb"

// This file contains utilities for compatibility with old KV interface (no columns).
// TODO: Remove this file after migrating old KV interface.

var defaultColumn = [][]byte{nil}

func defaultRow(rowKey []byte) *kvrpcpb.Row {
	return &kvrpcpb.Row{
		RowKey:  rowKey,
		Columns: defaultColumn,
	}
}

func defaultRowValue(rowVal *kvrpcpb.RowValue) []byte {
	if rowVal == nil || len(rowVal.GetValues()) == 0 {
		return nil
	}
	return rowVal.GetValues()[0]
}
