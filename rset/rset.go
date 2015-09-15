// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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

package rset

import (
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
)

// Recordset is an abstract result set interface to help get data from Plan.
type Recordset interface {
	// Data filter for input data.
	Do(f func(data []interface{}) (more bool, err error)) error

	// Fields gets result fields.
	Fields() (fields []*field.ResultField, err error)

	// FirstRow gets first row data.
	FirstRow() (row []interface{}, err error)

	// Rows gets rows data by using limit/offset.
	// Negative limit means no limit.
	Rows(limit, offset int) (rows [][]interface{}, err error)

	// Next returns the next row, nil row means there is no more to return.
	Next() (row *plan.Row, err error)

	// Close closes the underlying iterator, call Next after Close will
	// restart the iteration.
	Close() error
}
