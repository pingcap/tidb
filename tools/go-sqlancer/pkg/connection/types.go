// Copyright 2022 PingCAP, Inc.
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

package connection

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/juju/errors"
)

// QueryItem define query result
type QueryItem struct {
	Null      bool
	ValType   *sql.ColumnType
	ValString string
}

// QueryItems is a list of QueryItem
type QueryItems []*QueryItem

func (q *QueryItem) String() string {
	var strValue = q.ValString
	switch strings.ToUpper(q.ValType.DatabaseTypeName()) {
	case "VARCHAR", "CHAR", "TEXT":
		strValue = fmt.Sprintf("'%s'", q.ValString)
	}
	if q.Null {
		strValue = "NULL"
	}
	return fmt.Sprintf("%s is %s type", strValue, q.ValType.DatabaseTypeName())
}

// StringWithoutType is to become string without type information
func (q *QueryItem) StringWithoutType() string {
	if q.Null {
		return "NULL"
	}
	return q.ValString
}

// MustSame compare tow QueryItem and return error if not same
func (q *QueryItem) MustSame(q1 *QueryItem) error {
	if (q == nil) != (q1 == nil) {
		return errors.Errorf("one is nil but another is not, self: %t, another: %t", q == nil, q1 == nil)
	}

	if q.Null != q1.Null {
		return errors.Errorf("one is NULL but another is not, self: %t, another: %t", q.Null, q1.Null)
	}

	if q.Null && q1.Null {
		return nil
	}

	if q.ValType.Name() != q1.ValType.Name() {
		return errors.Errorf("column type diff, self: %s, another: %s", q.ValType.Name(), q1.ValType.Name())
	}

	if q.ValString != q1.ValString {
		return errors.Errorf("column data diff, self: %s, another: %s", q.ValString, q1.ValString)
	}

	return nil
}
