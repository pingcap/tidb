// Copyright 2020 PingCAP, Inc.
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

package util

import (
	"fmt"
	"strings"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/util/size"
)

// ByItems wraps a "by" item.
type ByItems struct {
	Expr expression.Expression
	Desc bool
}

// String implements fmt.Stringer interface.
func (by *ByItems) String() string {
	return by.StringWithCtx(perrors.RedactLogDisable)
}

// StringWithCtx implements expression.StringerWithCtx interface.
func (by *ByItems) StringWithCtx(redact string) string {
	if by.Desc {
		return fmt.Sprintf("%s true", by.Expr.StringWithCtx(redact))
	}
	return by.Expr.StringWithCtx(redact)
}

// Clone makes a copy of ByItems.
func (by *ByItems) Clone() *ByItems {
	return &ByItems{Expr: by.Expr.Clone(), Desc: by.Desc}
}

// Equal checks whether two ByItems are equal.
func (by *ByItems) Equal(ctx expression.EvalContext, other *ByItems) bool {
	return by.Expr.Equal(ctx, other.Expr) && by.Desc == other.Desc
}

// MemoryUsage return the memory usage of ByItems.
func (by *ByItems) MemoryUsage() (sum int64) {
	if by == nil {
		return
	}

	sum = size.SizeOfBool
	if by.Expr != nil {
		sum += by.Expr.MemoryUsage()
	}
	return sum
}

// StringifyByItemsWithCtx is used to print ByItems slice.
func StringifyByItemsWithCtx(byItems []*ByItems) string {
	sb := strings.Builder{}
	sb.WriteString("[")
	for i, item := range byItems {
		sb.WriteString(item.StringWithCtx(perrors.RedactLogDisable))
		if i != len(byItems)-1 {
			sb.WriteString(" ")
		}
	}
	sb.WriteString("]")
	return sb.String()
}
