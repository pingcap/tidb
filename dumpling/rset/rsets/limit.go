// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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

package rsets

import (
	"fmt"

	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
)

var (
	_ plan.Planner = (*LimitRset)(nil)
	_ plan.Planner = (*OffsetRset)(nil)
)

// OffsetRset is record set for limit offset.
type OffsetRset struct {
	Count uint64
	Src   plan.Plan
}

// Plan gets OffsetDefaultPlan.
func (r *OffsetRset) Plan(ctx context.Context) (plan.Plan, error) {
	return &plans.OffsetDefaultPlan{Count: r.Count, Src: r.Src, Fields: r.Src.GetFields()}, nil
}

func (r *OffsetRset) String() string {
	return fmt.Sprintf(" OFFSET %d", r.Count)
}

// LimitRset is record set for limit.
type LimitRset struct {
	Count uint64
	Src   plan.Plan
}

// Plan gets LimitDefaultPlan.
func (r *LimitRset) Plan(ctx context.Context) (plan.Plan, error) {
	return &plans.LimitDefaultPlan{Count: r.Count, Src: r.Src, Fields: r.Src.GetFields()}, nil
}

func (r *LimitRset) String() string {
	return fmt.Sprintf(" LIMIT %d", r.Count)
}
