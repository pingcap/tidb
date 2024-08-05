// Copyright 2024 PingCAP, Inc.
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

package session

import (
	planctx "github.com/pingcap/tidb/pkg/planner/context"
	planctximpl "github.com/pingcap/tidb/pkg/planner/contextimpl"
)

var _ planctx.PlanContext = &planContextImpl{}

// planContextImpl implements the PlanContext interface.
// Because there is some force casting between `PlanContext` and some other interfaces in the codebase, we have to embed
// the `session` here to make it safe for casting.
type planContextImpl struct {
	*session
	*planctximpl.PlanCtxExtendedImpl
}

// NewPlanContextImpl creates a new PlanContextImpl.
func newPlanContextImpl(s *session) *planContextImpl {
	return &planContextImpl{
		session:             s,
		PlanCtxExtendedImpl: planctximpl.NewPlanCtxExtendedImpl(s),
	}
}
