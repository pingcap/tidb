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

package core

import (
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
)

// ExplainForConnection returns the minimal process info payload used to render
// EXPLAIN FOR CONNECTION on the caller side.
type ExplainForConnection struct {
	physicalSchemaProducer

	ConnectionID uint64
}

// NewExplainForConnection creates the internal cop plan used for remote
// EXPLAIN FOR CONNECTION lookups.
func NewExplainForConnection(ctx base.PlanContext, connectionID uint64) *ExplainForConnection {
	p := &ExplainForConnection{ConnectionID: connectionID}
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, "ExplainForConnection", p, 0)
	schema := newColumnsWithNames(3)
	schema.Append(buildColumnWithName("", "USER", mysql.TypeVarchar, mysql.MaxFieldVarCharLength))
	schema.Append(buildColumnWithName("", "HOST", mysql.TypeVarchar, mysql.MaxFieldVarCharLength))
	schema.Append(buildColumnWithName("", "BRIEF_BINARY_PLAN", mysql.TypeString, mysql.MaxBlobWidth))
	p.SetSchema(schema.col2Schema())
	return p
}
