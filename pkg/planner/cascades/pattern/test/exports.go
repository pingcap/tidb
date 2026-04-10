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

package pattern_test

import (
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
)

// Exported constants and functions from parent package for use in tests.
var (
	EngineAll     = pattern.ExportEngineAll
	EngineTiDB    = pattern.ExportEngineTiDB
	EngineTiKV    = pattern.ExportEngineTiKV
	EngineTiFlash = pattern.ExportEngineTiFlash

	EngineTiDBOnly      = pattern.ExportEngineTiDBOnly
	EngineTiKVOnly      = pattern.ExportEngineTiKVOnly
	EngineTiFlashOnly   = pattern.ExportEngineTiFlashOnly
	EngineTiKVOrTiFlash = pattern.ExportEngineTiKVOrTiFlash

	OperandAny         = pattern.ExportOperandAny
	OperandJoin        = pattern.ExportOperandJoin
	OperandAggregation = pattern.ExportOperandAggregation
	OperandProjection  = pattern.ExportOperandProjection
	OperandSelection   = pattern.ExportOperandSelection
	OperandApply       = pattern.ExportOperandApply
	OperandMaxOneRow   = pattern.ExportOperandMaxOneRow
	OperandTableDual   = pattern.ExportOperandTableDual
	OperandDataSource  = pattern.ExportOperandDataSource
	OperandUnionScan   = pattern.ExportOperandUnionScan
	OperandUnionAll    = pattern.ExportOperandUnionAll
	OperandSort        = pattern.ExportOperandSort
	OperandTopN        = pattern.ExportOperandTopN
	OperandLock        = pattern.ExportOperandLock
	OperandLimit       = pattern.ExportOperandLimit

	GetOperand = pattern.ExportGetOperand
	NewPattern = pattern.ExportNewPattern
)
