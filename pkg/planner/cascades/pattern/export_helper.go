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

package pattern

// Exported constants and types for testing in subpackages.
// This file provides access to unexported identifiers for the pattern_test package.

// ExportEngineAll exports EngineAll for testing.
var ExportEngineAll = EngineAll

// ExportEngineTiDB exports EngineTiDB for testing.
var ExportEngineTiDB = EngineTiDB

// ExportEngineTiKV exports EngineTiKV for testing.
var ExportEngineTiKV = EngineTiKV

// ExportEngineTiFlash exports EngineTiFlash for testing.
var ExportEngineTiFlash = EngineTiFlash

// ExportEngineTiDBOnly exports EngineTiDBOnly for testing.
var ExportEngineTiDBOnly = EngineTiDBOnly

// ExportEngineTiKVOnly exports EngineTiKVOnly for testing.
var ExportEngineTiKVOnly = EngineTiKVOnly

// ExportEngineTiFlashOnly exports EngineTiFlashOnly for testing.
var ExportEngineTiFlashOnly = EngineTiFlashOnly

// ExportEngineTiKVOrTiFlash exports EngineTiKVOrTiFlash for testing.
var ExportEngineTiKVOrTiFlash = EngineTiKVOrTiFlash

// ExportOperandAny exports OperandAny for testing.
var ExportOperandAny = OperandAny

// ExportOperandJoin exports OperandJoin for testing.
var ExportOperandJoin = OperandJoin

// ExportOperandAggregation exports OperandAggregation for testing.
var ExportOperandAggregation = OperandAggregation

// ExportOperandProjection exports OperandProjection for testing.
var ExportOperandProjection = OperandProjection

// ExportOperandSelection exports OperandSelection for testing.
var ExportOperandSelection = OperandSelection

// ExportOperandApply exports OperandApply for testing.
var ExportOperandApply = OperandApply

// ExportOperandMaxOneRow exports OperandMaxOneRow for testing.
var ExportOperandMaxOneRow = OperandMaxOneRow

// ExportOperandTableDual exports OperandTableDual for testing.
var ExportOperandTableDual = OperandTableDual

// ExportOperandDataSource exports OperandDataSource for testing.
var ExportOperandDataSource = OperandDataSource

// ExportOperandUnionScan exports OperandUnionScan for testing.
var ExportOperandUnionScan = OperandUnionScan

// ExportOperandUnionAll exports OperandUnionAll for testing.
var ExportOperandUnionAll = OperandUnionAll

// ExportOperandSort exports OperandSort for testing.
var ExportOperandSort = OperandSort

// ExportOperandTopN exports OperandTopN for testing.
var ExportOperandTopN = OperandTopN

// ExportOperandLock exports OperandLock for testing.
var ExportOperandLock = OperandLock

// ExportOperandLimit exports OperandLimit for testing.
var ExportOperandLimit = OperandLimit

// ExportGetOperand exports GetOperand function for testing.
var ExportGetOperand = GetOperand

// ExportNewPattern exports NewPattern function for testing.
var ExportNewPattern = NewPattern
