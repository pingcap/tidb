// Copyright 2019 PingCAP, Inc.
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
	"reflect"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

// LogicalOptimize exports the `logicalOptimize` function for test packages and
// doesn't affect the normal package and access control of Golang (tricky ^_^)
var LogicalOptimize = logicalOptimize

func testDecimalConvert(t *testing.T, lDec, lLen, rDec, rLen int, lConvert, rConvert bool, cDec, cLen int) {
	lType := types.NewFieldType(mysql.TypeNewDecimal)
	lType.SetDecimal(lDec)
	lType.SetFlen(lLen)

	rType := types.NewFieldType(mysql.TypeNewDecimal)
	rType.SetDecimal(rDec)
	rType.SetFlen(rLen)

	cType, lCon, rCon := negotiateCommonType(lType, rType)
	require.Equal(t, mysql.TypeNewDecimal, cType.GetType())
	require.Equal(t, cDec, cType.GetDecimal())
	require.Equal(t, cLen, cType.GetFlen())
	require.Equal(t, lConvert, lCon)
	require.Equal(t, rConvert, rCon)
}

func TestMPPDecimalConvert(t *testing.T) {
	testDecimalConvert(t, 5, 9, 5, 8, false, false, 5, 9)
	testDecimalConvert(t, 5, 8, 5, 9, false, false, 5, 9)
	testDecimalConvert(t, 0, 8, 0, 11, true, false, 0, 11)
	testDecimalConvert(t, 0, 16, 0, 11, false, false, 0, 16)
	testDecimalConvert(t, 5, 9, 4, 9, true, true, 5, 10)
	testDecimalConvert(t, 5, 8, 4, 9, true, true, 5, 10)
	testDecimalConvert(t, 5, 9, 4, 8, false, true, 5, 9)
	testDecimalConvert(t, 10, 16, 0, 11, true, true, 10, 21)
	testDecimalConvert(t, 5, 19, 0, 20, false, true, 5, 25)
	testDecimalConvert(t, 20, 20, 0, 60, true, true, 20, 65)
	testDecimalConvert(t, 20, 40, 0, 60, false, true, 20, 65)
	testDecimalConvert(t, 0, 40, 0, 60, false, false, 0, 60)
}

func testJoinKeyTypeConvert(t *testing.T, leftType, rightType, retType *types.FieldType, lConvert, rConvert bool) {
	cType, lCon, rCon := negotiateCommonType(leftType, rightType)
	require.Equal(t, retType.GetType(), cType.GetType())
	require.Equal(t, retType.GetFlen(), cType.GetFlen())
	require.Equal(t, retType.GetDecimal(), cType.GetDecimal())
	require.Equal(t, retType.GetFlag(), cType.GetFlag())
	require.Equal(t, lConvert, lCon)
	require.Equal(t, rConvert, rCon)
}

func TestMPPJoinKeyTypeConvert(t *testing.T) {
	tinyIntType := types.NewFieldTypeBuilder().SetType(mysql.TypeTiny).BuildP()
	flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeTiny)
	tinyIntType.SetFlen(flen)
	tinyIntType.SetDecimal(decimal)

	unsignedTinyIntType := types.NewFieldTypeBuilder().SetType(mysql.TypeTiny).BuildP()
	unsignedTinyIntType.SetFlen(flen)
	unsignedTinyIntType.SetDecimal(decimal)
	unsignedTinyIntType.SetFlag(mysql.UnsignedFlag)

	bigIntType := types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP()
	flen, decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	bigIntType.SetFlen(flen)
	bigIntType.SetDecimal(decimal)

	unsignedBigIntType := types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP()
	unsignedBigIntType.SetFlen(flen)
	unsignedBigIntType.SetDecimal(decimal)
	unsignedBigIntType.SetFlag(mysql.UnsignedFlag)

	decimalType := types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).SetFlen(20).SetDecimal(0).BuildP()

	testJoinKeyTypeConvert(t, tinyIntType, tinyIntType, tinyIntType, false, false)
	testJoinKeyTypeConvert(t, tinyIntType, unsignedTinyIntType, bigIntType, true, true)
	testJoinKeyTypeConvert(t, tinyIntType, bigIntType, bigIntType, true, false)
	testJoinKeyTypeConvert(t, bigIntType, tinyIntType, bigIntType, false, true)
	testJoinKeyTypeConvert(t, unsignedBigIntType, tinyIntType, decimalType, true, true)
	testJoinKeyTypeConvert(t, tinyIntType, unsignedBigIntType, decimalType, true, true)
	testJoinKeyTypeConvert(t, bigIntType, bigIntType, bigIntType, false, false)
	testJoinKeyTypeConvert(t, unsignedBigIntType, bigIntType, decimalType, true, true)
	testJoinKeyTypeConvert(t, bigIntType, unsignedBigIntType, decimalType, true, true)
}

// Test for core.handleFineGrainedShuffle()
func TestHandleFineGrainedShuffle(t *testing.T) {
	sortItem := property.SortItem{
		Col:  nil,
		Desc: true,
	}
	var plans []*physicalop.BasePhysicalPlan
	tableReader := &PhysicalTableReader{}
	partWindow := &PhysicalWindow{
		// Meaningless sort item, just for test.
		PartitionBy: []property.SortItem{sortItem},
	}
	partialSort := &PhysicalSort{
		IsPartialSort: true,
	}
	sort := &PhysicalSort{}
	recv := &PhysicalExchangeReceiver{}
	passSender := &PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_PassThrough,
	}
	hashSender := &PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_Hash,
	}
	tableScan := &PhysicalTableScan{}
	plans = append(plans, &partWindow.BasePhysicalPlan)
	plans = append(plans, &partialSort.BasePhysicalPlan)
	plans = append(plans, &sort.BasePhysicalPlan)
	plans = append(plans, &recv.BasePhysicalPlan)
	plans = append(plans, &hashSender.BasePhysicalPlan)
	clearFunc := func(plans []*physicalop.BasePhysicalPlan) {
		for _, p := range plans {
			p.SetChildren(nil)
			p.TiFlashFineGrainedShuffleStreamCount = 0
		}
	}
	var check func(p base.PhysicalPlan, expStreamCount int64, expChildCount int, curChildCount int)
	check = func(p base.PhysicalPlan, expStreamCount int64, expChildCount int, curChildCount int) {
		if len(p.Children()) == 0 {
			require.Equal(t, expChildCount, curChildCount)
			_, isTableScan := p.(*PhysicalTableScan)
			require.True(t, isTableScan)
			return
		}
		val := reflect.ValueOf(p)
		actStreamCount := reflect.Indirect(val).FieldByName("TiFlashFineGrainedShuffleStreamCount").Interface().(uint64)
		require.Equal(t, uint64(expStreamCount), actStreamCount)
		for _, child := range p.Children() {
			check(child, expStreamCount, expChildCount, curChildCount+1)
		}
	}

	const expStreamCount int64 = 8
	sctx := MockContext()
	sctx.GetSessionVars().TiFlashFineGrainedShuffleStreamCount = expStreamCount
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()
	start := func(p base.PhysicalPlan, expStreamCount int64, expChildCount int, curChildCount int) {
		handleFineGrainedShuffle(nil, sctx.GetPlanCtx(), tableReader)
		check(p, expStreamCount, expChildCount, curChildCount)
		clearFunc(plans)
	}

	// Window <- Sort <- ExchangeReceiver <- ExchangeSender
	tableReader.tablePlan = passSender
	passSender.SetChildren([]base.PhysicalPlan{partWindow}...)
	partWindow.SetChildren([]base.PhysicalPlan{partialSort}...)
	partialSort.SetChildren([]base.PhysicalPlan{recv}...)
	recv.SetChildren([]base.PhysicalPlan{hashSender}...)
	hashSender.SetChildren([]base.PhysicalPlan{tableScan}...)
	start(partWindow, expStreamCount, 4, 0)

	// Window <- ExchangeReceiver <- ExchangeSender
	tableReader.tablePlan = passSender
	passSender.SetChildren([]base.PhysicalPlan{partWindow}...)
	partWindow.SetChildren([]base.PhysicalPlan{recv}...)
	recv.SetChildren([]base.PhysicalPlan{hashSender}...)
	hashSender.SetChildren([]base.PhysicalPlan{tableScan}...)
	start(partWindow, expStreamCount, 3, 0)

	// Window <- Sort(x) <- ExchangeReceiver <- ExchangeSender
	// Fine-grained shuffle is disabled because sort is not partial.
	tableReader.tablePlan = passSender
	passSender.SetChildren([]base.PhysicalPlan{partWindow}...)
	partWindow.SetChildren([]base.PhysicalPlan{sort}...)
	sort.SetChildren([]base.PhysicalPlan{recv}...)
	recv.SetChildren([]base.PhysicalPlan{hashSender}...)
	hashSender.SetChildren([]base.PhysicalPlan{tableScan}...)
	start(partWindow, 0, 4, 0)

	// Window <- Sort <- Window <- Sort <- ExchangeReceiver <- ExchangeSender
	partWindow1 := &PhysicalWindow{
		// Meaningless sort item, just for test.
		PartitionBy: []property.SortItem{sortItem},
	}
	partialSort1 := &PhysicalSort{
		IsPartialSort: true,
	}
	tableReader.tablePlan = passSender
	passSender.SetChildren([]base.PhysicalPlan{partWindow}...)
	partWindow.SetChildren([]base.PhysicalPlan{partialSort}...)
	partialSort.SetChildren([]base.PhysicalPlan{partWindow1}...)
	partWindow1.SetChildren([]base.PhysicalPlan{partialSort1}...)
	partialSort1.SetChildren([]base.PhysicalPlan{recv}...)
	recv.SetChildren([]base.PhysicalPlan{hashSender}...)
	hashSender.SetChildren([]base.PhysicalPlan{tableScan}...)
	start(partWindow, expStreamCount, 6, 0)

	// Window <- Sort <- Window(x) <- Sort <- ExchangeReceiver <- ExchangeSender(x)
	// Fine-grained shuffle is disabled because Window is not hash partition.
	nonPartWindow := &PhysicalWindow{}
	partialSort1 = &PhysicalSort{
		IsPartialSort: true,
	}
	tableReader.tablePlan = passSender
	passSender.SetChildren([]base.PhysicalPlan{partWindow}...)
	partWindow.SetChildren([]base.PhysicalPlan{partialSort}...)
	partialSort.SetChildren([]base.PhysicalPlan{nonPartWindow}...)
	nonPartWindow.SetChildren([]base.PhysicalPlan{partialSort1}...)
	partialSort1.SetChildren([]base.PhysicalPlan{recv}...)
	recv.SetChildren([]base.PhysicalPlan{passSender}...)
	passSender.SetChildren([]base.PhysicalPlan{tableScan}...)
	start(partWindow, 0, 6, 0)

	// HashAgg <- Window <- ExchangeReceiver <- ExchangeSender
	hashAgg := &PhysicalHashAgg{}
	tableReader.tablePlan = passSender
	passSender.SetChildren([]base.PhysicalPlan{hashAgg}...)
	hashAgg.SetChildren([]base.PhysicalPlan{partWindow}...)
	partWindow.SetChildren([]base.PhysicalPlan{recv}...)
	recv.SetChildren([]base.PhysicalPlan{hashSender}...)
	hashSender.SetChildren([]base.PhysicalPlan{tableScan}...)
	require.Equal(t, uint64(0), hashAgg.TiFlashFineGrainedShuffleStreamCount)
	start(partWindow, expStreamCount, 3, 0)

	// Window <- HashAgg(x) <- ExchangeReceiver <- ExchangeSender
	tableReader.tablePlan = passSender
	passSender.SetChildren([]base.PhysicalPlan{partWindow}...)
	hashAgg = &PhysicalHashAgg{}
	partWindow.SetChildren([]base.PhysicalPlan{hashAgg}...)
	hashAgg.SetChildren([]base.PhysicalPlan{recv}...)
	recv.SetChildren([]base.PhysicalPlan{hashSender}...)
	hashSender.SetChildren([]base.PhysicalPlan{tableScan}...)
	start(partWindow, 0, 4, 0)

	// Window <- Join(x) <- ExchangeReceiver <- ExchangeSender
	//                   <- ExchangeReceiver <- ExchangeSender
	tableReader.tablePlan = passSender
	passSender.SetChildren([]base.PhysicalPlan{partWindow}...)
	hashJoin := &PhysicalHashJoin{}
	recv1 := &PhysicalExchangeReceiver{}
	tableScan1 := &PhysicalTableScan{}
	partWindow.SetChildren([]base.PhysicalPlan{hashJoin}...)
	hashSender1 := &PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_Hash,
	}
	hashJoin.SetChildren([]base.PhysicalPlan{recv, recv1}...)
	recv.SetChildren([]base.PhysicalPlan{hashSender}...)
	recv1.SetChildren([]base.PhysicalPlan{hashSender1}...)
	hashSender.SetChildren([]base.PhysicalPlan{tableScan}...)
	hashSender1.SetChildren([]base.PhysicalPlan{tableScan1}...)
	start(partWindow, 0, 4, 0)

	// Join <- ExchangeReceiver <- ExchangeSender <- Window <- ExchangeReceiver(2) <- ExchangeSender(2)
	//      <- ExchangeReceiver(1) <- ExchangeSender(1)
	tableReader.tablePlan = passSender
	passSender.SetChildren([]base.PhysicalPlan{partWindow}...)
	hashJoin = &PhysicalHashJoin{}
	recv1 = &PhysicalExchangeReceiver{}
	hashJoin.SetChildren([]base.PhysicalPlan{recv, recv1}...)
	recv.SetChildren([]base.PhysicalPlan{hashSender}...)
	hashSender.SetChildren([]base.PhysicalPlan{partWindow}...)
	recv2 := &PhysicalExchangeReceiver{}
	hashSender2 := &PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_Hash,
	}
	tableScan2 := &PhysicalTableScan{}
	partWindow.SetChildren([]base.PhysicalPlan{recv2}...)
	recv2.SetChildren([]base.PhysicalPlan{hashSender2}...)
	hashSender2.SetChildren([]base.PhysicalPlan{tableScan2}...)
	recv1.SetChildren([]base.PhysicalPlan{hashSender1}...)
	tableScan1 = &PhysicalTableScan{}
	hashSender1.SetChildren([]base.PhysicalPlan{tableScan1}...)
	start(partWindow, expStreamCount, 3, 0)

	instances := []string{
		"tiflash,127.0.0.1:3933,127.0.0.1:7777,,",
		"tikv,127.0.0.1:11080,127.0.0.1:10080,,",
	}
	fpName := "github.com/pingcap/tidb/pkg/infoschema/mockStoreServerInfo"
	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	require.NoError(t, failpoint.Enable(fpName, fpExpr))
	defer func() { require.NoError(t, failpoint.Disable(fpName)) }()
	fpName2 := "github.com/pingcap/tidb/pkg/planner/core/mockTiFlashStreamCountUsingMinLogicalCores"
	require.NoError(t, failpoint.Enable(fpName2, `return("16")`))
	sctx.GetSessionVars().TiFlashFineGrainedShuffleStreamCount = 0

	col0 := &expression.Column{
		UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	cond, err := expression.NewFunction(sctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), col0, col0)
	require.True(t, err == nil)
	sf, isSF := cond.(*expression.ScalarFunction)
	require.True(t, isSF)
	var partitionCols = make([]*property.MPPPartitionColumn, 0, 1)
	partitionCols = append(partitionCols, &property.MPPPartitionColumn{
		Col:       col0,
		CollateID: property.GetCollateIDByNameForPartition(col0.GetType(sctx).GetCollate()),
	})

	// HashAgg(x) <- ExchangeReceiver <- ExchangeSender
	tableReader.tablePlan = passSender
	hashAgg = &PhysicalHashAgg{}
	passSender.SetChildren([]base.PhysicalPlan{hashAgg}...)
	hashAgg.SetChildren([]base.PhysicalPlan{recv}...)
	recv.SetChildren([]base.PhysicalPlan{hashSender}...)
	hashSender.SetChildren([]base.PhysicalPlan{tableScan}...)
	tableScan.Schema().Columns = append(tableScan.Schema().Columns, col0)
	start(hashAgg, 16, 3, 0)

	// Join(x) <- ExchangeReceiver <- ExchangeSender
	//                   <- ExchangeReceiver <- ExchangeSender
	tableReader.tablePlan = passSender
	hashJoin = &PhysicalHashJoin{}
	hashJoin.EqualConditions = append(hashJoin.EqualConditions, sf)
	hashJoin.RightJoinKeys = append(hashJoin.RightJoinKeys, col0)
	hashJoin.InnerChildIdx = 1
	passSender.SetChildren([]base.PhysicalPlan{hashJoin}...)
	recv = &PhysicalExchangeReceiver{}
	recv1 = &PhysicalExchangeReceiver{}
	tableScan = &PhysicalTableScan{}
	tableScan1 = &PhysicalTableScan{}
	hashSender = &PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_Hash,
	}
	hashSender1 = &PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_Hash,
	}
	hashJoin.SetChildren([]base.PhysicalPlan{recv, recv1}...)
	recv.SetChildren([]base.PhysicalPlan{hashSender}...)
	recv1.SetChildren([]base.PhysicalPlan{hashSender1}...)
	hashSender.SetChildren([]base.PhysicalPlan{tableScan}...)
	hashSender1.SetChildren([]base.PhysicalPlan{tableScan1}...)
	hashSender1.HashCols = partitionCols
	tableScan1.Schema().Columns = append(tableScan1.Schema().Columns, col0)
	handleFineGrainedShuffle(nil, sctx.GetPlanCtx(), tableReader)
	require.Equal(t, uint64(16), hashJoin.TiFlashFineGrainedShuffleStreamCount)
	require.Equal(t, uint64(16), recv1.TiFlashFineGrainedShuffleStreamCount)
	require.Equal(t, uint64(16), hashSender1.TiFlashFineGrainedShuffleStreamCount)
	require.Equal(t, uint64(0), recv.TiFlashFineGrainedShuffleStreamCount)
	require.Equal(t, uint64(0), hashSender.TiFlashFineGrainedShuffleStreamCount)
	clearFunc(plans)

	require.NoError(t, failpoint.Disable(fpName2))
	require.NoError(t, failpoint.Enable(fpName2, `return("8000")`))
	// HashAgg(x) <- ExchangeReceiver <- ExchangeSender， exceed splitLimit
	tableReader.tablePlan = passSender
	hashAgg = &PhysicalHashAgg{}
	passSender.SetChildren([]base.PhysicalPlan{hashAgg}...)
	hashAgg.SetChildren([]base.PhysicalPlan{recv}...)
	recv.SetChildren([]base.PhysicalPlan{hashSender}...)
	hashSender.SetChildren([]base.PhysicalPlan{tableScan}...)
	tableScan.Schema().Columns = append(tableScan.Schema().Columns, col0)
	start(hashAgg, 0, 3, 0)

	// exceed splitLimit
	// Join(x) <- ExchangeReceiver <- ExchangeSender
	//                   <- ExchangeReceiver <- ExchangeSender
	tableReader.tablePlan = passSender
	hashJoin = &PhysicalHashJoin{}
	hashJoin.EqualConditions = append(hashJoin.EqualConditions, sf)
	hashJoin.LeftJoinKeys = append(hashJoin.LeftJoinKeys, col0)
	hashJoin.InnerChildIdx = 1
	passSender.SetChildren([]base.PhysicalPlan{hashJoin}...)
	recv1 = &PhysicalExchangeReceiver{}
	tableScan1 = &PhysicalTableScan{}
	hashSender1 = &PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_Hash,
	}
	hashJoin.SetChildren([]base.PhysicalPlan{recv, recv1}...)
	recv.SetChildren([]base.PhysicalPlan{hashSender}...)
	recv1.SetChildren([]base.PhysicalPlan{hashSender1}...)
	hashSender.SetChildren([]base.PhysicalPlan{tableScan}...)
	hashSender1.SetChildren([]base.PhysicalPlan{tableScan1}...)
	hashSender1.HashCols = partitionCols
	tableScan1.Schema().Columns = append(tableScan1.Schema().Columns, col0)
	start(hashJoin, 0, 3, 0)
	require.NoError(t, failpoint.Disable(fpName2))
}
