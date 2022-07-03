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
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/types"
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
	var plans []*basePhysicalPlan
	partWindow := &PhysicalWindow{
		// Meaningless sort item, just for test.
		PartitionBy: []property.SortItem{sortItem},
	}
	// nonPartWindow := &PhysicalWindow {}
	partialSort := &PhysicalSort{
		IsPartialSort: true,
	}
	sort := &PhysicalSort{}
	recv := &PhysicalExchangeReceiver{}
	hashSender := &PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_Hash,
	}
	plans = append(plans, &partWindow.basePhysicalPlan)
	plans = append(plans, &partialSort.basePhysicalPlan)
	plans = append(plans, &sort.basePhysicalPlan)
	plans = append(plans, &recv.basePhysicalPlan)
	plans = append(plans, &hashSender.basePhysicalPlan)
	clear := func(plans []*basePhysicalPlan) {
		for _, p := range plans {
			p.children = nil
			p.TiFlashFineGrainedShuffleStreamCount = 0
		}
	}
	var check func(p PhysicalPlan, exp_stream_count int64, exp_child_count int, cur_child_count int)
	check = func(p PhysicalPlan, exp_stream_count int64, exp_child_count int, cur_child_count int) {
		if p == nil {
			return
		}
		if len(p.Children()) == 0 {
			require.Equal(t, exp_child_count, cur_child_count)
		}
		val := reflect.ValueOf(p)
		act_stream_count := reflect.Indirect(val).FieldByName("TiFlashFineGrainedShuffleStreamCount").Interface().(uint64)
		require.Equal(t, uint64(exp_stream_count), act_stream_count)
		for _, child := range p.Children() {
			check(child, exp_stream_count, exp_child_count, cur_child_count+1)
		}
	}

	const exp_stream_count int64 = 8
	sctx := MockContext()
	sctx.GetSessionVars().TiFlashFineGrainedShuffleStreamCount = exp_stream_count

	// Window <- Sort <- ExchangeReceiver <- ExchangeSender
	partWindow.children = []PhysicalPlan{partialSort}
	partialSort.children = []PhysicalPlan{recv}
	recv.children = []PhysicalPlan{hashSender}
	handleFineGrainedShuffle(sctx, partWindow)
	check(partWindow, exp_stream_count, 4, 1)
	clear(plans)

	// Window <- ExchangeReceiver <- ExchangeSender
	partWindow.children = []PhysicalPlan{recv}
	recv.children = []PhysicalPlan{hashSender}
	handleFineGrainedShuffle(sctx, partWindow)
	check(partWindow, exp_stream_count, 3, 1)
	clear(plans)

	// Window <- Sort(x) <- ExchangeReceiver <- ExchangeSender
	// Fine-grained shuffle is disabled because sort is not partial.
	partWindow.children = []PhysicalPlan{sort}
	sort.children = []PhysicalPlan{recv}
	recv.children = []PhysicalPlan{hashSender}
	handleFineGrainedShuffle(sctx, partWindow)
	check(partWindow, 0, 4, 1)
	clear(plans)

	// Window <- Sort <- Window <- Sort <- ExchangeReceiver <- ExchangeSender
	partWindow1 := &PhysicalWindow{
		// Meaningless sort item, just for test.
		PartitionBy: []property.SortItem{sortItem},
	}
	partialSort1 := &PhysicalSort{
		IsPartialSort: true,
	}
	partWindow.children = []PhysicalPlan{partialSort}
	partialSort.children = []PhysicalPlan{partWindow1}
	partWindow1.children = []PhysicalPlan{partialSort1}
	partialSort1.children = []PhysicalPlan{recv}
	recv.children = []PhysicalPlan{hashSender}
	handleFineGrainedShuffle(sctx, partWindow)
	check(partWindow, exp_stream_count, 6, 1)
	clear(plans)

	// Window <- Sort <- Window(x) <- Sort <- ExchangeReceiver <- ExchangeSender
	// Fine-grained shuffle is disabled because Window is not hash partition.
	partWindow1 = &PhysicalWindow{}
	partialSort1 = &PhysicalSort{
		IsPartialSort: true,
	}
	partWindow.children = []PhysicalPlan{partialSort}
	partialSort.children = []PhysicalPlan{partWindow1}
	partWindow1.children = []PhysicalPlan{partialSort1}
	partialSort1.children = []PhysicalPlan{recv}
	recv.children = []PhysicalPlan{hashSender}
	handleFineGrainedShuffle(sctx, partWindow)
	check(partWindow, 0, 6, 1)
	clear(plans)

	// HashAgg <- Window <- ExchangeReceiver <- ExchangeSender
	hashAgg := &PhysicalHashAgg{}
	hashAgg.children = []PhysicalPlan{partWindow}
	partWindow.children = []PhysicalPlan{recv}
	recv.children = []PhysicalPlan{hashSender}
	handleFineGrainedShuffle(sctx, partWindow)
	require.Equal(t, uint64(0), hashAgg.TiFlashFineGrainedShuffleStreamCount)
	check(partWindow, exp_stream_count, 3, 1)
	clear(plans)

	// Window <- HashAgg <- ExchangeReceiver <- ExchangeSender
	hashAgg = &PhysicalHashAgg{}
	partWindow.children = []PhysicalPlan{hashAgg}
	hashAgg.children = []PhysicalPlan{recv}
	recv.children = []PhysicalPlan{hashSender}
	handleFineGrainedShuffle(sctx, partWindow)
	check(partWindow, 0, 4, 1)
	clear(plans)
}
