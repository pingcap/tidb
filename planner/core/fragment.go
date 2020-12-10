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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tipb/go-tipb"
)

// Fragment is cut from the whole pushed-down plan by network communication.
// Communication by pfs are always through shuffling / broadcasting / passing through.
type Fragment struct {
	p PhysicalPlan

	// following field are filled during getPlanFragment.
	// TODO: Strictly speaking, not all plan fragment contain table scan. we can do this assumption until more plans are supported.
	TableScan         *PhysicalTableScan          // result physical table scan
	ExchangeReceivers []*PhysicalExchangeReceiver // data receivers

	// following fields are filled after scheduling.
	ExchangeSender *PhysicalExchangeSender // data exporter
}

// Schema is the output schema of the current plan fragment.
func (f *Fragment) Schema() *expression.Schema {
	return f.p.Schema()
}

// GetRootPlanFragments will cut and generate all the plan fragments which is divided by network communication.
// Then return the root plan fragment.
func GetRootPlanFragments(ctx sessionctx.Context, p PhysicalPlan, startTS uint64) *Fragment {
	tidbTask := &kv.MPPTask{
		StartTs: startTS,
		ID:      -1,
	}
	rootPf := &Fragment{
		p:              p,
		ExchangeSender: &PhysicalExchangeSender{ExchangeType: tipb.ExchangeType_PassThrough, Tasks: []*kv.MPPTask{tidbTask}},
	}
	rootPf.ExchangeSender.InitBasePlan(ctx, plancodec.TypeExchangeSender)
	rootPf.ExchangeSender.SetChildren(rootPf.p)
	getPlanFragments(ctx, p, rootPf)
	return rootPf
}

// getPlanFragment passes the plan and which fragment the plan belongs to, then walk through the plan recursively.
// When we found an edge can be cut, we will add exchange operators and construct new fragment.
func getPlanFragments(ctx sessionctx.Context, p PhysicalPlan, pf *Fragment) {
	switch x := p.(type) {
	case *PhysicalTableScan:
		x.IsGlobalRead = false
		pf.TableScan = x
	case *PhysicalBroadCastJoin:
		// This is a fragment cutter. So we replace broadcast side with a exchangerClient
		bcChild := x.Children()[x.InnerChildIdx]
		exchangeSender := &PhysicalExchangeSender{ExchangeType: tipb.ExchangeType_Broadcast}
		exchangeSender.InitBasePlan(ctx, plancodec.TypeExchangeSender)
		npf := &Fragment{p: bcChild, ExchangeSender: exchangeSender}
		exchangeSender.SetChildren(npf.p)

		exchangeReceivers := &PhysicalExchangeReceiver{
			ChildPf: npf,
		}
		exchangeReceivers.InitBasePlan(ctx, plancodec.TypeExchangeReceiver)
		x.Children()[x.InnerChildIdx] = exchangeReceivers
		pf.ExchangeReceivers = append(pf.ExchangeReceivers, exchangeReceivers)

		// For the inner side of join, we use a new plan fragment.
		getPlanFragments(ctx, bcChild, npf)
		getPlanFragments(ctx, x.Children()[1-x.InnerChildIdx], pf)
	default:
		if len(x.Children()) > 0 {
			getPlanFragments(ctx, x.Children()[0], pf)
		}
	}
}
