// Copyright 2025 PingCAP, Inc.
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

package physicalop

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PhysicalShuffle represents a shuffle plan.
// `Tails` and `DataSources` are the last plan within and the first plan following the "shuffle", respectively,
//
//	to build the child executors chain.
//
// Take `Window` operator for example:
//
//	Shuffle -> Window -> Sort -> DataSource, will be separated into:
//	  ==> Shuffle: for main thread
//	  ==> Window -> Sort(:Tail) -> shuffleWorker: for workers
//	  ==> DataSource: for `fetchDataAndSplit` thread
type PhysicalShuffle struct {
	BasePhysicalPlan

	Concurrency int
	Tails       []base.PhysicalPlan
	DataSources []base.PhysicalPlan

	SplitterType PartitionSplitterType
	ByItemArrays [][]expression.Expression
}

// Init initializes PhysicalShuffle.
func (p PhysicalShuffle) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalShuffle {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeShuffle, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// MemoryUsage return the memory usage of PhysicalShuffle
func (p *PhysicalShuffle) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalPlan.MemoryUsage() + size.SizeOfInt*2 + size.SizeOfSlice*(3+int64(cap(p.ByItemArrays))) +
		int64(cap(p.Tails)+cap(p.DataSources))*size.SizeOfInterface

	for _, plan := range p.Tails {
		sum += plan.MemoryUsage()
	}
	for _, plan := range p.DataSources {
		sum += plan.MemoryUsage()
	}
	for _, exprs := range p.ByItemArrays {
		sum += int64(cap(exprs)) * size.SizeOfInterface
		for _, expr := range exprs {
			sum += expr.MemoryUsage()
		}
	}
	return
}

// PartitionSplitterType is the type of `Shuffle` executor splitter, which splits data source into partitions.
type PartitionSplitterType int

const (
	// PartitionHashSplitterType is the splitter splits by hash.
	PartitionHashSplitterType = iota
	// PartitionRangeSplitterType is the splitter that split sorted data into the same range
	PartitionRangeSplitterType
)

// ExplainInfo implements Plan interface.
func (p *PhysicalShuffle) ExplainInfo() string {
	explainIDs := make([]fmt.Stringer, len(p.DataSources))
	for i := range p.DataSources {
		explainIDs[i] = p.DataSources[i].ExplainID()
	}

	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "execution info: concurrency:%v, data sources:%v", p.Concurrency, explainIDs)
	return buffer.String()
}

// ResolveIndices implements Plan interface.
func (p *PhysicalShuffle) ResolveIndices() (err error) {
	err = p.BasePhysicalPlan.ResolveIndices()
	if err != nil {
		return err
	}
	// There may be one or more DataSource
	for i := range p.ByItemArrays {
		// Each DataSource has an array of HashByItems
		for j := range p.ByItemArrays[i] {
			// "Shuffle" get value of items from `DataSource`, other than children[0].
			p.ByItemArrays[i][j], _, err = p.ByItemArrays[i][j].ResolveIndices(p.DataSources[i].Schema(), true)
			if err != nil {
				return err
			}
		}
	}
	return err
}

// PhysicalShuffleReceiverStub represents a receiver stub of `PhysicalShuffle`,
// and actually, is executed by `executor.shuffleWorker`.
type PhysicalShuffleReceiverStub struct {
	PhysicalSchemaProducer

	// Receiver points to `executor.shuffleReceiver`.
	Receiver unsafe.Pointer
	// DataSource is the op.PhysicalPlan of the Receiver.
	DataSource base.PhysicalPlan
}

// MemoryUsage return the memory usage of PhysicalShuffleReceiverStub
func (p *PhysicalShuffleReceiverStub) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfPointer + size.SizeOfInterface
	if p.DataSource != nil {
		sum += p.DataSource.MemoryUsage()
	}
	return
}

// Init initializes PhysicalShuffleReceiverStub.
func (p PhysicalShuffleReceiverStub) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalShuffleReceiverStub {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeShuffleReceiver, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}
