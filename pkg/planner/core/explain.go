// Copyright 2017 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tipb/go-tipb"
)

// ExplainInfo implements Plan interface.
func (p *PhysicalTableReader) ExplainInfo() string {
	tablePlanInfo := "data:" + p.tablePlan.ExplainID().String()

	if p.ReadReqType == MPP {
		return fmt.Sprintf("MppVersion: %d, %s", p.SCtx().GetSessionVars().ChooseMppVersion(), tablePlanInfo)
	}

	return tablePlanInfo
}

// ExplainNormalizedInfo implements Plan interface.
func (*PhysicalTableReader) ExplainNormalizedInfo() string {
	return ""
}

// OperatorInfo return other operator information to be explained.
func (p *PhysicalTableReader) OperatorInfo(_ bool) string {
	return "data:" + p.tablePlan.ExplainID().String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexReader) ExplainInfo() string {
	return "index:" + p.indexPlan.ExplainID().String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexReader) ExplainNormalizedInfo() string {
	return "index:" + p.indexPlan.TP()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexLookUpReader) ExplainInfo() string {
	var str strings.Builder
	// The children can be inferred by the relation symbol.
	if p.PushedLimit != nil {
		str.WriteString("limit embedded(offset:")
		str.WriteString(strconv.FormatUint(p.PushedLimit.Offset, 10))
		str.WriteString(", count:")
		str.WriteString(strconv.FormatUint(p.PushedLimit.Count, 10))
		str.WriteString(")")
	}
	return str.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexMergeReader) ExplainInfo() string {
	var str strings.Builder
	if p.IsIntersectionType {
		str.WriteString("type: intersection")
	} else {
		str.WriteString("type: union")
	}
	if p.PushedLimit != nil {
		str.WriteString(", limit embedded(offset:")
		str.WriteString(strconv.FormatUint(p.PushedLimit.Offset, 10))
		str.WriteString(", count:")
		str.WriteString(strconv.FormatUint(p.PushedLimit.Count, 10))
		str.WriteString(")")
	}
	return str.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexMergeJoin) ExplainInfo() string {
	return p.PhysicalIndexJoin.ExplainInfoInternal(false, true)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexMergeJoin) ExplainNormalizedInfo() string {
	return p.PhysicalIndexJoin.ExplainInfoInternal(true, true)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalExchangeSender) ExplainInfo() string {
	buffer := bytes.NewBufferString("ExchangeType: ")
	switch p.ExchangeType {
	case tipb.ExchangeType_PassThrough:
		fmt.Fprintf(buffer, "PassThrough")
	case tipb.ExchangeType_Broadcast:
		fmt.Fprintf(buffer, "Broadcast")
	case tipb.ExchangeType_Hash:
		fmt.Fprintf(buffer, "HashPartition")
	}
	if p.CompressionMode != vardef.ExchangeCompressionModeNONE {
		fmt.Fprintf(buffer, ", Compression: %s", p.CompressionMode.Name())
	}
	if p.ExchangeType == tipb.ExchangeType_Hash {
		fmt.Fprintf(buffer, ", Hash Cols: %s", property.ExplainColumnList(p.SCtx().GetExprCtx().GetEvalCtx(), p.HashCols))
	}
	if len(p.Tasks) > 0 {
		fmt.Fprintf(buffer, ", tasks: [")
		for idx, task := range p.Tasks {
			if idx != 0 {
				fmt.Fprintf(buffer, ", ")
			}
			fmt.Fprintf(buffer, "%v", task.ID)
		}
		fmt.Fprintf(buffer, "]")
	}
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		fmt.Fprintf(buffer, ", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount)
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalExchangeReceiver) ExplainInfo() (res string) {
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		res = fmt.Sprintf("stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount)
	}
	return res
}
