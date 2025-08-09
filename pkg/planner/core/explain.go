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

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

// ExplainID overrides the ExplainID in order to match different range.
func (p *PhysicalIndexScan) ExplainID(_ ...bool) fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		if p.SCtx() != nil && p.SCtx().GetSessionVars().StmtCtx.IgnoreExplainIDSuffix {
			return p.TP()
		}
		return p.TP() + "_" + strconv.Itoa(p.ID())
	})
}

// TP overrides the TP in order to match different range.
func (p *PhysicalIndexScan) TP(_ ...bool) string {
	if p.isFullScan() {
		return plancodec.TypeIndexFullScan
	}
	return plancodec.TypeIndexRangeScan
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexScan) ExplainInfo() string {
	return p.AccessObject().String() + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexScan) ExplainNormalizedInfo() string {
	return p.AccessObject().NormalizedString() + ", " + p.OperatorInfo(true)
}

// OperatorInfo implements DataAccesser interface.
func (p *PhysicalIndexScan) OperatorInfo(normalized bool) string {
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()
	redact := p.SCtx().GetSessionVars().EnableRedactLog
	var buffer strings.Builder
	if len(p.rangeInfo) > 0 {
		if !normalized {
			buffer.WriteString("range: decided by ")
			buffer.WriteString(p.rangeInfo)
			buffer.WriteString(", ")
		}
	} else if p.haveCorCol() {
		if normalized {
			buffer.WriteString("range: decided by ")
			buffer.Write(expression.SortedExplainNormalizedExpressionList(p.AccessCondition))
			buffer.WriteString(", ")
		} else {
			buffer.WriteString("range: decided by [")
			for i, expr := range p.AccessCondition {
				if i != 0 {
					buffer.WriteString(" ")
				}
				buffer.WriteString(expr.StringWithCtx(ectx, redact))
			}
			buffer.WriteString("], ")
		}
	} else if len(p.Ranges) > 0 {
		if normalized {
			buffer.WriteString("range:[?,?], ")
		} else if !p.isFullScan() {
			buffer.WriteString("range:")
			for _, idxRange := range p.Ranges {
				buffer.WriteString(idxRange.Redact(redact))
				buffer.WriteString(", ")
			}
		}
	}
	buffer.WriteString("keep order:")
	buffer.WriteString(strconv.FormatBool(p.KeepOrder))
	if p.Desc {
		buffer.WriteString(", desc")
	}
	if !normalized {
		if p.usedStatsInfo != nil {
			str := p.usedStatsInfo.FormatForExplain()
			if len(str) > 0 {
				buffer.WriteString(", ")
				buffer.WriteString(str)
			}
		} else if p.StatsInfo().StatsVersion == statistics.PseudoVersion {
			// This branch is not needed in fact, we add this to prevent test result changes under planner/cascades/
			buffer.WriteString(", stats:pseudo")
		}
	}
	return buffer.String()
}

func (p *PhysicalIndexScan) haveCorCol() bool {
	for _, cond := range p.AccessCondition {
		if len(expression.ExtractCorColumns(cond)) > 0 {
			return true
		}
	}
	return false
}

func (p *PhysicalIndexScan) isFullScan() bool {
	if len(p.rangeInfo) > 0 || p.haveCorCol() {
		return false
	}
	for _, ran := range p.Ranges {
		if !ran.IsFullRange(false) {
			return false
		}
	}
	return true
}

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

func (p *PhysicalExpand) explainInfoV2() string {
	sb := strings.Builder{}
	evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
	enableRedactLog := p.SCtx().GetSessionVars().EnableRedactLog
	for i, oneL := range p.LevelExprs {
		if i == 0 {
			sb.WriteString("level-projection:")
			sb.WriteString("[")
			sb.WriteString(expression.ExplainExpressionList(evalCtx, oneL, p.Schema(), enableRedactLog))
			sb.WriteString("]")
		} else {
			sb.WriteString(",[")
			sb.WriteString(expression.ExplainExpressionList(evalCtx, oneL, p.Schema(), enableRedactLog))
			sb.WriteString("]")
		}
	}
	sb.WriteString("; schema: [")
	colStrs := make([]string, 0, len(p.Schema().Columns))
	for _, col := range p.Schema().Columns {
		colStrs = append(colStrs, col.StringWithCtx(evalCtx, perrors.RedactLogDisable))
	}
	sb.WriteString(strings.Join(colStrs, ","))
	sb.WriteString("]")
	return sb.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalExpand) ExplainInfo() string {
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()
	if len(p.LevelExprs) > 0 {
		return p.explainInfoV2()
	}
	var str strings.Builder
	str.WriteString("group set num:")
	str.WriteString(strconv.FormatInt(int64(len(p.GroupingSets)), 10))
	if p.GroupingIDCol != nil {
		str.WriteString(", groupingID:")
		str.WriteString(p.GroupingIDCol.StringWithCtx(ectx, perrors.RedactLogDisable))
		str.WriteString(", ")
	}
	str.WriteString(p.GroupingSets.StringWithCtx(ectx, perrors.RedactLogDisable))
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
