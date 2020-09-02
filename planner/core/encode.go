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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash"
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/plancodec"
)

var encoderPool = sync.Pool{
	New: func() interface{} {
		return &planEncoder{}
	},
}

type planEncoder struct {
	buf          bytes.Buffer
	encodedPlans map[int]bool
}

// EncodePlan is used to encodePlan the plan to the plan tree with compressing.
func EncodePlan(p Plan) string {
	pn := encoderPool.Get().(*planEncoder)
	defer encoderPool.Put(pn)
	if p == nil || p.SCtx() == nil {
		return ""
	}
	selectPlan := getSelectPlan(p)
	if selectPlan != nil {
		failpoint.Inject("mockPlanRowCount", func(val failpoint.Value) {
			selectPlan.statsInfo().RowCount = float64(val.(int))
		})
	}
	return pn.encodePlanTree(p)
}

func (pn *planEncoder) encodePlanTree(p Plan) string {
	pn.encodedPlans = make(map[int]bool)
	pn.buf.Reset()
	pn.encodePlan(p, true, 0)
	return plancodec.Compress(pn.buf.Bytes())
}

func (pn *planEncoder) encodePlan(p Plan, isRoot bool, depth int) {
	var storeType kv.StoreType = kv.UnSpecified
	if !isRoot {
		switch copPlan := p.(type) {
		case *PhysicalTableReader:
			storeType = copPlan.StoreType
		case *PhysicalTableScan:
			storeType = copPlan.StoreType
		default:
			storeType = kv.TiKV
		}
	}
	taskTypeInfo := plancodec.EncodeTaskType(isRoot, storeType)
	actRows, analyzeInfo, memoryInfo, diskInfo := getRuntimeInfo(p.SCtx(), p)
	rowCount := 0.0
	if statsInfo := p.statsInfo(); statsInfo != nil {
		rowCount = p.statsInfo().RowCount
	}
	plancodec.EncodePlanNode(depth, p.ID(), p.TP(), rowCount, taskTypeInfo, p.ExplainInfo(), actRows, analyzeInfo, memoryInfo, diskInfo, &pn.buf)
	pn.encodedPlans[p.ID()] = true
	depth++

	selectPlan := getSelectPlan(p)
	if selectPlan == nil {
		return
	}
	if !pn.encodedPlans[selectPlan.ID()] {
		pn.encodePlan(selectPlan, isRoot, depth)
		return
	}
	for _, child := range selectPlan.Children() {
		if pn.encodedPlans[child.ID()] {
			continue
		}
		pn.encodePlan(child.(PhysicalPlan), isRoot, depth)
	}
	switch copPlan := selectPlan.(type) {
	case *PhysicalTableReader:
		pn.encodePlan(copPlan.tablePlan, false, depth)
	case *PhysicalIndexReader:
		pn.encodePlan(copPlan.indexPlan, false, depth)
	case *PhysicalIndexLookUpReader:
		pn.encodePlan(copPlan.indexPlan, false, depth)
		pn.encodePlan(copPlan.tablePlan, false, depth)
	case *PhysicalIndexMergeReader:
		for _, p := range copPlan.partialPlans {
			pn.encodePlan(p, false, depth)
		}
		if copPlan.tablePlan != nil {
			pn.encodePlan(copPlan.tablePlan, false, depth)
		}
	}
}

var digesterPool = sync.Pool{
	New: func() interface{} {
		return &planDigester{
			hasher: sha256.New(),
		}
	},
}

type planDigester struct {
	buf          bytes.Buffer
	encodedPlans map[int]bool
	hasher       hash.Hash
}

// NormalizePlan is used to normalize the plan and generate plan digest.
func NormalizePlan(p Plan) (normalized, digest string) {
	selectPlan := getSelectPlan(p)
	if selectPlan == nil {
		return "", ""
	}
	d := digesterPool.Get().(*planDigester)
	defer digesterPool.Put(d)
	d.normalizePlanTree(selectPlan)
	normalized = d.buf.String()
	d.hasher.Write(d.buf.Bytes())
	d.buf.Reset()
	digest = fmt.Sprintf("%x", d.hasher.Sum(nil))
	d.hasher.Reset()
	return
}

func (d *planDigester) normalizePlanTree(p PhysicalPlan) {
	d.encodedPlans = make(map[int]bool)
	d.buf.Reset()
	d.normalizePlan(p, true, 0)
}

func (d *planDigester) normalizePlan(p PhysicalPlan, isRoot bool, depth int) {
	plancodec.NormalizePlanNode(depth, p.TP(), isRoot, p.ExplainNormalizedInfo(), &d.buf)
	d.encodedPlans[p.ID()] = true

	depth++
	for _, child := range p.Children() {
		if d.encodedPlans[child.ID()] {
			continue
		}
		d.normalizePlan(child.(PhysicalPlan), isRoot, depth)
	}
	switch x := p.(type) {
	case *PhysicalTableReader:
		d.normalizePlan(x.tablePlan, false, depth)
	case *PhysicalIndexReader:
		d.normalizePlan(x.indexPlan, false, depth)
	case *PhysicalIndexLookUpReader:
		d.normalizePlan(x.indexPlan, false, depth)
		d.normalizePlan(x.tablePlan, false, depth)
	case *PhysicalIndexMergeReader:
		for _, p := range x.partialPlans {
			d.normalizePlan(p, false, depth)
		}
		if x.tablePlan != nil {
			d.normalizePlan(x.tablePlan, false, depth)
		}
	}
}

func getSelectPlan(p Plan) PhysicalPlan {
	var selectPlan PhysicalPlan
	if physicalPlan, ok := p.(PhysicalPlan); ok {
		selectPlan = physicalPlan
	} else {
		switch x := p.(type) {
		case *Delete:
			selectPlan = x.SelectPlan
		case *Update:
			selectPlan = x.SelectPlan
		case *Insert:
			selectPlan = x.SelectPlan
		}
	}
	return selectPlan
}
