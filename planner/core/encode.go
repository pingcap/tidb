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
	"github.com/pingcap/tidb/v4/util/plancodec"
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
	selectPlan := getSelectPlan(p)
	if selectPlan == nil {
		return ""
	}
	failpoint.Inject("mockPlanRowCount", func(val failpoint.Value) {
		selectPlan.statsInfo().RowCount = float64(val.(int))
	})
	return pn.encodePlanTree(selectPlan)
}

func (pn *planEncoder) encodePlanTree(p PhysicalPlan) string {
	pn.encodedPlans = make(map[int]bool)
	pn.buf.Reset()
	pn.encodePlan(p, true, 0)
	return plancodec.Compress(pn.buf.Bytes())
}

func (pn *planEncoder) encodePlan(p PhysicalPlan, isRoot bool, depth int) {
	plancodec.EncodePlanNode(depth, p.ID(), p.TP(), isRoot, p.statsInfo().RowCount, p.ExplainInfo(), &pn.buf)
	pn.encodedPlans[p.ID()] = true

	depth++
	for _, child := range p.Children() {
		if pn.encodedPlans[child.ID()] {
			continue
		}
		pn.encodePlan(child.(PhysicalPlan), isRoot, depth)
	}
	switch copPlan := p.(type) {
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
	plancodec.NormalizePlanNode(depth, p.ID(), p.TP(), isRoot, p.ExplainNormalizedInfo(), &d.buf)
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
