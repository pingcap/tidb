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
func EncodePlan(p PhysicalPlan) string {
	pn := encoderPool.Get().(*planEncoder)
	defer encoderPool.Put(pn)
	return pn.encodePlanTree(p)
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

// NormalizePlan is used to normalize the plan and generated plan digest.
func NormalizePlan(p PhysicalPlan) (normalized, digest string) {
	d := digesterPool.Get().(*planDigester)
	defer digesterPool.Put(d)
	d.normalizePlanTree(p)
	normalized = string(d.buf.Bytes())
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
	switch copPlan := p.(type) {
	case *PhysicalTableReader:
		d.normalizePlan(copPlan.tablePlan, false, depth)
	case *PhysicalIndexReader:
		d.normalizePlan(copPlan.indexPlan, false, depth)
	case *PhysicalIndexLookUpReader:
		d.normalizePlan(copPlan.indexPlan, false, depth)
		d.normalizePlan(copPlan.tablePlan, false, depth)
	}
}
