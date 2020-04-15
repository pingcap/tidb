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

package profile

import (
	"fmt"
	"math"
	"sort"

	"github.com/google/pprof/profile"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/texttree"
)

type flamegraphNode struct {
	cumValue int64
	children map[uint64]*flamegraphNode
}

func newFlamegraphNode() *flamegraphNode {
	return &flamegraphNode{
		cumValue: 0,
		children: make(map[uint64]*flamegraphNode),
	}
}

// add the value from a sample into the flamegraph DAG.
// This method should only be called on the root node.
func (n *flamegraphNode) add(sample *profile.Sample) {
	// FIXME: we take the last sample value by default, but some profiles have multiple samples.
	//  - allocs:    alloc_objects, alloc_space, inuse_objects, inuse_space
	//  - block:     contentions, delay
	//  - cpu:       samples, cpu
	//  - heap:      alloc_objects, alloc_space, inuse_objects, inuse_space
	//  - mutex:     contentions, delay

	value := sample.Value[len(sample.Value)-1]
	if value == 0 {
		return
	}

	locs := sample.Location

	for {
		n.cumValue += value
		if len(locs) == 0 {
			return
		}

		// The previous implementation in TiDB identify nodes using location ID,
		// but `go tool pprof` identify nodes using function ID. Should we follow?
		loc := locs[len(locs)-1].ID
		child, ok := n.children[loc]
		if !ok {
			child = newFlamegraphNode()
			n.children[loc] = child
		}
		locs = locs[:len(locs)-1]
		n = child
	}
}

type flamegraphNodeWithLocation struct {
	*flamegraphNode
	locID uint64
}

// sortedChildren returns a list of children of this node, sorted by each
// child's cumulative value.
func (n *flamegraphNode) sortedChildren() []flamegraphNodeWithLocation {
	children := make([]flamegraphNodeWithLocation, 0, len(n.children))
	for locID, child := range n.children {
		children = append(children, flamegraphNodeWithLocation{
			flamegraphNode: child,
			locID:          locID,
		})
	}
	sort.Slice(children, func(i, j int) bool {
		a, b := children[i], children[j]
		if a.cumValue != b.cumValue {
			return a.cumValue > b.cumValue
		}
		return a.locID < b.locID
	})

	return children
}

type flamegraphCollector struct {
	rows      [][]types.Datum
	locations map[uint64]*profile.Location
	total     int64
	rootChild int
}

func newFlamegraphCollector(p *profile.Profile) *flamegraphCollector {
	locations := make(map[uint64]*profile.Location, len(p.Location))
	for _, loc := range p.Location {
		locations[loc.ID] = loc
	}
	return &flamegraphCollector{locations: locations}
}

func (c *flamegraphCollector) locationName(locID uint64) (funcName, fileLine string) {
	loc := c.locations[locID]
	if len(loc.Line) == 0 {
		return "<unknown>", "<unknown>"
	}
	line := loc.Line[0]
	funcName = line.Function.Name
	fileLine = fmt.Sprintf("%s:%d", line.Function.Filename, line.Line)
	return
}

func (c *flamegraphCollector) collectChild(
	node flamegraphNodeWithLocation,
	depth int64,
	indent string,
	parentCumValue int64,
	isLastChild bool,
) {
	funcName, fileLine := c.locationName(node.locID)
	c.rows = append(c.rows, types.MakeDatums(
		texttree.PrettyIdentifier(funcName, indent, isLastChild),
		percentage(node.cumValue, c.total),
		percentage(node.cumValue, parentCumValue),
		c.rootChild,
		depth,
		fileLine,
	))

	if len(node.children) == 0 {
		return
	}

	indent4Child := texttree.Indent4Child(indent, isLastChild)
	children := node.sortedChildren()
	for i, child := range children {
		c.collectChild(child, depth+1, indent4Child, node.cumValue, i == len(children)-1)
	}
}

func (c *flamegraphCollector) collect(root *flamegraphNode) {
	c.rows = append(c.rows, types.MakeDatums("root", "100%", "100%", 0, 0, "root"))
	if len(root.children) == 0 {
		return
	}

	c.total = root.cumValue
	indent4Child := texttree.Indent4Child("", false)
	children := root.sortedChildren()
	for i, child := range children {
		c.rootChild = i + 1
		c.collectChild(child, 1, indent4Child, root.cumValue, i == len(children)-1)
	}
}

func percentage(value, total int64) string {
	var ratio float64
	if total != 0 {
		ratio = math.Abs(float64(value)/float64(total)) * 100
	}
	switch {
	case ratio >= 99.95 && ratio <= 100.05:
		return "100%"
	case ratio >= 1.0:
		return fmt.Sprintf("%.2f%%", ratio)
	default:
		return fmt.Sprintf("%.2g%%", ratio)
	}
}
