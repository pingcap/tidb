// Copyright 2026 PingCAP, Inc.
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

package joinorder

import (
	"fmt"
	"testing"

	"github.com/bits-and-blooms/bitset"
	"github.com/pingcap/tidb/pkg/util/intset"
)

type joinorderBenchCase struct {
	name      string
	nodeCount int
	edgeCount int
}

type edgeFast struct {
	tes   intset.FastIntSet
	left  intset.FastIntSet
	right intset.FastIntSet
	rules []ruleFast
}

type ruleFast struct {
	from intset.FastIntSet
	to   intset.FastIntSet
}

type edgeBit struct {
	tes   *bitset.BitSet
	left  *bitset.BitSet
	right *bitset.BitSet
	rules []ruleBit
}

type ruleBit struct {
	from *bitset.BitSet
	to   *bitset.BitSet
}

func buildFastSingleton(idx int) intset.FastIntSet {
	return intset.NewFastIntSet(idx)
}

func buildBitSingleton(idx int) *bitset.BitSet {
	bs := bitset.New(uint(idx + 1))
	bs.Set(uint(idx))
	return bs
}

func buildFastEdges(nodes []intset.FastIntSet, edgeCount int) []edgeFast {
	edges := make([]edgeFast, 0, edgeCount)
	n := len(nodes)
	for i := 0; i < edgeCount; i++ {
		l := i % n
		r := (i*7 + 3) % n
		if r == l {
			r = (r + 1) % n
		}
		extra := (i*11 + 1) % n
		if extra == l || extra == r {
			extra = (extra + 2) % n
		}
		tes := nodes[l].Union(nodes[r]).Union(nodes[extra])
		left := nodes[l]
		right := nodes[r]
		rules := []ruleFast{
			{from: right, to: left},
			{from: left, to: right},
		}
		edges = append(edges, edgeFast{tes: tes, left: left, right: right, rules: rules})
	}
	return edges
}

func buildBitEdges(nodes []*bitset.BitSet, edgeCount int) []edgeBit {
	edges := make([]edgeBit, 0, edgeCount)
	n := len(nodes)
	for i := 0; i < edgeCount; i++ {
		l := i % n
		r := (i*7 + 3) % n
		if r == l {
			r = (r + 1) % n
		}
		extra := (i*11 + 1) % n
		if extra == l || extra == r {
			extra = (extra + 2) % n
		}
		tes := nodes[l].Union(nodes[r]).Union(nodes[extra])
		left := nodes[l]
		right := nodes[r]
		rules := []ruleBit{
			{from: right, to: left},
			{from: left, to: right},
		}
		edges = append(edges, edgeBit{tes: tes, left: left, right: right, rules: rules})
	}
	return edges
}

func BenchmarkJoinOrderConflictDetectorOps(b *testing.B) {
	cases := []joinorderBenchCase{
		{name: "n16_e32", nodeCount: 16, edgeCount: 32},
		{name: "n32_e64", nodeCount: 32, edgeCount: 64},
		{name: "n64_e128", nodeCount: 64, edgeCount: 128},
		{name: "n128_e256", nodeCount: 128, edgeCount: 256},
	}

	for _, c := range cases {
		fastNodes := make([]intset.FastIntSet, 0, c.nodeCount)
		bitNodes := make([]*bitset.BitSet, 0, c.nodeCount)
		for i := 0; i < c.nodeCount; i++ {
			fastNodes = append(fastNodes, buildFastSingleton(i))
			bitNodes = append(bitNodes, buildBitSingleton(i))
		}

		fastEdges := buildFastEdges(fastNodes, c.edgeCount)
		bitEdges := buildBitEdges(bitNodes, c.edgeCount)

		b.Run(fmt.Sprintf("fastintset/conflict/%s", c.name), func(b *testing.B) {
			var sink bool
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ok := true
				for _, e := range fastEdges {
					s := e.left.Union(e.right)
					if !e.tes.SubsetOf(s) || !e.tes.Intersects(e.left) || !e.tes.Intersects(e.right) {
						ok = false
					}
					if !e.left.Intersection(e.tes).SubsetOf(e.left) || !e.right.Intersection(e.tes).SubsetOf(e.right) {
						ok = false
					}
					for _, r := range e.rules {
						if r.from.Intersects(s) && !r.to.SubsetOf(s) {
							ok = false
						}
					}
				}
				sink = ok
			}
			if sink {
				_ = sink
			}
		})

		b.Run(fmt.Sprintf("bitset/conflict/%s", c.name), func(b *testing.B) {
			var sink bool
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ok := true
				for _, e := range bitEdges {
					s := e.left.Union(e.right)
					if !s.IsSuperSet(e.tes) || e.tes.IntersectionCardinality(e.left) == 0 || e.tes.IntersectionCardinality(e.right) == 0 {
						ok = false
					}
					if !e.left.IsSuperSet(e.left.Intersection(e.tes)) || !e.right.IsSuperSet(e.right.Intersection(e.tes)) {
						ok = false
					}
					for _, r := range e.rules {
						if r.from.IntersectionCardinality(s) > 0 && !s.IsSuperSet(r.to) {
							ok = false
						}
					}
				}
				sink = ok
			}
			if sink {
				_ = sink
			}
		})
	}
}
