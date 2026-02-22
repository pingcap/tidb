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

package statistics

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// TopN stores most-common values, which is used to estimate point queries.
type TopN struct {
	TopN []TopNMeta

	totalCount uint64
	minCount   uint64
	// minCount and totalCount are initialized only once.
	once sync.Once
}

// AppendTopN appends a topn into the TopN struct.
func (c *TopN) AppendTopN(data []byte, count uint64) {
	if c == nil {
		return
	}
	c.TopN = append(c.TopN, TopNMeta{data, count})
}

func (c *TopN) String() string {
	if c == nil {
		return "EmptyTopN"
	}
	builder := &strings.Builder{}
	fmt.Fprintf(builder, "TopN{length: %v, ", len(c.TopN))
	fmt.Fprint(builder, "[")
	for i := range c.TopN {
		fmt.Fprintf(builder, "(%v, %v)", c.TopN[i].Encoded, c.TopN[i].Count)
		if i+1 != len(c.TopN) {
			fmt.Fprint(builder, ", ")
		}
	}
	fmt.Fprint(builder, "]")
	fmt.Fprint(builder, "}")
	return builder.String()
}

// Num returns the ndv of the TopN.
//
//	TopN is declared directly in Histogram. So the Len is occupied by the Histogram. We use Num instead.
func (c *TopN) Num() int {
	if c == nil {
		return 0
	}
	return len(c.TopN)
}

// DecodedString returns the value with decoded result.
func (c *TopN) DecodedString(ctx sessionctx.Context, colTypes []byte) (string, error) {
	if c == nil {
		return "", nil
	}
	builder := &strings.Builder{}
	fmt.Fprintf(builder, "TopN{length: %v, ", len(c.TopN))
	fmt.Fprint(builder, "[")
	var tmpDatum types.Datum
	for i := range c.TopN {
		tmpDatum.SetBytes(c.TopN[i].Encoded)
		valStr, err := ValueToString(ctx.GetSessionVars(), &tmpDatum, len(colTypes), colTypes)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(builder, "(%v, %v)", valStr, c.TopN[i].Count)
		if i+1 != len(c.TopN) {
			fmt.Fprint(builder, ", ")
		}
	}
	fmt.Fprint(builder, "]")
	fmt.Fprint(builder, "}")
	return builder.String(), nil
}

// Copy makes a copy for current TopN.
func (c *TopN) Copy() *TopN {
	if c == nil {
		return nil
	}
	topN := make([]TopNMeta, len(c.TopN))
	for i, t := range c.TopN {
		topN[i].Encoded = make([]byte, len(t.Encoded))
		copy(topN[i].Encoded, t.Encoded)
		topN[i].Count = t.Count
	}
	return &TopN{
		TopN: topN,
	}
}

// MinCount returns the minimum count in the TopN.
func (c *TopN) MinCount() uint64 {
	if c == nil || len(c.TopN) == 0 {
		return 0
	}
	c.calculateMinCountAndCount()
	return c.minCount
}

func (c *TopN) calculateMinCountAndCount() {
	if intest.InTest {
		// In test, After the sync.Once is called, topN will not be modified anymore.
		minCount, totalCount := c.calculateMinCountAndCountInternal()
		c.onceCalculateMinCountAndCount()
		intest.Assert(minCount == c.minCount, "minCount should be equal to the calculated minCount")
		intest.Assert(totalCount == c.totalCount, "totalCount should be equal to the calculated totalCount")
		return
	}
	c.onceCalculateMinCountAndCount()
}

func (c *TopN) onceCalculateMinCountAndCount() {
	c.once.Do(func() {
		// Initialize to the first value in TopN
		minCount, total := c.calculateMinCountAndCountInternal()
		c.minCount = minCount
		c.totalCount = total
	})
}

func (c *TopN) calculateMinCountAndCountInternal() (minCount, total uint64) {
	minCount = c.TopN[0].Count
	for _, t := range c.TopN {
		minCount = min(minCount, t.Count)
		total += t.Count
	}
	return minCount, total
}

// TopNMeta stores the unit of the TopN.
type TopNMeta struct {
	Encoded []byte
	Count   uint64
}

// QueryTopN returns the results for (h1, h2) in murmur3.Sum128(), if not exists, return (0, false).
// The input sctx is just for debug trace, you can pass nil safely if that's not needed.
func (c *TopN) QueryTopN(_ planctx.PlanContext, d []byte) (result uint64, found bool) {
	if c == nil {
		return 0, false
	}
	idx := c.FindTopN(d)
	if idx < 0 {
		return 0, false
	}
	return c.TopN[idx].Count, true
}

// FindTopN finds the index of the given value in the TopN.
func (c *TopN) FindTopN(d []byte) int {
	if c == nil {
		return -1
	}
	if len(c.TopN) == 0 {
		return -1
	}
	if len(c.TopN) == 1 {
		if bytes.Equal(c.TopN[0].Encoded, d) {
			return 0
		}
		return -1
	}
	if bytes.Compare(c.TopN[len(c.TopN)-1].Encoded, d) < 0 {
		return -1
	}
	if bytes.Compare(c.TopN[0].Encoded, d) > 0 {
		return -1
	}
	idx, match := slices.BinarySearchFunc(c.TopN, d, func(a TopNMeta, b []byte) int {
		return bytes.Compare(a.Encoded, b)
	})
	if !match {
		return -1
	}
	return idx
}

// LowerBound searches on the sorted top-n items,
// returns the smallest index i such that the value at element i is not less than `d`.
func (c *TopN) LowerBound(d []byte) (idx int, match bool) {
	if c == nil {
		return 0, false
	}
	idx, match = slices.BinarySearchFunc(c.TopN, d, func(a TopNMeta, b []byte) int {
		return bytes.Compare(a.Encoded, b)
	})
	return idx, match
}

// BetweenCount estimates the row count for interval [l, r).
// The input sctx is just for debug trace, you can pass nil safely if that's not needed.
func (c *TopN) BetweenCount(_ planctx.PlanContext, l, r []byte) (result uint64) {
	if c == nil {
		return 0
	}
	lIdx, _ := c.LowerBound(l)
	rIdx, _ := c.LowerBound(r)
	ret := uint64(0)
	for i := lIdx; i < rIdx; i++ {
		ret += c.TopN[i].Count
	}
	return ret
}

// Sort sorts the topn items.
func (c *TopN) Sort() {
	if c == nil {
		return
	}
	slices.SortFunc(c.TopN, func(i, j TopNMeta) int {
		return bytes.Compare(i.Encoded, j.Encoded)
	})
}

// TotalCount returns how many data is stored in TopN.
func (c *TopN) TotalCount() uint64 {
	if c == nil || len(c.TopN) == 0 {
		return 0
	}
	c.calculateMinCountAndCount()
	return c.totalCount
}

// Equal checks whether the two TopN are equal.
func (c *TopN) Equal(cc *TopN) bool {
	if c.TotalCount() == 0 && cc.TotalCount() == 0 {
		return true
	} else if c.TotalCount() != cc.TotalCount() {
		return false
	}
	if len(c.TopN) != len(cc.TopN) {
		return false
	}
	for i := range c.TopN {
		if !bytes.Equal(c.TopN[i].Encoded, cc.TopN[i].Encoded) {
			return false
		}
		if c.TopN[i].Count != cc.TopN[i].Count {
			return false
		}
	}
	return true
}

// MemoryUsage returns the total memory usage of a topn.
func (c *TopN) MemoryUsage() (sum int64) {
	if c == nil {
		return
	}
	sum = 32 // size of array (24) + reference (8)
	for _, meta := range c.TopN {
		sum += 32 + int64(cap(meta.Encoded)) // 32 is size of byte array (24) + size of uint64 (8)
	}
	return
}

// NewTopN creates the new TopN struct by the given size.
func NewTopN(n int) *TopN {
	return &TopN{TopN: make([]TopNMeta, 0, n)}
}

// MergeTopN is used to merge more TopN structures to generate a new TopN struct by the given size.
// The input parameters are multiple TopN structures to be merged and the size of the new TopN that will be generated.
// The output parameters are the newly generated TopN structure and the remaining numbers.
// Notice: The n can be 0. So n has no default value, we must explicitly specify this value.
func MergeTopN(topNs []*TopN, n uint32) (*TopN, []TopNMeta) {
	if CheckEmptyTopNs(topNs) {
		return nil, nil
	}
	// Different TopN structures may hold the same value, we have to merge them.
	counter := make(map[hack.MutableString]uint64)
	for _, topN := range topNs {
		if topN.TotalCount() == 0 {
			continue
		}
		for _, val := range topN.TopN {
			counter[hack.String(val.Encoded)] += val.Count
		}
	}
	numTop := len(counter)
	if numTop == 0 {
		return nil, nil
	}
	sorted := make([]TopNMeta, 0, numTop)
	for value, cnt := range counter {
		data := hack.Slice(string(value))
		sorted = append(sorted, TopNMeta{Encoded: data, Count: cnt})
	}
	return GetMergedTopNFromSortedSlice(sorted, n)
}

// CheckEmptyTopNs checks whether all TopNs are empty.
func CheckEmptyTopNs(topNs []*TopN) bool {
	for _, topN := range topNs {
		if topN.TotalCount() != 0 {
			return false
		}
	}
	return true
}

// SortTopnMeta sort topnMeta
func SortTopnMeta(topnMetas []TopNMeta) {
	slices.SortFunc(topnMetas, func(i, j TopNMeta) int {
		if i.Count != j.Count {
			return cmp.Compare(j.Count, i.Count)
		}
		return bytes.Compare(i.Encoded, j.Encoded)
	})
}

// TopnMetaCompare compare topnMeta
func TopnMetaCompare(i, j TopNMeta) int {
	c := cmp.Compare(j.Count, i.Count)
	if c != 0 {
		return c
	}
	return bytes.Compare(i.Encoded, j.Encoded)
}

// GetMergedTopNFromSortedSlice returns merged topn
func GetMergedTopNFromSortedSlice(sorted []TopNMeta, n uint32) (*TopN, []TopNMeta) {
	SortTopnMeta(sorted)
	n = min(uint32(len(sorted)), n)

	var finalTopN TopN
	finalTopN.TopN = sorted[:n]
	finalTopN.Sort()
	return &finalTopN, sorted[n:]
}
