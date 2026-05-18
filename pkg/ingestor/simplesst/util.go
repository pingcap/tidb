// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package simplesst

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"sort"

	errors2 "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap/zapcore"
)

// EndpointTp is the type of Endpoint.Key.
type EndpointTp int

const (
	// ExclusiveEnd represents "..., Endpoint.Key)".
	ExclusiveEnd EndpointTp = iota
	// InclusiveStart represents "[Endpoint.Key, ...".
	InclusiveStart
	// InclusiveEnd represents "..., Endpoint.Key]".
	InclusiveEnd
)

// Endpoint represents an endpoint of an interval which can be used by GetMaxOverlapping.
type Endpoint struct {
	Key    []byte
	Tp     EndpointTp
	Weight int64 // all EndpointTp use positive weight
}

// GetMaxOverlapping returns the maximum overlapping weight treating given
// `points` as endpoints of intervals. `points` are not required to be sorted,
// and will be sorted in-place in this function.
func GetMaxOverlapping(points []Endpoint) int64 {
	slices.SortFunc(points, func(i, j Endpoint) int {
		if cmp := bytes.Compare(i.Key, j.Key); cmp != 0 {
			return cmp
		}
		return int(i.Tp) - int(j.Tp)
	})
	var maxWeight int64
	var curWeight int64
	for _, p := range points {
		switch p.Tp {
		case InclusiveStart:
			curWeight += p.Weight
		case ExclusiveEnd, InclusiveEnd:
			curWeight -= p.Weight
		}
		if curWeight > maxWeight {
			maxWeight = curWeight
		}
	}
	return maxWeight
}

// RemoveDuplicates remove all duplicates inside sorted array in place, i.e.
// input elements will be changed.
func RemoveDuplicates[E any](in []E, keyGetter func(*E) []byte, recordRemoved bool) ([]E, []E, int) {
	return doRemoveDuplicates(in, keyGetter, 0, recordRemoved)
}

// remove all duplicates inside sorted array in place if the duplicate count is
// more than 2, and keep the first two duplicates.
// we also return the total number of duplicates as the third return value.
func RemoveDuplicatesMoreThanTwo[E any](in []E, keyGetter func(*E) []byte) (out []E, removed []E, totalDup int) {
	return doRemoveDuplicates(in, keyGetter, 2, true)
}

// remove duplicates inside the sorted slice 'in', if keptDupCnt=2, we keep the
// first 2 duplicates, if keptDupCnt=0, we remove all duplicates.
// removed duplicates are returned in 'removed' if recordRemoved=true.
// we also return the total number of duplicates, either it's removed or not, as
// the third return value.
func doRemoveDuplicates[E any](
	in []E,
	keyGetter func(*E) []byte,
	keptDupCnt int,
	recordRemoved bool,
) (out []E, removed []E, totalDup int) {
	intest.Assert(keptDupCnt == 0 || keptDupCnt == 2, "keptDupCnt must be 0 or 2")
	if len(in) <= 1 {
		return in, []E{}, 0
	}
	pivotIdx, fillIdx := 0, 0
	pivot := keyGetter(&in[pivotIdx])
	if recordRemoved {
		removed = make([]E, 0, 2)
	}
	for idx := 1; idx <= len(in); idx++ {
		var key []byte
		if idx < len(in) {
			key = keyGetter(&in[idx])
			if bytes.Equal(pivot, key) {
				continue
			}
		}
		dupCount := idx - pivotIdx
		if dupCount >= 2 {
			totalDup += dupCount
			// keep the first keptDupCnt duplicates, and remove the rest
			for startIdx := pivotIdx; startIdx < pivotIdx+keptDupCnt; startIdx++ {
				if startIdx != fillIdx {
					in[fillIdx] = in[startIdx]
				}
				fillIdx++
			}
			if recordRemoved {
				removed = append(removed, in[pivotIdx+keptDupCnt:idx]...)
			}
		} else {
			if pivotIdx != fillIdx {
				in[fillIdx] = in[pivotIdx]
			}
			fillIdx++
		}
		pivotIdx = idx
		pivot = key
	}
	return in[:fillIdx], removed, totalDup
}

var (
	// getReadRangeFromPropsConcurrency limits the number of stats files scanned in
	// parallel to avoid bursty object-storage reads when an import step tracks a
	// large number of files. Use a lower default than the data-reader budget
	// because props scanning is metadata-heavy and benefits less from high fanout.
	getReadRangeFromPropsConcurrency = 64
)

// GetReadRangeFromProps reads the statistic files to find the largest offset of
// corresponding sorted data file such that the key at offset is less than or
// equal to the given start keys. These returned offsets can be used to seek data
// file reader, read, parse and skip few smaller keys, and then locate the needed
// data.
//
// Caller can specify multiple ascending keys and GetReadRangeFromProps will return
// the offsets per file for each key. For a range [keyA, keyB), the caller can use
// result[A] as startOffsets and result[B] as estimatedEndOffsets.
// Empty jobKeys returns an empty result.
func GetReadRangeFromProps(
	ctx context.Context,
	jobKeys [][]byte,
	paths []string,
	exStorage storeapi.Storage,
) (_ [][]uint64, err error) {
	logger := logutil.Logger(ctx)
	task := log.BeginTask(logger, "seek props offsets")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()

	starts := make([]kv.Key, len(jobKeys))
	for i := range jobKeys {
		starts[i] = kv.Key(jobKeys[i])
	}
	if len(starts) == 0 {
		return [][]uint64{}, nil
	}

	readRangesPerKey := make([][]uint64, len(starts))
	for i := range starts {
		readRangesPerKey[i] = make([]uint64, len(paths))
	}

	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	eg.SetLimit(getReadRangeFromPropsConcurrency)
	for i := range paths {
		eg.Go(func() error {
			r, err2 := NewStatsReader(egCtx, exStorage, paths[i], 250*1024)
			if err2 != nil {
				if errors.Is(err2, io.EOF) {
					return nil
				}
				return errors2.Trace(err2)
			}
			defer func() {
				_ = r.Close()
			}()

			keyIdx := 0
			curKey := starts[keyIdx]

			p, err3 := r.NextProp()
			var firstKey kv.Key
			if err3 == nil {
				firstKey = kv.Key(p.FirstKey)
			}
			for {
				if err3 != nil {
					if errors.Is(err3, io.EOF) {
						// fill the rest of the offsets with the last offset
						off := readRangesPerKey[keyIdx][i]
						for keyIdx++; keyIdx < len(starts); keyIdx++ {
							readRangesPerKey[keyIdx][i] = off
						}
						return nil
					}
					return errors2.Trace(err3)
				}
				for firstKey.Cmp(curKey) > 0 {
					keyIdx++
					if keyIdx >= len(starts) {
						return nil
					}
					readRangesPerKey[keyIdx][i] = readRangesPerKey[keyIdx-1][i]
					curKey = starts[keyIdx]
				}
				readRangesPerKey[keyIdx][i] = p.Offset
				p, err3 = r.NextProp()
				if err3 == nil {
					firstKey = kv.Key(p.FirstKey)
				}
			}
		})
	}

	if err = eg.Wait(); err != nil {
		return nil, err
	}
	return readRangesPerKey, nil
}

// GetAllFileNames returns files with the same non-partitioned dir.
//   - for intermediate KV/stat files we store them with a partitioned way to mitigate
//     limitation on Cloud, see randPartitionedPrefix for how we partition the files.
//   - for meta files, we store them directly under the non-partitioned dir.
//
// for example, if nonPartitionedDir is '30001', the files returned might be
//   - 30001/6/meta.json
//   - 30001/7/meta.json
//   - 30001/plan/ingest/1/meta.json
//   - 30001/plan/merge-sort/1/meta.json
//   - p00110000/30001/7/617527bf-e25d-4312-8784-4a4576eb0195_stat/one-file
//   - p00000000/30001/7/617527bf-e25d-4312-8784-4a4576eb0195/one-file
func GetAllFileNames(
	ctx context.Context,
	store storeapi.Storage,
	nonPartitionedDir string,
) ([]string, error) {
	var data []string

	err := store.WalkDir(ctx,
		&storeapi.WalkOption{},
		func(path string, size int64) error {
			// extract the first dir
			bs := hack.Slice(path)
			firstIdx := bytes.IndexByte(bs, '/')
			if firstIdx == -1 {
				return nil
			}

			firstDir := bs[:firstIdx]
			if string(firstDir) == nonPartitionedDir {
				data = append(data, path)
				return nil
			}

			if !IsValidPartition(firstDir) {
				return nil
			}
			secondIdx := bytes.IndexByte(bs[firstIdx+1:], '/')
			if secondIdx == -1 {
				return nil
			}
			secondDir := path[firstIdx+1 : firstIdx+1+secondIdx]

			if secondDir == nonPartitionedDir {
				data = append(data, path)
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	// in case the external storage does not guarantee the order of walk
	sort.Strings(data)
	return data, nil
}
