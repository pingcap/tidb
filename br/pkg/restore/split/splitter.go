// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Splitter defines the interface for basic splitting strategies.
type Splitter interface {
	// The execution will split the keys on one region, and starts scatter after the region finish split.
	ExecuteOneRegion(ctx context.Context, region *RegionInfo, keys [][]byte) ([]*RegionInfo, error)
	// The execution will split all privided keys
	// and make sure new splitted regions are balance.
	// It will split regions by the rewrite rules,
	// then it will split regions by the end key of each range.
	// tableRules includes the prefix of a table, since some ranges may have
	// a prefix with record sequence or index sequence.
	// note: all ranges and rewrite rules must have raw key.
	ExecuteSortedKeys(ctx context.Context, keys [][]byte) error

	// Wait until all region scatter finished.
	WaitForScatterRegionsTimeout(ctx context.Context, regionInfos []*RegionInfo, timeout time.Duration) error
}

// SplitStrategy defines how values should be accumulated and when to trigger a split.
type SplitStrategy[T any] interface {
	// Accumulate adds a new value into the split strategy's internal state.
	// This method accumulates data points or values, preparing them for potential splitting.
	Accumulate(T)
	ShouldSplit() bool

	AllAccumlations() *splitHelperIterator
	// ShouldSplit evaluates whether the conditions for a split are met.
	// If the conditions are met, it performs the split and returns the resulting split values.
	// Returns a non-nil result when a split is triggered.
	// SplitByAccmulation(ctx context.Context) error
}

type rewriteSplitter struct {
	rewriteKey []byte
	tableID    int64
	rule       *restoreutils.RewriteRules
	splitter   *SplitHelper
}

type splitHelperIterator struct {
	tableSplitters []*rewriteSplitter
}

func (iter *splitHelperIterator) Traverse(fn func(v Valued, endKey []byte, rule *restoreutils.RewriteRules) bool) {
	for _, entry := range iter.tableSplitters {
		endKey := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(entry.tableID+1))
		rule := entry.rule
		entry.splitter.Traverse(func(v Valued) bool {
			return fn(v, endKey, rule)
		})
	}
}

type LogSplitStrategy struct {
	tableSplitter map[int64]*SplitHelper
	rules         map[int64]*restoreutils.RewriteRules
}

const splitFileThreshold = 1024 * 1024 // 1 MB

func (l *LogSplitStrategy) skipFile(file *backuppb.DataFileInfo) bool {
	_, exist := l.rules[file.TableId]
	return file.Length < splitFileThreshold || file.IsMeta || !exist
}

func (l *LogSplitStrategy) Accumulate(file *backuppb.DataFileInfo) {
	if l.skipFile(file) {
		return
	}
	splitHelper, exist := l.tableSplitter[file.TableId]
	if !exist {
		splitHelper = NewSplitHelper()
		l.tableSplitter[file.TableId] = splitHelper
	}

	splitHelper.Merge(Valued{
		Key: Span{
			StartKey: file.StartKey,
			EndKey:   file.EndKey,
		},
		Value: Value{
			Size:   file.Length,
			Number: file.NumberOfEntries,
		},
	})
}

func (l *LogSplitStrategy) ShouldSplit() bool {
	return len(l.tableSplitter) > 4096
}

func (l *LogSplitStrategy) AccumlationsIter() *splitHelperIterator {
	tableSplitters := make([]*rewriteSplitter, 0, len(l.tableSplitter))
	for tableID, splitter := range l.tableSplitter {
		delete(l.tableSplitter, tableID)
		rewriteRule, exists := l.rules[tableID]
		if !exists {
			log.Info("skip splitting due to no table id matched", zap.Int64("tableID", tableID))
			continue
		}
		newTableID := restoreutils.GetRewriteTableID(tableID, rewriteRule)
		if newTableID == 0 {
			log.Warn("failed to get the rewrite table id", zap.Int64("tableID", tableID))
			continue
		}
		tableSplitters = append(tableSplitters, &rewriteSplitter{
			rewriteKey: codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(newTableID)),
			tableID:    newTableID,
			rule:       rewriteRule,
			splitter:   splitter,
		})
	}
	sort.Slice(tableSplitters, func(i, j int) bool {
		return bytes.Compare(tableSplitters[i].rewriteKey, tableSplitters[j].rewriteKey) < 0
	})
	return &splitHelperIterator{
		tableSplitters: tableSplitters,
	}
}

// PipelineSplitter defines the interface for advanced (pipeline) splitting strategies.
// log / compacted sst files restore need to use this to split after full restore.
// and the splitter must perform with a control.
// so we choose to split and restore in a continuous flow.
type MultiRegionsSplitter interface {
	Splitter
	ExecuteRegions(ctx context.Context) error // Method for executing pipeline-based splitting
}

type RegionsSplitter struct {
	RegionSplitter
	strategy           SplitStrategy[*backuppb.DataFileInfo]
	pool               util.WorkerPool
	splitThreSholdSize uint64
	splitThreSholdKeys int64

	eg        *errgroup.Group
	regionsCh chan []*RegionInfo
}

func (r *RegionsSplitter) ExecuteRegions(ctx context.Context, s *splitHelperIterator) error {
	var ectx context.Context
	var wg sync.WaitGroup
	r.eg, ectx = errgroup.WithContext(ctx)
	r.regionsCh = make(chan []*RegionInfo, 1024)
	wg.Add(1)
	go func() {
		defer wg.Done()
		scatterRegions := make([]*RegionInfo, 0)
	receiveNewRegions:
		for {
			select {
			case <-ctx.Done():
				return
			case newRegions, ok := <-r.regionsCh:
				if !ok {
					break receiveNewRegions
				}

				scatterRegions = append(scatterRegions, newRegions...)
			}
		}

		// It is too expensive to stop recovery and wait for a small number of regions
		// to complete scatter, so the maximum waiting time is reduced to 1 minute.
		_ = r.WaitForScatterRegionsTimeout(ectx, scatterRegions, time.Minute)
	}()

	err := SplitPoint(ectx, s, nil, r.splitRegionByPoints)
	if err != nil {
		return errors.Trace(err)
	}

	// wait for completion of splitting regions
	if err := r.eg.Wait(); err != nil {
		return errors.Trace(err)
	}

	// wait for completion of scattering regions
	close(r.regionsCh)
	wg.Wait()

	return nil
}

type splitFunc = func(context.Context, uint64, int64, *RegionInfo, []Valued) error

// SplitPoint selects ranges overlapped with each region, and calls `splitF` to split the region
func SplitPoint(
	ctx context.Context,
	iter *splitHelperIterator,
	client SplitClient,
	splitF splitFunc,
) (err error) {
	// region traverse status
	var (
		// the region buffer of each scan
		regions     []*RegionInfo = nil
		regionIndex int           = 0
	)
	// region split status
	var (
		// range span   +----------------+------+---+-------------+
		// region span    +------------------------------------+
		//                +initial length+          +end valued+
		// regionValueds is the ranges array overlapped with `regionInfo`
		regionValueds []Valued = nil
		// regionInfo is the region to be split
		regionInfo *RegionInfo = nil
		// intialLength is the length of the part of the first range overlapped with the region
		initialLength uint64 = 0
		initialNumber int64  = 0
	)
	// range status
	var (
		// regionOverCount is the number of regions overlapped with the range
		regionOverCount uint64 = 0
	)

	iter.Traverse(func(v Valued, endKey []byte, rule *restoreutils.RewriteRules) bool {
		if v.Value.Number == 0 || v.Value.Size == 0 {
			return true
		}
		var (
			vStartKey []byte
			vEndKey   []byte
		)
		// use `vStartKey` and `vEndKey` to compare with region's key
		vStartKey, vEndKey, err = restoreutils.GetRewriteEncodedKeys(v, rule)
		if err != nil {
			return false
		}
		// traverse to the first region overlapped with the range
		for ; regionIndex < len(regions); regionIndex++ {
			if bytes.Compare(vStartKey, regions[regionIndex].Region.EndKey) < 0 {
				break
			}
		}
		// cannot find any regions overlapped with the range
		// need to scan regions again
		if regionIndex == len(regions) {
			regions = nil
		}
		regionOverCount = 0
		for {
			if regionIndex >= len(regions) {
				var startKey []byte
				if len(regions) > 0 {
					// has traversed over the region buffer, should scan from the last region's end-key of the region buffer
					startKey = regions[len(regions)-1].Region.EndKey
				} else {
					// scan from the range's start-key
					startKey = vStartKey
				}
				// scan at most 64 regions into the region buffer
				regions, err = ScanRegionsWithRetry(ctx, client, startKey, endKey, 64)
				if err != nil {
					return false
				}
				regionIndex = 0
			}

			region := regions[regionIndex]
			// this region must be overlapped with the range
			regionOverCount++
			// the region is the last one overlapped with the range,
			// should split the last recorded region,
			// and then record this region as the region to be split
			if bytes.Compare(vEndKey, region.Region.EndKey) < 0 {
				endLength := v.Value.Size / regionOverCount
				endNumber := v.Value.Number / int64(regionOverCount)
				if len(regionValueds) > 0 && regionInfo != region {
					// add a part of the range as the end part
					if bytes.Compare(vStartKey, regionInfo.Region.EndKey) < 0 {
						regionValueds = append(regionValueds, NewValued(vStartKey, regionInfo.Region.EndKey, Value{Size: endLength, Number: endNumber}))
					}
					// try to split the region
					err = splitF(ctx, initialLength, initialNumber, regionInfo, regionValueds)
					if err != nil {
						return false
					}
					regionValueds = make([]Valued, 0)
				}
				if regionOverCount == 1 {
					// the region completely contains the range
					regionValueds = append(regionValueds, Valued{
						Key: Span{
							StartKey: vStartKey,
							EndKey:   vEndKey,
						},
						Value: v.Value,
					})
				} else {
					// the region is overlapped with the last part of the range
					initialLength = endLength
					initialNumber = endNumber
				}
				regionInfo = region
				// try the next range
				return true
			}

			// try the next region
			regionIndex++
		}
	})

	if err != nil {
		return errors.Trace(err)
	}
	if len(regionValueds) > 0 {
		// try to split the region
		err = splitF(ctx, initialLength, initialNumber, regionInfo, regionValueds)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (r *RegionsSplitter) splitRegionByPoints(
	ctx context.Context,
	initialLength uint64,
	initialNumber int64,
	region *RegionInfo,
	valueds []Valued,
) error {
	var (
		splitPoints [][]byte = make([][]byte, 0)
		lastKey     []byte   = region.Region.StartKey
		length      uint64   = initialLength
		number      int64    = initialNumber
	)
	for _, v := range valueds {
		// decode will discard ts behind the key, which results in the same key for consecutive ranges
		if !bytes.Equal(lastKey, v.GetStartKey()) && (v.Value.Size+length > r.splitThreSholdSize || v.Value.Number+number > r.splitThreSholdKeys) {
			_, rawKey, _ := codec.DecodeBytes(v.GetStartKey(), nil)
			splitPoints = append(splitPoints, rawKey)
			length = 0
			number = 0
		}
		lastKey = v.GetStartKey()
		length += v.Value.Size
		number += v.Value.Number
	}

	if len(splitPoints) == 0 {
		return nil
	}

	r.pool.ApplyOnErrorGroup(r.eg, func() error {
		newRegions, errSplit := r.ExecuteOneRegion(ctx, region, splitPoints)
		if errSplit != nil {
			log.Warn("failed to split the scaned region", zap.Error(errSplit))
			sort.Slice(splitPoints, func(i, j int) bool {
				return bytes.Compare(splitPoints[i], splitPoints[j]) < 0
			})
			return r.ExecuteSortedKeys(ctx, splitPoints)
		}
		select {
		case <-ctx.Done():
			return nil
		case r.regionsCh <- newRegions:
		}
		log.Info("split the region", zap.Uint64("region-id", region.Region.Id), zap.Int("split-point-number", len(splitPoints)))
		return nil
	})
	return nil
}
