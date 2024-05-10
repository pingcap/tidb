// Copyright 2024 PingCAP, Inc.
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

package logsplit

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/restore/internal/utils"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

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

func NewSplitHelperIteratorForTest(helper *SplitHelper, tableID int64, rule *restoreutils.RewriteRules) *splitHelperIterator {
	return &splitHelperIterator{
		tableSplitters: []*rewriteSplitter{
			{
				tableID:  tableID,
				rule:     rule,
				splitter: helper,
			},
		},
	}
}

type LogSplitHelper struct {
	tableSplitter map[int64]*SplitHelper
	rules         map[int64]*restoreutils.RewriteRules
	client        split.SplitClient
	pool          *util.WorkerPool
	eg            *errgroup.Group
	regionsCh     chan []*split.RegionInfo

	splitThreSholdSize uint64
	splitThreSholdKeys int64
}

func NewLogSplitHelper(rules map[int64]*restoreutils.RewriteRules, client split.SplitClient, splitSize uint64, splitKeys int64) *LogSplitHelper {
	return &LogSplitHelper{
		tableSplitter: make(map[int64]*SplitHelper),
		rules:         rules,
		client:        client,
		pool:          util.NewWorkerPool(128, "split region"),
		eg:            nil,

		splitThreSholdSize: splitSize,
		splitThreSholdKeys: splitKeys,
	}
}

func (helper *LogSplitHelper) iterator() *splitHelperIterator {
	tableSplitters := make([]*rewriteSplitter, 0, len(helper.tableSplitter))
	for tableID, splitter := range helper.tableSplitter {
		delete(helper.tableSplitter, tableID)
		rewriteRule, exists := helper.rules[tableID]
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

const splitFileThreshold = 1024 * 1024 // 1 MB

func (helper *LogSplitHelper) skipFile(file *backuppb.DataFileInfo) bool {
	_, exist := helper.rules[file.TableId]
	return file.Length < splitFileThreshold || file.IsMeta || !exist
}

func (helper *LogSplitHelper) Merge(file *backuppb.DataFileInfo) {
	if helper.skipFile(file) {
		return
	}
	splitHelper, exist := helper.tableSplitter[file.TableId]
	if !exist {
		splitHelper = NewSplitHelper()
		helper.tableSplitter[file.TableId] = splitHelper
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

type splitFunc = func(context.Context, *utils.RegionSplitter, uint64, int64, *split.RegionInfo, []Valued) error

func (helper *LogSplitHelper) splitRegionByPoints(
	ctx context.Context,
	regionSplitter *utils.RegionSplitter,
	initialLength uint64,
	initialNumber int64,
	region *split.RegionInfo,
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
		if !bytes.Equal(lastKey, v.GetStartKey()) && (v.Value.Size+length > helper.splitThreSholdSize || v.Value.Number+number > helper.splitThreSholdKeys) {
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

	helper.pool.ApplyOnErrorGroup(helper.eg, func() error {
		newRegions, errSplit := regionSplitter.SplitWaitAndScatter(ctx, region, splitPoints)
		if errSplit != nil {
			log.Warn("failed to split the scaned region", zap.Error(errSplit))
			_, startKey, _ := codec.DecodeBytes(region.Region.StartKey, nil)
			ranges := make([]rtree.Range, 0, len(splitPoints))
			for _, point := range splitPoints {
				ranges = append(ranges, rtree.Range{StartKey: startKey, EndKey: point})
				startKey = point
			}

			return regionSplitter.ExecuteSplit(ctx, ranges)
		}
		select {
		case <-ctx.Done():
			return nil
		case helper.regionsCh <- newRegions:
		}
		log.Info("split the region", zap.Uint64("region-id", region.Region.Id), zap.Int("split-point-number", len(splitPoints)))
		return nil
	})
	return nil
}

// SplitPoint selects ranges overlapped with each region, and calls `splitF` to split the region
func SplitPoint(
	ctx context.Context,
	iter *splitHelperIterator,
	client split.SplitClient,
	splitF splitFunc,
) (err error) {
	// common status
	var (
		regionSplitter *utils.RegionSplitter = utils.NewRegionSplitter(client)
	)
	// region traverse status
	var (
		// the region buffer of each scan
		regions     []*split.RegionInfo = nil
		regionIndex int                 = 0
	)
	// region split status
	var (
		// range span   +----------------+------+---+-------------+
		// region span    +------------------------------------+
		//                +initial length+          +end valued+
		// regionValueds is the ranges array overlapped with `regionInfo`
		regionValueds []Valued = nil
		// regionInfo is the region to be split
		regionInfo *split.RegionInfo = nil
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
				regions, err = split.ScanRegionsWithRetry(ctx, client, startKey, endKey, 64)
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
					err = splitF(ctx, regionSplitter, initialLength, initialNumber, regionInfo, regionValueds)
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
		err = splitF(ctx, regionSplitter, initialLength, initialNumber, regionInfo, regionValueds)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (helper *LogSplitHelper) Split(ctx context.Context) error {
	var ectx context.Context
	var wg sync.WaitGroup
	helper.eg, ectx = errgroup.WithContext(ctx)
	helper.regionsCh = make(chan []*split.RegionInfo, 1024)
	wg.Add(1)
	go func() {
		defer wg.Done()
		scatterRegions := make([]*split.RegionInfo, 0)
	receiveNewRegions:
		for {
			select {
			case <-ectx.Done():
				return
			case newRegions, ok := <-helper.regionsCh:
				if !ok {
					break receiveNewRegions
				}

				scatterRegions = append(scatterRegions, newRegions...)
			}
		}

		regionSplitter := utils.NewRegionSplitter(helper.client)
		// It is too expensive to stop recovery and wait for a small number of regions
		// to complete scatter, so the maximum waiting time is reduced to 1 minute.
		_ = regionSplitter.WaitForScatterRegionsTimeout(ctx, scatterRegions, time.Minute)
	}()

	iter := helper.iterator()
	if err := SplitPoint(ectx, iter, helper.client, helper.splitRegionByPoints); err != nil {
		return errors.Trace(err)
	}

	// wait for completion of splitting regions
	if err := helper.eg.Wait(); err != nil {
		return errors.Trace(err)
	}

	// wait for completion of scattering regions
	close(helper.regionsCh)
	wg.Wait()

	return nil
}
