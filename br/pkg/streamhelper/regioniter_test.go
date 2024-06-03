// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type constantRegions []streamhelper.RegionWithLeader

func regionToRange(region streamhelper.RegionWithLeader) kv.KeyRange {
	return kv.KeyRange{
		StartKey: region.Region.StartKey,
		EndKey:   region.Region.EndKey,
	}
}

func (c constantRegions) EqualsTo(other []streamhelper.RegionWithLeader) bool {
	if len(c) != len(other) {
		return false
	}
	for i := 0; i < len(c); i++ {
		r1 := regionToRange(c[i])
		r2 := regionToRange(other[i])

		equals := bytes.Equal(r1.StartKey, r2.StartKey) && bytes.Equal(r1.EndKey, r2.EndKey)
		if !equals {
			return false
		}
	}
	return true
}

func (c constantRegions) String() string {
	segs := make([]string, 0, len(c))
	for _, region := range c {
		segs = append(segs, fmt.Sprintf("%d%s", region.Region.Id, logutil.StringifyRange(regionToRange(region))))
	}
	return strings.Join(segs, ";")
}

// RegionScan gets a list of regions, starts from the region that contains key.
// Limit limits the maximum number of regions returned.
func (c constantRegions) RegionScan(ctx context.Context, key []byte, endKey []byte, limit int) ([]streamhelper.RegionWithLeader, error) {
	result := make([]streamhelper.RegionWithLeader, 0, limit)
	for _, region := range c {
		if spans.Overlaps(kv.KeyRange{StartKey: key, EndKey: endKey}, kv.KeyRange{StartKey: region.Region.StartKey, EndKey: region.Region.EndKey}) && len(result) < limit {
			result = append(result, region)
		} else if bytes.Compare(region.Region.StartKey, key) > 0 {
			break
		}
	}
	fmt.Printf("all = %s\n", c)
	fmt.Printf("start = %s, end = %s, result = %s\n", redact.Key(key), redact.Key(endKey), constantRegions(result))
	return result, nil
}

// Stores returns the store metadata from the cluster.
func (c constantRegions) Stores(ctx context.Context) ([]streamhelper.Store, error) {
	return nil, status.Error(codes.Unimplemented, "Unsupported operation")
}

// Updates the service GC safe point for the cluster.
// Returns the latest service GC safe point.
// If the arguments is `0`, this would remove the service safe point.
func (c constantRegions) BlockGCUntil(ctx context.Context, at uint64) (uint64, error) {
	return 0, status.Error(codes.Unimplemented, "Unsupported operation")
}

func (c constantRegions) UnblockGC(ctx context.Context) error {
	return status.Error(codes.Unimplemented, "Unsupported operation")
}

// TODO: It should be able to synchoronize the current TS with the PD.
func (c constantRegions) FetchCurrentTS(ctx context.Context) (uint64, error) {
	return oracle.ComposeTS(time.Now().UnixMilli(), 0), nil
}

func makeSubrangeRegions(keys ...string) constantRegions {
	if len(keys) == 0 {
		return nil
	}
	id := uint64(1)
	regions := make([]streamhelper.RegionWithLeader, 0, len(keys)+1)
	start := keys[0]
	for _, key := range keys[1:] {
		region := streamhelper.RegionWithLeader{
			Region: &metapb.Region{
				Id:       id,
				StartKey: []byte(start),
				EndKey:   []byte(key),
			},
		}
		id++
		start = key
		regions = append(regions, region)
	}
	return constantRegions(regions)
}

func useRegions(keys ...string) constantRegions {
	ks := []string{""}
	ks = append(ks, keys...)
	ks = append(ks, "")
	return makeSubrangeRegions(ks...)
}

func manyRegions(from, to int) []string {
	regions := []string{}
	for i := from; i < to; i++ {
		regions = append(regions, fmt.Sprintf("%06d", i))
	}
	return regions
}

func appendInitial(a []string) []string {
	return append([]string{""}, a...)
}

func appendFinal(a []string) []string {
	return append(a, "")
}

func TestRegionIterator(t *testing.T) {
	type Case struct {
		// boundary of regions, doesn't include the initial key (implicitly "")
		// or the final key (implicitly +inf)
		// Example:
		// ["0001", "0002"] => [Region("", "0001"), Region("0001", "0002"), Region("0002", "")]
		RegionBoundary []string
		StartKey       string
		EndKey         string
		// border of required regions, include the initial key and the final key.
		// Example:
		// ["0001", "0002", ""] => [Region("0001", "0002"), Region("0002", "")]
		RequiredRegionBoundary []string
	}

	run := func(t *testing.T, c Case) {
		req := require.New(t)
		regions := useRegions(c.RegionBoundary...)
		requiredRegions := makeSubrangeRegions(c.RequiredRegionBoundary...)
		ctx := context.Background()

		collected := make([]streamhelper.RegionWithLeader, 0, len(c.RequiredRegionBoundary))
		iter := streamhelper.IterateRegion(regions, []byte(c.StartKey), []byte(c.EndKey))
		for !iter.Done() {
			regions, err := iter.Next(ctx)
			req.NoError(err)
			collected = append(collected, regions...)
		}
		req.True(requiredRegions.EqualsTo(collected), "%s :: %s", requiredRegions, collected)
	}

	cases := []Case{
		{
			RegionBoundary:         []string{"0001", "0003", "0008", "0078"},
			StartKey:               "0077",
			EndKey:                 "0079",
			RequiredRegionBoundary: []string{"0008", "0078", ""},
		},
		{
			RegionBoundary:         []string{"0001", "0005", "0008", "0097"},
			StartKey:               "0000",
			EndKey:                 "0008",
			RequiredRegionBoundary: []string{"", "0001", "0005", "0008"},
		},
		{
			RegionBoundary:         manyRegions(0, 10000),
			StartKey:               "000001",
			EndKey:                 "005000",
			RequiredRegionBoundary: manyRegions(1, 5001),
		},
		{
			RegionBoundary:         manyRegions(0, 10000),
			StartKey:               "000100",
			EndKey:                 "",
			RequiredRegionBoundary: appendFinal(manyRegions(100, 10000)),
		},
		{
			RegionBoundary:         manyRegions(0, 10000),
			StartKey:               "",
			EndKey:                 "003000",
			RequiredRegionBoundary: appendInitial(manyRegions(0, 3001)),
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case#%d", i), func(t *testing.T) {
			run(t, c)
		})
	}
}
