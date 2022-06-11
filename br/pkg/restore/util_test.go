// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"encoding/binary"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
)

func TestParseQuoteName(t *testing.T) {
	schema, table := restore.ParseQuoteName("`a`.`b`")
	require.Equal(t, "a", schema)
	require.Equal(t, "b", table)

	schema, table = restore.ParseQuoteName("`a``b`.``````")
	require.Equal(t, "a`b", schema)
	require.Equal(t, "``", table)

	schema, table = restore.ParseQuoteName("`.`.`.`")
	require.Equal(t, ".", schema)
	require.Equal(t, ".", table)

	schema, table = restore.ParseQuoteName("`.``.`.`.`")
	require.Equal(t, ".`.", schema)
	require.Equal(t, ".", table)
}

func TestGetSSTMetaFromFile(t *testing.T) {
	file := &backuppb.File{
		Name:     "file_write.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte("t1ccc"),
	}
	rule := &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("t1"),
		NewKeyPrefix: []byte("t2"),
	}
	region := &metapb.Region{
		StartKey: []byte("t2abc"),
		EndKey:   []byte("t3a"),
	}
	sstMeta := restore.GetSSTMetaFromFile([]byte{}, file, region, rule)
	require.Equal(t, "t2abc", string(sstMeta.GetRange().GetStart()))
	require.Equal(t, "t2\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", string(sstMeta.GetRange().GetEnd()))
}

func TestMapTableToFiles(t *testing.T) {
	filesOfTable1 := []*backuppb.File{
		{
			Name:     "table1-1.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(1),
		},
		{
			Name:     "table1-2.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(1),
		},
		{
			Name:     "table1-3.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(1),
		},
	}
	filesOfTable2 := []*backuppb.File{
		{
			Name:     "table2-1.sst",
			StartKey: tablecodec.EncodeTablePrefix(2),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		},
		{
			Name:     "table2-2.sst",
			StartKey: tablecodec.EncodeTablePrefix(2),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		},
	}

	result := restore.MapTableToFiles(append(filesOfTable2, filesOfTable1...))

	require.Equal(t, filesOfTable1, result[1])
	require.Equal(t, filesOfTable2, result[2])
}

func TestValidateFileRewriteRule(t *testing.T) {
	rules := &restore.RewriteRules{
		Data: []*import_sstpb.RewriteRule{{
			OldKeyPrefix: []byte(tablecodec.EncodeTablePrefix(1)),
			NewKeyPrefix: []byte(tablecodec.EncodeTablePrefix(2)),
		}},
	}

	// Empty start/end key is not allowed.
	err := restore.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: []byte(""),
			EndKey:   []byte(""),
		},
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*cannot find rewrite rule.*", err.Error())

	// Range is not overlap, no rule found.
	err = restore.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(0),
			EndKey:   tablecodec.EncodeTablePrefix(1),
		},
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*cannot find rewrite rule.*", err.Error())

	// No rule for end key.
	err = restore.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		},
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*cannot find rewrite rule.*", err.Error())

	// Add a rule for end key.
	rules.Data = append(rules.Data, &import_sstpb.RewriteRule{
		OldKeyPrefix: tablecodec.EncodeTablePrefix(2),
		NewKeyPrefix: tablecodec.EncodeTablePrefix(3),
	})
	err = restore.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		},
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*rewrite rule mismatch.*", err.Error())

	// Add a bad rule for end key, after rewrite start key > end key.
	rules.Data = append(rules.Data[:1], &import_sstpb.RewriteRule{
		OldKeyPrefix: tablecodec.EncodeTablePrefix(2),
		NewKeyPrefix: tablecodec.EncodeTablePrefix(1),
	})
	err = restore.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		},
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*rewrite rule mismatch.*", err.Error())
}

func TestPaginateScanRegion(t *testing.T) {
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	stores := make(map[uint64]*metapb.Store)
	stores[1] = &metapb.Store{
		Id: 1,
	}

	makeRegions := func(num uint64) (map[uint64]*restore.RegionInfo, []*restore.RegionInfo) {
		regionsMap := make(map[uint64]*restore.RegionInfo, num)
		regions := make([]*restore.RegionInfo, 0, num)
		endKey := make([]byte, 8)
		for i := uint64(0); i < num-1; i++ {
			ri := &restore.RegionInfo{
				Region: &metapb.Region{
					Id:    i + 1,
					Peers: peers,
				},
			}

			if i != 0 {
				startKey := make([]byte, 8)
				binary.BigEndian.PutUint64(startKey, i)
				ri.Region.StartKey = codec.EncodeBytes([]byte{}, startKey)
			}
			endKey = make([]byte, 8)
			binary.BigEndian.PutUint64(endKey, i+1)
			ri.Region.EndKey = codec.EncodeBytes([]byte{}, endKey)

			regionsMap[i] = ri
			regions = append(regions, ri)
		}

		if num == 1 {
			endKey = []byte{}
		} else {
			endKey = codec.EncodeBytes([]byte{}, endKey)
		}
		ri := &restore.RegionInfo{
			Region: &metapb.Region{
				Id:       num,
				Peers:    peers,
				StartKey: endKey,
				EndKey:   []byte{},
			},
		}
		regionsMap[num] = ri
		regions = append(regions, ri)

		return regionsMap, regions
	}

	ctx := context.Background()
	regionMap := make(map[uint64]*restore.RegionInfo)
	var regions []*restore.RegionInfo
	var batch []*restore.RegionInfo
	_, err := restore.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	require.Error(t, err)
	require.True(t, berrors.ErrPDBatchScanRegion.Equal(err))
	require.Regexp(t, ".*scan region return empty result.*", err.Error())

	regionMap, regions = makeRegions(1)
	batch, err = restore.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	require.NoError(t, err)
	require.Equal(t, regions, batch)

	regionMap, regions = makeRegions(2)
	batch, err = restore.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	require.NoError(t, err)
	require.Equal(t, regions, batch)

	regionMap, regions = makeRegions(3)
	batch, err = restore.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	require.NoError(t, err)
	require.Equal(t, regions, batch)

	regionMap, regions = makeRegions(8)
	batch, err = restore.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	require.NoError(t, err)
	require.Equal(t, regions, batch)

	regionMap, regions = makeRegions(8)
	batch, err = restore.PaginateScanRegion(
		ctx, NewTestClient(stores, regionMap, 0), regions[1].Region.StartKey, []byte{}, 3)
	require.NoError(t, err)
	require.Equal(t, regions[1:], batch)

	batch, err = restore.PaginateScanRegion(
		ctx, NewTestClient(stores, regionMap, 0), []byte{}, regions[6].Region.EndKey, 3)
	require.NoError(t, err)
	require.Equal(t, regions[:7], batch)

	batch, err = restore.PaginateScanRegion(
		ctx, NewTestClient(stores, regionMap, 0), regions[1].Region.StartKey, regions[1].Region.EndKey, 3)
	require.NoError(t, err)
	require.Equal(t, regions[1:2], batch)

	_, err = restore.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{2}, []byte{1}, 3)
	require.Error(t, err)
	require.True(t, berrors.ErrRestoreInvalidRange.Equal(err))
	require.Regexp(t, ".*startKey > endKey.*", err.Error())

	tc := NewTestClient(stores, regionMap, 0)
	tc.InjectErr = true
	_, err = restore.PaginateScanRegion(ctx, tc, regions[1].Region.EndKey, regions[5].Region.EndKey, 3)
	require.Error(t, err)
	require.Regexp(t, ".*mock scan error.*", err.Error())

	// make the regionMap losing some region, this will cause scan region check fails
	delete(regionMap, uint64(3))
	_, err = restore.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), regions[1].Region.EndKey, regions[5].Region.EndKey, 3)
	require.Error(t, err)
	require.True(t, berrors.ErrPDBatchScanRegion.Equal(err))
	require.Regexp(t, ".*region endKey not equal to next region startKey.*", err.Error())

}

func TestRewriteFileKeys(t *testing.T) {
	rewriteRules := restore.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				NewKeyPrefix: tablecodec.GenTablePrefix(2),
				OldKeyPrefix: tablecodec.GenTablePrefix(1),
			},
		},
	}
	rawKeyFile := backuppb.File{
		Name:     "backup.sst",
		StartKey: tablecodec.GenTableRecordPrefix(1),
		EndKey:   tablecodec.GenTableRecordPrefix(1).PrefixNext(),
	}
	start, end, err := restore.RewriteFileKeys(&rawKeyFile, &rewriteRules)
	require.NoError(t, err)
	_, end, err = codec.DecodeBytes(end, nil)
	require.NoError(t, err)
	_, start, err = codec.DecodeBytes(start, nil)
	require.NoError(t, err)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(2)), start)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(2).PrefixNext()), end)

	encodeKeyFile := backuppb.DataFileInfo{
		Path:     "bakcup.log",
		StartKey: codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(1)),
		EndKey:   codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(1).PrefixNext()),
	}
	start, end, err = restore.RewriteFileKeys(&encodeKeyFile, &rewriteRules)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(2)), start)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(2).PrefixNext()), end)
}
