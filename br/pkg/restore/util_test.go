// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"encoding/binary"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	recover_data "github.com/pingcap/kvproto/pkg/recoverdatapb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/split"
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
	sstMeta, err := restore.GetSSTMetaFromFile([]byte{}, file, region, rule, restore.RewriteModeLegacy)
	require.Nil(t, err)
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

	makeRegions := func(num uint64) (map[uint64]*split.RegionInfo, []*split.RegionInfo) {
		regionsMap := make(map[uint64]*split.RegionInfo, num)
		regions := make([]*split.RegionInfo, 0, num)
		endKey := make([]byte, 8)
		for i := uint64(0); i < num-1; i++ {
			ri := &split.RegionInfo{
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
		ri := &split.RegionInfo{
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
	regionMap := make(map[uint64]*split.RegionInfo)
	var regions []*split.RegionInfo
	var batch []*split.RegionInfo
	backup := split.ScanRegionAttemptTimes
	split.ScanRegionAttemptTimes = 3
	defer func() {
		split.ScanRegionAttemptTimes = backup
	}()
	_, err := split.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	require.Error(t, err)
	require.True(t, berrors.ErrPDBatchScanRegion.Equal(err))
	require.Regexp(t, ".*scan region return empty result.*", err.Error())

	regionMap, regions = makeRegions(1)
	tc := NewTestClient(stores, regionMap, 0)
	tc.InjectErr = true
	tc.InjectTimes = 2
	batch, err = split.PaginateScanRegion(ctx, tc, []byte{}, []byte{}, 3)
	require.NoError(t, err)
	require.Equal(t, regions, batch)

	regionMap, regions = makeRegions(2)
	batch, err = split.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	require.NoError(t, err)
	require.Equal(t, regions, batch)

	regionMap, regions = makeRegions(3)
	batch, err = split.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	require.NoError(t, err)
	require.Equal(t, regions, batch)

	regionMap, regions = makeRegions(8)
	batch, err = split.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	require.NoError(t, err)
	require.Equal(t, regions, batch)

	regionMap, regions = makeRegions(8)
	batch, err = split.PaginateScanRegion(
		ctx, NewTestClient(stores, regionMap, 0), regions[1].Region.StartKey, []byte{}, 3)
	require.NoError(t, err)
	require.Equal(t, regions[1:], batch)

	batch, err = split.PaginateScanRegion(
		ctx, NewTestClient(stores, regionMap, 0), []byte{}, regions[6].Region.EndKey, 3)
	require.NoError(t, err)
	require.Equal(t, regions[:7], batch)

	batch, err = split.PaginateScanRegion(
		ctx, NewTestClient(stores, regionMap, 0), regions[1].Region.StartKey, regions[1].Region.EndKey, 3)
	require.NoError(t, err)
	require.Equal(t, regions[1:2], batch)

	_, err = split.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{2}, []byte{1}, 3)
	require.Error(t, err)
	require.True(t, berrors.ErrRestoreInvalidRange.Equal(err))
	require.Regexp(t, ".*startKey > endKey.*", err.Error())

	tc = NewTestClient(stores, regionMap, 0)
	tc.InjectErr = true
	tc.InjectTimes = 5
	_, err = split.PaginateScanRegion(ctx, tc, []byte{}, []byte{}, 3)
	require.Error(t, err)
	require.True(t, berrors.ErrPDBatchScanRegion.Equal(err))

	// make the regionMap losing some region, this will cause scan region check fails
	delete(regionMap, uint64(3))
	_, err = split.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), regions[1].Region.EndKey, regions[5].Region.EndKey, 3)
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
			{
				NewKeyPrefix: tablecodec.GenTablePrefix(511),
				OldKeyPrefix: tablecodec.GenTablePrefix(767),
			},
		},
	}
	rawKeyFile := backuppb.File{
		Name:     "backup.sst",
		StartKey: tablecodec.GenTableRecordPrefix(1),
		EndKey:   tablecodec.GenTableRecordPrefix(1).PrefixNext(),
	}
	start, end, err := restore.GetRewriteRawKeys(&rawKeyFile, &rewriteRules)
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
	start, end, err = restore.GetRewriteEncodedKeys(&encodeKeyFile, &rewriteRules)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(2)), start)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(2).PrefixNext()), end)

	// test for table id 767
	encodeKeyFile767 := backuppb.DataFileInfo{
		Path:     "bakcup.log",
		StartKey: codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(767)),
		EndKey:   codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(767).PrefixNext()),
	}
	// use raw rewrite should no error but not equal
	start, end, err = restore.GetRewriteRawKeys(&encodeKeyFile767, &rewriteRules)
	require.NoError(t, err)
	require.NotEqual(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(511)), start)
	require.NotEqual(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(511).PrefixNext()), end)
	// use encode rewrite should no error and equal
	start, end, err = restore.GetRewriteEncodedKeys(&encodeKeyFile767, &rewriteRules)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(511)), start)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(511).PrefixNext()), end)
}

func newPeerMeta(
	regionId uint64,
	peerId uint64,
	storeId uint64,
	startKey []byte,
	endKey []byte,
	lastLogTerm uint64,
	lastIndex uint64,
	commitIndex uint64,
	version uint64,
	tombstone bool,
) *restore.RecoverRegion {
	return &restore.RecoverRegion{
		&recover_data.RegionMeta{
			RegionId:    regionId,
			PeerId:      peerId,
			StartKey:    startKey,
			EndKey:      endKey,
			LastLogTerm: lastLogTerm,
			LastIndex:   lastIndex,
			CommitIndex: commitIndex,
			Version:     version,
			Tombstone:   tombstone,
		},
		storeId,
	}
}

func newRecoverRegionInfo(r *restore.RecoverRegion) *restore.RecoverRegionInfo {
	return &restore.RecoverRegionInfo{
		RegionVersion: r.Version,
		RegionId:      r.RegionId,
		StartKey:      restore.PrefixStartKey(r.StartKey),
		EndKey:        restore.PrefixEndKey(r.EndKey),
		TombStone:     r.Tombstone,
	}
}

func TestSortRecoverRegions(t *testing.T) {
	selectedPeer1 := newPeerMeta(9, 11, 2, []byte("aa"), nil, 2, 0, 0, 0, false)
	selectedPeer2 := newPeerMeta(19, 22, 3, []byte("bbb"), nil, 2, 1, 0, 1, false)
	selectedPeer3 := newPeerMeta(29, 30, 1, []byte("c"), nil, 2, 1, 1, 2, false)
	regions := map[uint64][]*restore.RecoverRegion{
		9: {
			// peer 11 should be selected because of log term
			newPeerMeta(9, 10, 1, []byte("a"), nil, 1, 1, 1, 1, false),
			selectedPeer1,
			newPeerMeta(9, 12, 3, []byte("aaa"), nil, 0, 0, 0, 0, false),
		},
		19: {
			// peer 22 should be selected because of log index
			newPeerMeta(19, 20, 1, []byte("b"), nil, 1, 1, 1, 1, false),
			newPeerMeta(19, 21, 2, []byte("bb"), nil, 2, 0, 0, 0, false),
			selectedPeer2,
		},
		29: {
			// peer 30 should be selected because of log index
			selectedPeer3,
			newPeerMeta(29, 31, 2, []byte("cc"), nil, 2, 0, 0, 0, false),
			newPeerMeta(29, 32, 3, []byte("ccc"), nil, 2, 1, 0, 0, false),
		},
	}
	regionsInfos := restore.SortRecoverRegions(regions)
	expectRegionInfos := []*restore.RecoverRegionInfo{
		newRecoverRegionInfo(selectedPeer3),
		newRecoverRegionInfo(selectedPeer2),
		newRecoverRegionInfo(selectedPeer1),
	}
	require.Equal(t, expectRegionInfos, regionsInfos)
}

func TestCheckConsistencyAndValidPeer(t *testing.T) {
	//key space is continuous
	validPeer1 := newPeerMeta(9, 11, 2, []byte(""), []byte("bb"), 2, 0, 0, 0, false)
	validPeer2 := newPeerMeta(19, 22, 3, []byte("bb"), []byte("cc"), 2, 1, 0, 1, false)
	validPeer3 := newPeerMeta(29, 30, 1, []byte("cc"), []byte(""), 2, 1, 1, 2, false)

	validRegionInfos := []*restore.RecoverRegionInfo{
		newRecoverRegionInfo(validPeer1),
		newRecoverRegionInfo(validPeer2),
		newRecoverRegionInfo(validPeer3),
	}

	validPeer, err := restore.CheckConsistencyAndValidPeer(validRegionInfos)
	require.NoError(t, err)
	require.Equal(t, 3, len(validPeer))
	var regions = make(map[uint64]struct{}, 3)
	regions[9] = struct{}{}
	regions[19] = struct{}{}
	regions[29] = struct{}{}

	require.Equal(t, regions, validPeer)

	//key space is not continuous
	invalidPeer1 := newPeerMeta(9, 11, 2, []byte("aa"), []byte("cc"), 2, 0, 0, 0, false)
	invalidPeer2 := newPeerMeta(19, 22, 3, []byte("dd"), []byte("cc"), 2, 1, 0, 1, false)
	invalidPeer3 := newPeerMeta(29, 30, 1, []byte("cc"), []byte("dd"), 2, 1, 1, 2, false)

	invalidRegionInfos := []*restore.RecoverRegionInfo{
		newRecoverRegionInfo(invalidPeer1),
		newRecoverRegionInfo(invalidPeer2),
		newRecoverRegionInfo(invalidPeer3),
	}

	_, err = restore.CheckConsistencyAndValidPeer(invalidRegionInfos)
	require.Error(t, err)
	require.Regexp(t, ".*invalid restore range.*", err.Error())
}

func TestLeaderCandidates(t *testing.T) {
	//key space is continuous
	validPeer1 := newPeerMeta(9, 11, 2, []byte(""), []byte("bb"), 2, 1, 0, 0, false)
	validPeer2 := newPeerMeta(19, 22, 3, []byte("bb"), []byte("cc"), 2, 1, 0, 1, false)
	validPeer3 := newPeerMeta(29, 30, 1, []byte("cc"), []byte(""), 2, 1, 0, 2, false)

	peers := []*restore.RecoverRegion{
		validPeer1,
		validPeer2,
		validPeer3,
	}

	candidates, err := restore.LeaderCandidates(peers)
	require.NoError(t, err)
	require.Equal(t, 3, len(candidates))
}

func TestSelectRegionLeader(t *testing.T) {
	validPeer1 := newPeerMeta(9, 11, 2, []byte(""), []byte("bb"), 2, 1, 0, 0, false)
	validPeer2 := newPeerMeta(19, 22, 3, []byte("bb"), []byte("cc"), 2, 1, 0, 1, false)
	validPeer3 := newPeerMeta(29, 30, 1, []byte("cc"), []byte(""), 2, 1, 0, 2, false)

	peers := []*restore.RecoverRegion{
		validPeer1,
		validPeer2,
		validPeer3,
	}
	// init store banlance score all is 0
	storeBalanceScore := make(map[uint64]int, len(peers))
	leader := restore.SelectRegionLeader(storeBalanceScore, peers)
	require.Equal(t, validPeer1, leader)

	// change store banlance store
	storeBalanceScore[2] = 3
	storeBalanceScore[3] = 2
	storeBalanceScore[1] = 1
	leader = restore.SelectRegionLeader(storeBalanceScore, peers)
	require.Equal(t, validPeer3, leader)

	// one peer
	peer := []*restore.RecoverRegion{
		validPeer3,
	}
	// init store banlance score all is 0
	storeScore := make(map[uint64]int, len(peer))
	leader = restore.SelectRegionLeader(storeScore, peer)
	require.Equal(t, validPeer3, leader)
}
