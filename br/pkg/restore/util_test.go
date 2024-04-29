// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"fmt"
	"math/rand"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	recover_data "github.com/pingcap/kvproto/pkg/recoverdatapb"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestGetKeyRangeByMode(t *testing.T) {
	file := &backuppb.File{
		Name:     "file_write.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte("t1ccc"),
	}
	endFile := &backuppb.File{
		Name:     "file_write.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte(""),
	}
	rule := &RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: []byte("t1"),
				NewKeyPrefix: []byte("t2"),
			},
		},
	}
	// raw kv
	testRawFn := getKeyRangeByMode(Raw)
	start, end, err := testRawFn(file, rule)
	require.NoError(t, err)
	require.Equal(t, []byte("t1a"), start)
	require.Equal(t, []byte("t1ccc"), end)

	start, end, err = testRawFn(endFile, rule)
	require.NoError(t, err)
	require.Equal(t, []byte("t1a"), start)
	require.Equal(t, []byte(""), end)

	// txn kv: the keys must be encoded.
	testTxnFn := getKeyRangeByMode(Txn)
	start, end, err = testTxnFn(file, rule)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t1a")), start)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t1ccc")), end)

	start, end, err = testTxnFn(endFile, rule)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t1a")), start)
	require.Equal(t, []byte(""), end)

	// normal kv: the keys must be encoded.
	testFn := getKeyRangeByMode(TiDB)
	start, end, err = testFn(file, rule)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t2a")), start)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t2ccc")), end)

	// TODO maybe fix later
	// current restore does not support rewrite empty endkey.
	// because backup guarantees that the end key is not empty.
	// start, end, err = testFn(endFile, rule)
	// require.NoError(t, err)
	// require.Equal(t, codec.EncodeBytes(nil, []byte("t2a")), start)
	// require.Equal(t, []byte(""), end)
}

func TestParseQuoteName(t *testing.T) {
	schema, table := ParseQuoteName("`a`.`b`")
	require.Equal(t, "a", schema)
	require.Equal(t, "b", table)

	schema, table = ParseQuoteName("`a``b`.``````")
	require.Equal(t, "a`b", schema)
	require.Equal(t, "``", table)

	schema, table = ParseQuoteName("`.`.`.`")
	require.Equal(t, ".", schema)
	require.Equal(t, ".", table)

	schema, table = ParseQuoteName("`.``.`.`.`")
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
	sstMeta, err := GetSSTMetaFromFile([]byte{}, file, region, rule, RewriteModeLegacy)
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

	result := MapTableToFiles(append(filesOfTable2, filesOfTable1...))

	require.Equal(t, filesOfTable1, result[1])
	require.Equal(t, filesOfTable2, result[2])
}

func TestValidateFileRewriteRule(t *testing.T) {
	rules := &RewriteRules{
		Data: []*import_sstpb.RewriteRule{{
			OldKeyPrefix: []byte(tablecodec.EncodeTablePrefix(1)),
			NewKeyPrefix: []byte(tablecodec.EncodeTablePrefix(2)),
		}},
	}

	// Empty start/end key is not allowed.
	err := ValidateFileRewriteRule(
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
	err = ValidateFileRewriteRule(
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
	err = ValidateFileRewriteRule(
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
	err = ValidateFileRewriteRule(
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
	err = ValidateFileRewriteRule(
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

func TestRewriteFileKeys(t *testing.T) {
	rewriteRules := RewriteRules{
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
	start, end, err := GetRewriteRawKeys(&rawKeyFile, &rewriteRules)
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
	start, end, err = GetRewriteEncodedKeys(&encodeKeyFile, &rewriteRules)
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
	start, end, err = GetRewriteRawKeys(&encodeKeyFile767, &rewriteRules)
	require.NoError(t, err)
	require.NotEqual(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(511)), start)
	require.NotEqual(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(511).PrefixNext()), end)
	// use encode rewrite should no error and equal
	start, end, err = GetRewriteEncodedKeys(&encodeKeyFile767, &rewriteRules)
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
) *RecoverRegion {
	return &RecoverRegion{
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

func newRecoverRegionInfo(r *RecoverRegion) *RecoverRegionInfo {
	return &RecoverRegionInfo{
		RegionVersion: r.Version,
		RegionId:      r.RegionId,
		StartKey:      PrefixStartKey(r.StartKey),
		EndKey:        PrefixEndKey(r.EndKey),
		TombStone:     r.Tombstone,
	}
}

func TestSortRecoverRegions(t *testing.T) {
	selectedPeer1 := newPeerMeta(9, 11, 2, []byte("aa"), nil, 2, 0, 0, 0, false)
	selectedPeer2 := newPeerMeta(19, 22, 3, []byte("bbb"), nil, 2, 1, 0, 1, false)
	selectedPeer3 := newPeerMeta(29, 30, 1, []byte("c"), nil, 2, 1, 1, 2, false)
	regions := map[uint64][]*RecoverRegion{
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
	regionsInfos := SortRecoverRegions(regions)
	expectRegionInfos := []*RecoverRegionInfo{
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

	validRegionInfos := []*RecoverRegionInfo{
		newRecoverRegionInfo(validPeer1),
		newRecoverRegionInfo(validPeer2),
		newRecoverRegionInfo(validPeer3),
	}

	validPeer, err := CheckConsistencyAndValidPeer(validRegionInfos)
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

	invalidRegionInfos := []*RecoverRegionInfo{
		newRecoverRegionInfo(invalidPeer1),
		newRecoverRegionInfo(invalidPeer2),
		newRecoverRegionInfo(invalidPeer3),
	}

	_, err = CheckConsistencyAndValidPeer(invalidRegionInfos)
	require.Error(t, err)
	require.Regexp(t, ".*invalid restore range.*", err.Error())
}

func TestLeaderCandidates(t *testing.T) {
	//key space is continuous
	validPeer1 := newPeerMeta(9, 11, 2, []byte(""), []byte("bb"), 2, 1, 0, 0, false)
	validPeer2 := newPeerMeta(19, 22, 3, []byte("bb"), []byte("cc"), 2, 1, 0, 1, false)
	validPeer3 := newPeerMeta(29, 30, 1, []byte("cc"), []byte(""), 2, 1, 0, 2, false)

	peers := []*RecoverRegion{
		validPeer1,
		validPeer2,
		validPeer3,
	}

	candidates, err := LeaderCandidates(peers)
	require.NoError(t, err)
	require.Equal(t, 3, len(candidates))
}

func TestSelectRegionLeader(t *testing.T) {
	validPeer1 := newPeerMeta(9, 11, 2, []byte(""), []byte("bb"), 2, 1, 0, 0, false)
	validPeer2 := newPeerMeta(19, 22, 3, []byte("bb"), []byte("cc"), 2, 1, 0, 1, false)
	validPeer3 := newPeerMeta(29, 30, 1, []byte("cc"), []byte(""), 2, 1, 0, 2, false)

	peers := []*RecoverRegion{
		validPeer1,
		validPeer2,
		validPeer3,
	}
	// init store banlance score all is 0
	storeBalanceScore := make(map[uint64]int, len(peers))
	leader := SelectRegionLeader(storeBalanceScore, peers)
	require.Equal(t, validPeer1, leader)

	// change store banlance store
	storeBalanceScore[2] = 3
	storeBalanceScore[3] = 2
	storeBalanceScore[1] = 1
	leader = SelectRegionLeader(storeBalanceScore, peers)
	require.Equal(t, validPeer3, leader)

	// one peer
	peer := []*RecoverRegion{
		validPeer3,
	}
	// init store banlance score all is 0
	storeScore := make(map[uint64]int, len(peer))
	leader = SelectRegionLeader(storeScore, peer)
	require.Equal(t, validPeer3, leader)
}

func TestLogFilesSkipMap(t *testing.T) {
	var (
		metaNum  = 2
		groupNum = 4
		fileNum  = 1000

		ratio = 0.1
	)

	for ratio < 1 {
		skipmap := NewLogFilesSkipMap()
		nativemap := make(map[string]map[int]map[int]struct{})
		count := 0
		for i := 0; i < int(ratio*float64(metaNum*groupNum*fileNum)); i++ {
			metaKey := fmt.Sprint(rand.Intn(metaNum))
			groupOff := rand.Intn(groupNum)
			fileOff := rand.Intn(fileNum)

			mp, exists := nativemap[metaKey]
			if !exists {
				mp = make(map[int]map[int]struct{})
				nativemap[metaKey] = mp
			}
			gp, exists := mp[groupOff]
			if !exists {
				gp = make(map[int]struct{})
				mp[groupOff] = gp
			}
			if _, exists := gp[fileOff]; !exists {
				gp[fileOff] = struct{}{}
				skipmap.Insert(metaKey, groupOff, fileOff)
				count += 1
			}
		}

		ncount := 0
		for metaKey, mp := range nativemap {
			for groupOff, gp := range mp {
				for fileOff := range gp {
					require.True(t, skipmap.NeedSkip(metaKey, groupOff, fileOff))
					ncount++
				}
			}
		}

		require.Equal(t, count, ncount)

		continueFunc := func(metaKey string, groupi, filei int) bool {
			mp, exists := nativemap[metaKey]
			if !exists {
				return false
			}
			gp, exists := mp[groupi]
			if !exists {
				return false
			}
			_, exists = gp[filei]
			return exists
		}

		for metai := 0; metai < metaNum; metai++ {
			metaKey := fmt.Sprint(metai)
			for groupi := 0; groupi < groupNum; groupi++ {
				for filei := 0; filei < fileNum; filei++ {
					if continueFunc(metaKey, groupi, filei) {
						continue
					}
					require.False(t, skipmap.NeedSkip(metaKey, groupi, filei))
				}
			}
		}

		ratio = ratio * 2
	}
}
