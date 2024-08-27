// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestStartWithComparator(t *testing.T) {
	comparator := NewStartWithComparator()
	require.True(t, comparator.Compare([]byte("aa_key"), []byte("aa")))
	require.False(t, comparator.Compare([]byte("aa_key"), []byte("aak")))
	require.False(t, comparator.Compare([]byte("aa_key"), []byte("bb")))
}

func fakeStorage(t *testing.T) storage.ExternalStorage {
	baseDir := t.TempDir()
	s, err := storage.NewLocalStorage(baseDir)
	require.NoError(t, err)
	return s
}

func encodeKey(key string, ts int64) []byte {
	encodedKey := codec.EncodeBytes([]byte{}, []byte(key))
	return codec.EncodeUintDesc(encodedKey, uint64(ts))
}

func encodeShortValue(val string, ts int64) []byte {
	flagShortValuePrefix := byte('v')
	valBytes := []byte(val)
	buff := make([]byte, 0, 11+len(valBytes))
	buff = append(buff, byte('P'))
	buff = codec.EncodeUvarint(buff, uint64(ts))
	buff = append(buff, flagShortValuePrefix)
	buff = append(buff, byte(len(valBytes)))
	buff = append(buff, valBytes...)
	return buff
}

type cf struct {
	key      string
	startTs  int64
	commitTS int64
	val      string
}

func fakeCFs() (defaultCFs, writeCFs []*cf) {
	defaultCFs = []*cf{
		{
			key:     "aa_big_key_1",
			startTs: time.Now().UnixNano(),
			val:     "aa_big_val_1",
		},
		{
			key:     "bb_big_key_1",
			startTs: time.Now().UnixNano(),
			val:     "bb_big_val_1",
		},
		{
			key:     "cc_big_key_1",
			startTs: time.Now().UnixNano(),
			val:     "cc_big_val_1",
		},
	}

	writeCFs = []*cf{
		{
			key:      "aa_small_key_1",
			startTs:  time.Now().UnixNano(),
			commitTS: time.Now().UnixNano(),
			val:      "aa_small_val_1",
		},
		{
			key:      "aa_big_key_1",
			startTs:  defaultCFs[0].startTs,
			commitTS: time.Now().UnixNano(),
			val:      "aa_short_val_1",
		},
		{
			key:      "bb_small_key_1",
			startTs:  time.Now().UnixNano(),
			commitTS: time.Now().UnixNano(),
			val:      "bb_small_val_1",
		},
		{
			key:      "bb_big_key_1",
			startTs:  defaultCFs[1].startTs,
			commitTS: time.Now().UnixNano(),
			val:      "bb_short_val_1",
		},
	}

	return
}

func fakeDataFile(t *testing.T, s storage.ExternalStorage) (defaultCFDataFile, writeCFDataFile *backuppb.DataFileInfo) {
	const (
		defaultCFFile = "default_cf"
		writeCFFile   = "write_cf"
	)
	defaultCFs, writeCFs := fakeCFs()
	ctx := context.Background()
	defaultCFBuf := bytes.NewBuffer([]byte{})
	for _, defaultCF := range defaultCFs {
		defaultCFBuf.Write(EncodeKVEntry(encodeKey(defaultCF.key, defaultCF.startTs), []byte(defaultCF.val)))
	}

	err := s.WriteFile(ctx, defaultCFFile, defaultCFBuf.Bytes())
	require.NoError(t, err)
	defaultCFCheckSum := sha256.Sum256(defaultCFBuf.Bytes())
	defaultCFDataFile = &backuppb.DataFileInfo{
		Path:   defaultCFFile,
		Cf:     DefaultCF,
		Sha256: defaultCFCheckSum[:],
	}

	writeCFBuf := bytes.NewBuffer([]byte{})
	for _, writeCF := range writeCFs {
		writeCFBuf.Write(EncodeKVEntry(encodeKey(writeCF.key, writeCF.commitTS), encodeShortValue(writeCF.val, writeCF.startTs)))
	}

	err = s.WriteFile(ctx, writeCFFile, writeCFBuf.Bytes())
	require.NoError(t, err)
	writeCFCheckSum := sha256.Sum256(writeCFBuf.Bytes())
	writeCFDataFile = &backuppb.DataFileInfo{
		Path:   writeCFFile,
		Cf:     WriteCF,
		Sha256: writeCFCheckSum[:],
	}

	return
}

func TestSearchFromDataFile(t *testing.T) {
	s := fakeStorage(t)
	defaultCFDataFile, writeCFDataFile := fakeDataFile(t, s)
	comparator := NewStartWithComparator()
	searchKey := []byte("aa_big_key_1")
	bs := NewStreamBackupSearch(s, comparator, searchKey)
	ch := make(chan *StreamKVInfo, 16)
	ctx := context.Background()

	err := bs.searchFromDataFile(ctx, defaultCFDataFile, ch)
	require.NoError(t, err)
	err = bs.searchFromDataFile(ctx, writeCFDataFile, ch)
	require.NoError(t, err)
	close(ch)

	hexSearchKey := strings.ToUpper(hex.EncodeToString(searchKey))
	searchKeyCount := 0
	for kvEntry := range ch {
		require.True(t, strings.HasPrefix(kvEntry.Key, hexSearchKey))
		searchKeyCount++
	}

	require.Equal(t, 2, searchKeyCount)
}

func TestMergeCFEntries(t *testing.T) {
	defaultCFs, writeCFs := fakeCFs()
	defaultCFEntries := make(map[string]*StreamKVInfo, 8)
	writeCFEntries := make(map[string]*StreamKVInfo, 8)

	for _, defaultCF := range defaultCFs {
		encodedKey := hex.EncodeToString(encodeKey(defaultCF.key, defaultCF.startTs))
		defaultCFEntries[encodedKey] = &StreamKVInfo{
			Key:        hex.EncodeToString([]byte(defaultCF.key)),
			EncodedKey: encodedKey,
			StartTs:    uint64(defaultCF.startTs),
			CFName:     DefaultCF,
			Value:      defaultCF.val,
		}
	}
	for _, writeCF := range writeCFs {
		encodedKey := hex.EncodeToString(encodeKey(writeCF.key, writeCF.commitTS))
		writeCFEntries[encodedKey] = &StreamKVInfo{
			Key:        hex.EncodeToString([]byte(writeCF.key)),
			EncodedKey: encodedKey,
			StartTs:    uint64(writeCF.startTs),
			CommitTs:   uint64(writeCF.commitTS),
			CFName:     WriteCF,
			Value:      writeCF.val,
		}
	}

	s := fakeStorage(t)
	comparator := NewStartWithComparator()
	bs := NewStreamBackupSearch(s, comparator, []byte{})
	kvEntries := bs.mergeCFEntries(defaultCFEntries, writeCFEntries)
	require.Equal(t, len(writeCFs)+1, len(kvEntries))
}
