// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package logutil_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func assertTrimEqual(t *testing.T, f zapcore.Field, expect string) {
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{f})
	require.NoError(t, err)
	require.JSONEq(t, expect, strings.TrimRight(out.String(), "\n"))
}

func newFile(j int) *backuppb.File {
	return &backuppb.File{
		Name:         strconv.Itoa(j),
		StartKey:     []byte(strconv.Itoa(j)),
		EndKey:       []byte(strconv.Itoa(j + 1)),
		TotalKvs:     uint64(j),
		TotalBytes:   uint64(j),
		StartVersion: uint64(j),
		EndVersion:   uint64(j + 1),
		Crc64Xor:     uint64(j),
		Sha256:       []byte(strconv.Itoa(j)),
		Cf:           "write",
		Size_:        uint64(j),
	}
}

func TestRater(t *testing.T) {
	m := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "testing",
		Name:      "rater",
		Help:      "A testing counter for the rater",
	})
	m.Add(42)

	rater := logutil.TraceRateOver(m)
	timePass := time.Now()
	rater.Inc()
	require.InEpsilon(t, 10.0, rater.RateAt(timePass.Add(100*time.Millisecond)), 0.1)
	rater.Inc()
	require.InEpsilon(t, 13.0, rater.RateAt(timePass.Add(150*time.Millisecond)), 0.1)
	rater.Add(18)
	require.InEpsilon(t, 100.0, rater.RateAt(timePass.Add(200*time.Millisecond)), 0.1)
}

func TestFile(t *testing.T) {
	assertTrimEqual(t, logutil.File(newFile(1)),
		`{"file": {"name": "1", "CF": "write", "sha256": "31", "startKey": "31", "endKey": "32", "startVersion": 1, "endVersion": 2, "totalKvs": 1, "totalBytes": 1, "CRC64Xor": 1}}`)
}

func TestFiles(t *testing.T) {
	cases := []struct {
		count  int
		expect string
	}{
		{0, `{"files": {"total": 0, "files": [], "totalKVs": 0, "totalBytes": 0, "totalSize": 0}}`},
		{1, `{"files": {"total": 1, "files": ["0"], "totalKVs": 0, "totalBytes": 0, "totalSize": 0}}`},
		{2, `{"files": {"total": 2, "files": ["0", "1"], "totalKVs": 1, "totalBytes": 1, "totalSize": 1}}`},
		{3, `{"files": {"total": 3, "files": ["0", "1", "2"], "totalKVs": 3, "totalBytes": 3, "totalSize": 3}}`},
		{4, `{"files": {"total": 4, "files": ["0", "1", "2", "3"], "totalKVs": 6, "totalBytes": 6, "totalSize": 6}}`},
		{5, `{"files": {"total": 5, "files": ["0", "(skip 3)", "4"], "totalKVs": 10, "totalBytes": 10, "totalSize": 10}}`},
		{6, `{"files": {"total": 6, "files": ["0", "(skip 4)", "5"], "totalKVs": 15, "totalBytes": 15, "totalSize": 15}}`},
		{1024, `{"files": {"total": 1024, "files": ["0", "(skip 1022)", "1023"], "totalKVs": 523776, "totalBytes": 523776, "totalSize": 523776}}`},
	}

	for _, cs := range cases {
		ranges := make([]*backuppb.File, cs.count)
		for j := 0; j < cs.count; j++ {
			ranges[j] = newFile(j)
		}
		assertTrimEqual(t, logutil.Files(ranges), cs.expect)
	}
}

func TestKey(t *testing.T) {
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{logutil.Key("test", []byte{0, 1, 2, 3})})
	require.NoError(t, err)
	require.JSONEq(t, `{"test": "00010203"}`, strings.Trim(out.String(), "\n"))
}

func TestKeys(t *testing.T) {
	cases := []struct {
		count  int
		expect string
	}{
		{0, `{"keys": {"total": 0, "keys": []}}`},
		{1, `{"keys": {"total": 1, "keys": ["30303030"]}}`},
		{2, `{"keys": {"total": 2, "keys": ["30303030", "30303031"]}}`},
		{3, `{"keys": {"total": 3, "keys": ["30303030", "30303031", "30303032"]}}`},
		{4, `{"keys": {"total": 4, "keys": ["30303030", "30303031", "30303032", "30303033"]}}`},
		{5, `{"keys": {"total": 5, "keys": ["30303030", "(skip 3)", "30303034"]}}`},
		{6, `{"keys": {"total": 6, "keys": ["30303030", "(skip 4)", "30303035"]}}`},
		{1024, `{"keys": {"total": 1024, "keys": ["30303030", "(skip 1022)", "31303233"]}}`},
	}

	for _, cs := range cases {
		keys := make([][]byte, cs.count)
		for j := 0; j < cs.count; j++ {
			keys[j] = []byte(fmt.Sprintf("%04d", j))
		}
		assertTrimEqual(t, logutil.Keys(keys), cs.expect)
	}
}

func TestRewriteRule(t *testing.T) {
	rule := &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("old"),
		NewKeyPrefix: []byte("new"),
		NewTimestamp: 0x555555,
	}

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{logutil.RewriteRule(rule)})
	require.NoError(t, err)
	require.JSONEq(t, `{"rewriteRule": {"oldKeyPrefix": "6f6c64", "newKeyPrefix": "6e6577", "newTimestamp": 5592405}}`, strings.Trim(out.String(), "\n"))
}

func TestRegion(t *testing.T) {
	region := &metapb.Region{
		Id:          1,
		StartKey:    []byte{0x00, 0x01},
		EndKey:      []byte{0x00, 0x02},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       []*metapb.Peer{{Id: 2, StoreId: 3}, {Id: 4, StoreId: 5}},
	}

	assertTrimEqual(t, logutil.Region(region),
		`{"region": {"ID": 1, "startKey": "0001", "endKey": "0002", "epoch": "conf_ver:1 version:1 ", "peers": "id:2 store_id:3 ,id:4 store_id:5 "}}`)
}

func TestLeader(t *testing.T) {
	leader := &metapb.Peer{Id: 2, StoreId: 3}

	assertTrimEqual(t, logutil.Leader(leader), `{"leader": "id:2 store_id:3 "}`)
}

func TestSSTMeta(t *testing.T) {
	meta := &import_sstpb.SSTMeta{
		Uuid: []byte("mock uuid"),
		Range: &import_sstpb.Range{
			Start: []byte{0x00, 0x01},
			End:   []byte{0x00, 0x02},
		},
		Crc32:       uint32(0x555555),
		Length:      1,
		CfName:      "default",
		RegionId:    1,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}

	assertTrimEqual(t, logutil.SSTMeta(meta),
		`{"sstMeta": {"CF": "default", "endKeyExclusive": false, "CRC32": 5592405, "length": 1, "regionID": 1, "regionEpoch": "conf_ver:1 version:1 ", "startKey": "0001", "endKey": "0002", "UUID": "invalid UUID 6d6f636b2075756964"}}`)
}

func TestShortError(t *testing.T) {
	err := errors.Annotate(berrors.ErrInvalidArgument, "test")

	assertTrimEqual(t, logutil.ShortError(err), `{"error": "test: [BR:Common:ErrInvalidArgument]invalid argument"}`)
}

func TestContextual(t *testing.T) {
	testCore, logs := observer.New(zap.InfoLevel)
	logutil.ResetGlobalLogger(zap.New(testCore))

	ctx := context.Background()
	l0 := logutil.LoggerFromContext(ctx)
	l0.Info("going to take an adventure?", zap.Int("HP", 50), zap.Int("HP-MAX", 50), zap.String("character", "solte"))
	lctx := logutil.ContextWithField(ctx, zap.Strings("friends", []string{"firo", "seren", "black"}))
	l := logutil.LoggerFromContext(lctx)
	l.Info("let's go!", zap.String("character", "solte"))

	observedLogs := logs.TakeAll()
	checkLog(t, observedLogs[0],
		"going to take an adventure?", zap.Int("HP", 50), zap.Int("HP-MAX", 50), zap.String("character", "solte"))
	checkLog(t, observedLogs[1],
		"let's go!", zap.Strings("friends", []string{"firo", "seren", "black"}), zap.String("character", "solte"))
}

func checkLog(t *testing.T, actual observer.LoggedEntry, message string, fields ...zap.Field) {
	require.Equal(t, message, actual.Message)
	for i, f := range fields {
		require.Truef(t, f.Equals(actual.Context[i]), "Expected field(%+v) does not equal to actual one(%+v).", f, actual.Context[i])
	}
}
