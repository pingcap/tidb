// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package logutil_test

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testLoggingSuite{})

type testLoggingSuite struct{}

func assertTrimEqual(c *C, f zapcore.Field, expect string) {
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{f})
	c.Assert(err, IsNil)
	c.Assert(strings.TrimRight(out.String(), "\n"), Equals, expect)
}

func newFile(j int) *backuppb.File {
	return &backuppb.File{
		Name:         fmt.Sprint(j),
		StartKey:     []byte(fmt.Sprint(j)),
		EndKey:       []byte(fmt.Sprint(j + 1)),
		TotalKvs:     uint64(j),
		TotalBytes:   uint64(j),
		StartVersion: uint64(j),
		EndVersion:   uint64(j + 1),
		Crc64Xor:     uint64(j),
		Sha256:       []byte(fmt.Sprint(j)),
		Cf:           "write",
		Size_:        uint64(j),
	}
}

type isAbout struct{}

func (isAbout) Info() *CheckerInfo {
	return &CheckerInfo{
		Name: "isAbout",
		Params: []string{
			"actual",
			"expect",
		},
	}
}

func (isAbout) Check(params []interface{}, names []string) (result bool, error string) {
	actual := params[0].(float64)
	expect := params[1].(float64)

	if diff := math.Abs(1 - (actual / expect)); diff > 0.1 {
		return false, fmt.Sprintf("The diff(%.2f) between actual(%.2f) and expect(%.2f) is too huge.", diff, actual, expect)
	}
	return true, ""
}

func (s *testLoggingSuite) TestRater(c *C) {
	m := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "testing",
		Name:      "rater",
		Help:      "A testing counter for the rater",
	})
	m.Add(42)

	rater := logutil.TraceRateOver(m)
	timePass := time.Now()
	rater.Inc()
	c.Assert(rater.RateAt(timePass.Add(100*time.Millisecond)), isAbout{}, 10.0)
	rater.Inc()
	c.Assert(rater.RateAt(timePass.Add(150*time.Millisecond)), isAbout{}, 13.0)
	rater.Add(18)
	c.Assert(rater.RateAt(timePass.Add(200*time.Millisecond)), isAbout{}, 100.0)
}

func (s *testLoggingSuite) TestFile(c *C) {
	assertTrimEqual(c, logutil.File(newFile(1)),
		`{"file": {"name": "1", "CF": "write", "sha256": "31", "startKey": "31", "endKey": "32", "startVersion": 1, "endVersion": 2, "totalKvs": 1, "totalBytes": 1, "CRC64Xor": 1}}`)
}

func (s *testLoggingSuite) TestFiles(c *C) {
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
		assertTrimEqual(c, logutil.Files(ranges), cs.expect)
	}
}

func (s *testLoggingSuite) TestKey(c *C) {
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{logutil.Key("test", []byte{0, 1, 2, 3})})
	c.Assert(err, IsNil)
	c.Assert(strings.Trim(out.String(), "\n"), Equals, `{"test": "00010203"}`)
}

func (s *testLoggingSuite) TestKeys(c *C) {
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
		assertTrimEqual(c, logutil.Keys(keys), cs.expect)
	}
}

func (s *testLoggingSuite) TestRewriteRule(c *C) {
	rule := &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("old"),
		NewKeyPrefix: []byte("new"),
		NewTimestamp: 0x555555,
	}

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{logutil.RewriteRule(rule)})
	c.Assert(err, IsNil)
	c.Assert(strings.Trim(out.String(), "\n"), Equals, `{"rewriteRule": {"oldKeyPrefix": "6f6c64", "newKeyPrefix": "6e6577", "newTimestamp": 5592405}}`)
}

func (s *testLoggingSuite) TestRegion(c *C) {
	region := &metapb.Region{
		Id:          1,
		StartKey:    []byte{0x00, 0x01},
		EndKey:      []byte{0x00, 0x02},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       []*metapb.Peer{{Id: 2, StoreId: 3}, {Id: 4, StoreId: 5}},
	}

	assertTrimEqual(c, logutil.Region(region),
		`{"region": {"ID": 1, "startKey": "0001", "endKey": "0002", "epoch": "conf_ver:1 version:1 ", "peers": "id:2 store_id:3 ,id:4 store_id:5 "}}`)
}

func (s *testLoggingSuite) TestLeader(c *C) {
	leader := &metapb.Peer{Id: 2, StoreId: 3}

	assertTrimEqual(c, logutil.Leader(leader), `{"leader": "id:2 store_id:3 "}`)
}

func (s *testLoggingSuite) TestSSTMeta(c *C) {
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

	assertTrimEqual(c, logutil.SSTMeta(meta),
		`{"sstMeta": {"CF": "default", "endKeyExclusive": false, "CRC32": 5592405, "length": 1, "regionID": 1, "regionEpoch": "conf_ver:1 version:1 ", "startKey": "0001", "endKey": "0002", "UUID": "invalid UUID 6d6f636b2075756964"}}`)
}

func (s *testLoggingSuite) TestShortError(c *C) {
	err := errors.Annotate(berrors.ErrInvalidArgument, "test")

	assertTrimEqual(c, logutil.ShortError(err), `{"error": "test: [BR:Common:ErrInvalidArgument]invalid argument"}`)
}
