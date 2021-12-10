// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"encoding/hex"
	"fmt"

	. "github.com/pingcap/check"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/config"
	"github.com/spf13/pflag"
)

var _ = Suite(&testCommonSuite{})

type testCommonSuite struct{}

type fakeValue string

func (f fakeValue) String() string {
	return string(f)
}

func (f fakeValue) Set(string) error {
	panic("implement me")
}

func (f fakeValue) Type() string {
	panic("implement me")
}

func (*testCommonSuite) TestUrlNoQuery(c *C) {
	flag := &pflag.Flag{
		Name:  flagStorage,
		Value: fakeValue("s3://some/what?secret=a123456789&key=987654321"),
	}

	field := flagToZapField(flag)
	c.Assert(field.Key, Equals, flagStorage)
	c.Assert(field.Interface.(fmt.Stringer).String(), Equals, "s3://some/what")
}

func (s *testCommonSuite) TestTiDBConfigUnchanged(c *C) {
	cfg := config.GetGlobalConfig()
	restoreConfig := enableTiDBConfig()
	c.Assert(cfg, Not(DeepEquals), config.GetGlobalConfig())
	restoreConfig()
	c.Assert(cfg, DeepEquals, config.GetGlobalConfig())
}

func (s *testCommonSuite) TestStripingPDURL(c *C) {
	nor1, err := normalizePDURL("https://pd:5432", true)
	c.Assert(err, IsNil)
	c.Assert(nor1, Equals, "pd:5432")
	_, err = normalizePDURL("https://pd.pingcap.com", false)
	c.Assert(err, ErrorMatches, ".*pd url starts with https while TLS disabled.*")
	_, err = normalizePDURL("http://127.0.0.1:2379", true)
	c.Assert(err, ErrorMatches, ".*pd url starts with http while TLS enabled.*")
	nor, err := normalizePDURL("http://127.0.0.1", false)
	c.Assert(nor, Equals, "127.0.0.1")
	c.Assert(err, IsNil)
	noChange, err := normalizePDURL("127.0.0.1:2379", false)
	c.Assert(err, IsNil)
	c.Assert(noChange, Equals, "127.0.0.1:2379")
}

func (s *testCommonSuite) TestCheckCipherKeyMatch(c *C) {
	testCases := []struct {
		CipherType encryptionpb.EncryptionMethod
		CipherKey  string
		ok         bool
	}{
		{
			CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
			ok:         true,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_UNKNOWN,
			ok:         false,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES128_CTR,
			CipherKey:  "0123456789abcdef0123456789abcdef",
			ok:         true,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES128_CTR,
			CipherKey:  "0123456789abcdef0123456789abcd",
			ok:         false,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES192_CTR,
			CipherKey:  "0123456789abcdef0123456789abcdef0123456789abcdef",
			ok:         true,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES192_CTR,
			CipherKey:  "0123456789abcdef0123456789abcdef0123456789abcdefff",
			ok:         false,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
			CipherKey:  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			ok:         true,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
			CipherKey:  "",
			ok:         false,
		},
	}

	for _, t := range testCases {
		cipherKey, err := hex.DecodeString(t.CipherKey)
		c.Assert(err, IsNil)

		r := checkCipherKeyMatch(&backuppb.CipherInfo{
			CipherType: t.CipherType,
			CipherKey:  cipherKey,
		})
		c.Assert(r, Equals, t.ok)
	}
}

func (s *testCommonSuite) TestCheckCipherKey(c *C) {
	cases := []struct {
		cipherKey string
		keyFile   string
		ok        bool
	}{
		{
			cipherKey: "0123456789abcdef0123456789abcdef",
			keyFile:   "",
			ok:        true,
		},
		{
			cipherKey: "0123456789abcdef0123456789abcdef",
			keyFile:   "/tmp/abc",
			ok:        false,
		},
		{
			cipherKey: "",
			keyFile:   "/tmp/abc",
			ok:        true,
		},
		{
			cipherKey: "",
			keyFile:   "",
			ok:        false,
		},
	}

	for _, t := range cases {
		err := checkCipherKey(t.cipherKey, t.keyFile)
		if t.ok {
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, NotNil)
		}
	}
}
