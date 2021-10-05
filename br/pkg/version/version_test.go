// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package version

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/version/build"
	pd "github.com/tikv/pd/client"
)

type checkSuite struct{}

var _ = Suite(&checkSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type mockPDClient struct {
	pd.Client
	getAllStores func() []*metapb.Store
}

func (m *mockPDClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	if m.getAllStores != nil {
		return m.getAllStores(), nil
	}
	return []*metapb.Store{}, nil
}

func tiflash(version string) []*metapb.Store {
	return []*metapb.Store{
		{Version: version, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}},
	}
}

func (s *checkSuite) TestCheckClusterVersion(c *C) {
	mock := mockPDClient{
		Client: nil,
	}

	{
		build.ReleaseVersion = "v4.0.5"
		mock.getAllStores = func() []*metapb.Store {
			return tiflash("v4.0.0-rc.1")
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		c.Assert(err, ErrorMatches, `incompatible.*version v4.0.0-rc.1, try update it to 4.0.0.*`)
	}

	{
		build.ReleaseVersion = "v3.0.14"
		mock.getAllStores = func() []*metapb.Store {
			return tiflash("v3.1.0-beta.1")
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		c.Assert(err, ErrorMatches, `incompatible.*version v3.1.0-beta.1, try update it to 3.1.0.*`)
	}

	{
		build.ReleaseVersion = "v3.1.1"
		mock.getAllStores = func() []*metapb.Store {
			return tiflash("v3.0.15")
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		c.Assert(err, ErrorMatches, `incompatible.*version v3.0.15, try update it to 3.1.0.*`)
	}

	{
		build.ReleaseVersion = "v3.1.0-beta.2"
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: minTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		c.Assert(err, IsNil)
	}

	{
		build.ReleaseVersion = "v3.1.0-beta.2"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV is too lower to support BR
			return []*metapb.Store{{Version: `v2.1.0`}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		c.Assert(err, ErrorMatches, ".*TiKV .* don't support BR, please upgrade cluster .*")
	}

	{
		build.ReleaseVersion = "v3.1.0"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v3.1.0-beta.2 is incompatible with BR v3.1.0
			return []*metapb.Store{{Version: minTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		c.Assert(err, ErrorMatches, "TiKV .* mismatch, please .*")
	}

	{
		build.ReleaseVersion = "v3.1.0"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc major version mismatch with BR v3.1.0
			return []*metapb.Store{{Version: "v4.0.0-rc"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		c.Assert(err, ErrorMatches, "TiKV .* major version mismatch, please .*")
	}

	{
		build.ReleaseVersion = "v4.0.0-rc.2"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc.2 is incompatible with BR v4.0.0-beta.1
			return []*metapb.Store{{Version: "v4.0.0-beta.1"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		c.Assert(err, ErrorMatches, "TiKV .* mismatch, please .*")
	}

	{
		build.ReleaseVersion = "v4.0.0-rc.2"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc.1 with BR v4.0.0-rc.2 is ok
			return []*metapb.Store{{Version: "v4.0.0-rc.1"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		c.Assert(err, IsNil)
	}

	{
		// Even across many patch versions, backup should be usable.
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: "v4.0.0-rc.1"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBackup(semver.New("4.0.12")))
		c.Assert(err, IsNil)
	}

	{
		// Restore across major version isn't allowed.
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: "v4.0.0-rc.1"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBackup(semver.New("5.0.0-rc")))
		c.Assert(err, Not(IsNil))
	}

	{
		build.ReleaseVersion = "v4.0.0-rc.1"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc.2 with BR v4.0.0-rc.1 is ok
			return []*metapb.Store{{Version: "v4.0.0-rc.2"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		c.Assert(err, IsNil)
	}
}

func (s *checkSuite) TestCompareVersion(c *C) {
	c.Assert(semver.New("4.0.0-rc").Compare(*semver.New("4.0.0-rc.2")), Equals, -1)
	c.Assert(semver.New("4.0.0-beta.3").Compare(*semver.New("4.0.0-rc.2")), Equals, -1)
	c.Assert(semver.New("4.0.0-rc.1").Compare(*semver.New("4.0.0")), Equals, -1)
	c.Assert(semver.New("4.0.0-beta.1").Compare(*semver.New("4.0.0")), Equals, -1)
	c.Assert(semver.New(removeVAndHash("4.0.0-rc-35-g31dae220")).Compare(*semver.New("4.0.0-rc.2")), Equals, -1)
	c.Assert(semver.New(removeVAndHash("4.0.0-9-g30f0b014")).Compare(*semver.New("4.0.0-rc.1")), Equals, 1)
	c.Assert(semver.New(removeVAndHash("v3.0.0-beta-211-g09beefbe0-dirty")).
		Compare(*semver.New("3.0.0-beta")), Equals, 0)
	c.Assert(semver.New(removeVAndHash("v3.0.5-dirty")).
		Compare(*semver.New("3.0.5")), Equals, 0)
	c.Assert(semver.New(removeVAndHash("v3.0.5-beta.12-dirty")).
		Compare(*semver.New("3.0.5-beta.12")), Equals, 0)
	c.Assert(semver.New(removeVAndHash("v2.1.0-rc.1-7-g38c939f-dirty")).
		Compare(*semver.New("2.1.0-rc.1")), Equals, 0)
}

func (s *checkSuite) TestNextMajorVersion(c *C) {
	build.ReleaseVersion = "v4.0.0-rc.1"
	c.Assert(NextMajorVersion().String(), Equals, "5.0.0")
	build.ReleaseVersion = "4.0.0-rc-35-g31dae220"
	c.Assert(NextMajorVersion().String(), Equals, "5.0.0")
	build.ReleaseVersion = "4.0.0-9-g30f0b014"
	c.Assert(NextMajorVersion().String(), Equals, "5.0.0")

	build.ReleaseVersion = "v5.0.0-rc.2"
	c.Assert(NextMajorVersion().String(), Equals, "6.0.0")
	build.ReleaseVersion = "v5.0.0-master"
	c.Assert(NextMajorVersion().String(), Equals, "6.0.0")
}

func (s *checkSuite) TestExtractTiDBVersion(c *C) {
	vers, err := ExtractTiDBVersion("5.7.10-TiDB-v2.1.0-rc.1-7-g38c939f")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.1.0-rc.1"))

	vers, err = ExtractTiDBVersion("5.7.10-TiDB-v2.0.4-1-g06a0bf5")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.0.4"))

	vers, err = ExtractTiDBVersion("5.7.10-TiDB-v2.0.7")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.0.7"))

	vers, err = ExtractTiDBVersion("8.0.12-TiDB-v3.0.5-beta.12")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.5-beta.12"))

	vers, err = ExtractTiDBVersion("5.7.25-TiDB-v3.0.0-beta-211-g09beefbe0-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.0-beta"))

	vers, err = ExtractTiDBVersion("8.0.12-TiDB-v3.0.5-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.5"))

	vers, err = ExtractTiDBVersion("8.0.12-TiDB-v3.0.5-beta.12-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.5-beta.12"))

	vers, err = ExtractTiDBVersion("5.7.10-TiDB-v2.1.0-rc.1-7-g38c939f-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.1.0-rc.1"))

	_, err = ExtractTiDBVersion("")
	c.Assert(err, ErrorMatches, "not a valid TiDB version.*")

	_, err = ExtractTiDBVersion("8.0.12")
	c.Assert(err, ErrorMatches, "not a valid TiDB version.*")

	_, err = ExtractTiDBVersion("not-a-valid-version")
	c.Assert(err, NotNil)
}

func (s *checkSuite) TestCheckVersion(c *C) {
	err := CheckVersion("TiNB", *semver.New("2.3.5"), *semver.New("2.1.0"), *semver.New("3.0.0"))
	c.Assert(err, IsNil)

	err = CheckVersion("TiNB", *semver.New("2.1.0"), *semver.New("2.3.5"), *semver.New("3.0.0"))
	c.Assert(err, ErrorMatches, "TiNB version too old.*")

	err = CheckVersion("TiNB", *semver.New("3.1.0"), *semver.New("2.3.5"), *semver.New("3.0.0"))
	c.Assert(err, ErrorMatches, "TiNB version too new.*")

	err = CheckVersion("TiNB", *semver.New("3.0.0-beta"), *semver.New("2.3.5"), *semver.New("3.0.0"))
	c.Assert(err, ErrorMatches, "TiNB version too new.*")
}

type versionEqualsC struct{}

func (v versionEqualsC) Info() *CheckerInfo {
	return &CheckerInfo{
		Name:   "VersionEquals",
		Params: []string{"source", "target"},
	}
}

func (v versionEqualsC) Check(params []interface{}, names []string) (result bool, error string) {
	source := params[0].(*semver.Version)
	target := params[1].(*semver.Version)

	if source == nil || target == nil {
		if target == source {
			return true, ""
		}
		return false, fmt.Sprintf("one of version is nil but another is not (%s and %s)", params[0], params[1])
	}

	if source.Equal(*target) {
		return true, ""
	}
	return false, fmt.Sprintf("version not equal (%s vs %s)", source, target)
}

var versionEquals versionEqualsC

func (s *checkSuite) TestNormalizeBackupVersion(c *C) {
	cases := []struct {
		target string
		source string
	}{
		{"4.0.0", `"4.0.0\n"`},
		{"5.0.0-rc.x", `"5.0.0-rc.x\n"`},
		{"5.0.0-rc.x", `5.0.0-rc.x`},
		{"4.0.12", `"4.0.12"` + "\n"},
		{"<error-version>", ""},
	}

	for _, testCase := range cases {
		target, _ := semver.NewVersion(testCase.target)
		source := NormalizeBackupVersion(testCase.source)
		c.Assert(source, versionEquals, target)
	}
}

func (s *checkSuite) TestDetectServerInfo(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	mkVer := makeVersion
	data := [][]interface{}{
		{1, "8.0.18", ServerTypeMySQL, mkVer(8, 0, 18, "")},
		{2, "10.4.10-MariaDB-1:10.4.10+maria~bionic", ServerTypeMariaDB, mkVer(10, 4, 10, "MariaDB-1")},
		{3, "5.7.25-TiDB-v4.0.0-alpha-1263-g635f2e1af", ServerTypeTiDB, mkVer(4, 0, 0, "alpha-1263-g635f2e1af")},
		{4, "5.7.25-TiDB-v3.0.7-58-g6adce2367", ServerTypeTiDB, mkVer(3, 0, 7, "58-g6adce2367")},
		{5, "5.7.25-TiDB-3.0.6", ServerTypeTiDB, mkVer(3, 0, 6, "")},
		{6, "invalid version", ServerTypeUnknown, (*semver.Version)(nil)},
	}
	dec := func(d []interface{}) (tag int, verStr string, tp ServerType, v *semver.Version) {
		return d[0].(int), d[1].(string), ServerType(d[2].(int)), d[3].(*semver.Version)
	}

	for _, datum := range data {
		tag, r, serverTp, expectVer := dec(datum)
		cmt := Commentf("test case number: %d", tag)

		rows := sqlmock.NewRows([]string{"version"}).AddRow(r)
		mock.ExpectQuery("SELECT version()").WillReturnRows(rows)

		verStr, err := FetchVersion(context.Background(), db)
		c.Assert(err, IsNil, cmt)
		info := ParseServerInfo(verStr)
		c.Assert(info.ServerType, Equals, serverTp, cmt)
		c.Assert(info.ServerVersion == nil, Equals, expectVer == nil, cmt)
		if info.ServerVersion == nil {
			c.Assert(expectVer, IsNil, cmt)
		} else {
			c.Assert(info.ServerVersion.Equal(*expectVer), IsTrue)
		}
		c.Assert(mock.ExpectationsWereMet(), IsNil, cmt)
	}
}
func makeVersion(major, minor, patch int64, preRelease string) *semver.Version {
	return &semver.Version{
		Major:      major,
		Minor:      minor,
		Patch:      patch,
		PreRelease: semver.PreRelease(preRelease),
		Metadata:   "",
	}
}
