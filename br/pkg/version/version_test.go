// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package version

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

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

func TestCheckClusterVersion(t *testing.T) {
	t.Parallel()

	mock := mockPDClient{
		Client: nil,
	}

	{
		build.ReleaseVersion = "v4.0.5"
		mock.getAllStores = func() []*metapb.Store {
			return tiflash("v4.0.0-rc.1")
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.Error(t, err)
		require.Regexp(t, `incompatible.*version v4.0.0-rc.1, try update it to 4.0.0.*`, err.Error())
	}

	{
		build.ReleaseVersion = "v3.0.14"
		mock.getAllStores = func() []*metapb.Store {
			return tiflash("v3.1.0-beta.1")
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.Error(t, err)
		require.Regexp(t, `incompatible.*version v3.1.0-beta.1, try update it to 3.1.0.*`, err.Error())
	}

	{
		build.ReleaseVersion = "v3.1.1"
		mock.getAllStores = func() []*metapb.Store {
			return tiflash("v3.0.15")
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.Error(t, err)
		require.Regexp(t, `incompatible.*version v3.0.15, try update it to 3.1.0.*`, err.Error())
	}

	{
		build.ReleaseVersion = "v3.1.0-beta.2"
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: minTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.NoError(t, err)
	}

	{
		build.ReleaseVersion = "v3.1.0-beta.2"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV is too lower to support BR
			return []*metapb.Store{{Version: `v2.1.0`}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.Error(t, err)
		require.Regexp(t, ".*TiKV .* don't support BR, please upgrade cluster .*", err.Error())
	}

	{
		build.ReleaseVersion = "v3.1.0"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v3.1.0-beta.2 is incompatible with BR v3.1.0
			return []*metapb.Store{{Version: minTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.Error(t, err)
		require.Regexp(t, "TiKV .* mismatch, please .*", err.Error())
	}

	{
		build.ReleaseVersion = "v3.1.0"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc major version mismatch with BR v3.1.0
			return []*metapb.Store{{Version: "v4.0.0-rc"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.Error(t, err)
		require.Regexp(t, "TiKV .* major version mismatch, please .*", err.Error())
	}

	{
		build.ReleaseVersion = "v4.0.0-rc.2"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc.2 is incompatible with BR v4.0.0-beta.1
			return []*metapb.Store{{Version: "v4.0.0-beta.1"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.Error(t, err)
		require.Regexp(t, "TiKV .* mismatch, please .*", err.Error())
	}

	{
		build.ReleaseVersion = "v4.0.0-rc.2"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc.1 with BR v4.0.0-rc.2 is ok
			return []*metapb.Store{{Version: "v4.0.0-rc.1"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.NoError(t, err)
	}

	{
		// Even across many patch versions, backup should be usable.
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: "v4.0.0-rc.1"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBackup(semver.New("4.0.12")))
		require.NoError(t, err)
	}

	{
		// Restore across major version isn't allowed.
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: "v4.0.0-rc.1"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBackup(semver.New("5.0.0-rc")))
		require.Error(t, err)
	}

	{
		build.ReleaseVersion = "v4.0.0-rc.1"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc.2 with BR v4.0.0-rc.1 is ok
			return []*metapb.Store{{Version: "v4.0.0-rc.2"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.NoError(t, err)
	}
}

func TestCompareVersion(t *testing.T) {
	t.Parallel()

	require.Equal(t, -1, semver.New("4.0.0-rc").Compare(*semver.New("4.0.0-rc.2")))
	require.Equal(t, -1, semver.New("4.0.0-beta.3").Compare(*semver.New("4.0.0-rc.2")))
	require.Equal(t, -1, semver.New("4.0.0-rc.1").Compare(*semver.New("4.0.0")))
	require.Equal(t, -1, semver.New("4.0.0-beta.1").Compare(*semver.New("4.0.0")))
	require.Equal(t, -1, semver.New(removeVAndHash("4.0.0-rc-35-g31dae220")).Compare(*semver.New("4.0.0-rc.2")))
	require.Equal(t, 1, semver.New(removeVAndHash("4.0.0-9-g30f0b014")).Compare(*semver.New("4.0.0-rc.1")))
	require.Equal(t, 0, semver.New(removeVAndHash("v3.0.0-beta-211-g09beefbe0-dirty")).
		Compare(*semver.New("3.0.0-beta")))
	require.Equal(t, 0, semver.New(removeVAndHash("v3.0.5-dirty")).
		Compare(*semver.New("3.0.5")))
	require.Equal(t, 0, semver.New(removeVAndHash("v3.0.5-beta.12-dirty")).
		Compare(*semver.New("3.0.5-beta.12")))
	require.Equal(t, 0, semver.New(removeVAndHash("v2.1.0-rc.1-7-g38c939f-dirty")).
		Compare(*semver.New("2.1.0-rc.1")))
}

func TestNextMajorVersion(t *testing.T) {
	t.Parallel()

	build.ReleaseVersion = "v4.0.0-rc.1"
	require.Equal(t, "5.0.0", NextMajorVersion().String())
	build.ReleaseVersion = "4.0.0-rc-35-g31dae220"
	require.Equal(t, "5.0.0", NextMajorVersion().String())
	build.ReleaseVersion = "4.0.0-9-g30f0b014"
	require.Equal(t, "5.0.0", NextMajorVersion().String())

	build.ReleaseVersion = "v5.0.0-rc.2"
	require.Equal(t, "6.0.0", NextMajorVersion().String())
	build.ReleaseVersion = "v5.0.0-master"
	require.Equal(t, "6.0.0", NextMajorVersion().String())
}

func TestExtractTiDBVersion(t *testing.T) {
	t.Parallel()

	vers, err := ExtractTiDBVersion("5.7.10-TiDB-v2.1.0-rc.1-7-g38c939f")
	require.NoError(t, err)
	require.Equal(t, *semver.New("2.1.0-rc.1"), *vers)

	vers, err = ExtractTiDBVersion("5.7.10-TiDB-v2.0.4-1-g06a0bf5")
	require.NoError(t, err)
	require.Equal(t, *semver.New("2.0.4"), *vers)

	vers, err = ExtractTiDBVersion("5.7.10-TiDB-v2.0.7")
	require.NoError(t, err)
	require.Equal(t, *semver.New("2.0.7"), *vers)

	vers, err = ExtractTiDBVersion("8.0.12-TiDB-v3.0.5-beta.12")
	require.NoError(t, err)
	require.Equal(t, *semver.New("3.0.5-beta.12"), *vers)

	vers, err = ExtractTiDBVersion("5.7.25-TiDB-v3.0.0-beta-211-g09beefbe0-dirty")
	require.NoError(t, err)
	require.Equal(t, *semver.New("3.0.0-beta"), *vers)

	vers, err = ExtractTiDBVersion("8.0.12-TiDB-v3.0.5-dirty")
	require.NoError(t, err)
	require.Equal(t, *semver.New("3.0.5"), *vers)

	vers, err = ExtractTiDBVersion("8.0.12-TiDB-v3.0.5-beta.12-dirty")
	require.NoError(t, err)
	require.Equal(t, *semver.New("3.0.5-beta.12"), *vers)

	vers, err = ExtractTiDBVersion("5.7.10-TiDB-v2.1.0-rc.1-7-g38c939f-dirty")
	require.NoError(t, err)
	require.Equal(t, *semver.New("2.1.0-rc.1"), *vers)

	_, err = ExtractTiDBVersion("")
	require.Error(t, err)
	require.Regexp(t, "not a valid TiDB version.*", err.Error())

	_, err = ExtractTiDBVersion("8.0.12")
	require.Error(t, err)
	require.Regexp(t, "not a valid TiDB version.*", err.Error())

	_, err = ExtractTiDBVersion("not-a-valid-version")
	require.Error(t, err)
}

func TestCheckVersion(t *testing.T) {
	t.Parallel()

	err := CheckVersion("TiDB", *semver.New("2.3.5"), *semver.New("2.1.0"), *semver.New("3.0.0"))
	require.NoError(t, err)

	err = CheckVersion("TiDB", *semver.New("2.1.0"), *semver.New("2.3.5"), *semver.New("3.0.0"))
	require.Error(t, err)
	require.Regexp(t, "TiDB version too old.*", err.Error())

	err = CheckVersion("TiDB", *semver.New("3.1.0"), *semver.New("2.3.5"), *semver.New("3.0.0"))
	require.Error(t, err)
	require.Regexp(t, "TiDB version too new.*", err.Error())

	err = CheckVersion("TiDB", *semver.New("3.0.0-beta"), *semver.New("2.3.5"), *semver.New("3.0.0"))
	require.Error(t, err)
	require.Regexp(t, "TiDB version too new.*", err.Error())
}

func versionEqualCheck(source *semver.Version, target *semver.Version) (result bool) {

	if source == nil || target == nil {
		if target == source {
			return true
		}
		return false
	}

	if source.Equal(*target) {
		return true
	}
	return false
}

func TestNormalizeBackupVersion(t *testing.T) {
	t.Parallel()

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
		result := versionEqualCheck(source, target)
		require.Equal(t, true, result)
	}
}

func TestDetectServerInfo(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
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
		cmt := fmt.Sprintf("test case number: %d", tag)

		rows := sqlmock.NewRows([]string{"version"}).AddRow(r)
		mock.ExpectQuery("SELECT version()").WillReturnRows(rows)

		verStr, err := FetchVersion(context.Background(), db)
		require.NoError(t, err, cmt)

		info := ParseServerInfo(verStr)
		require.Equal(t, serverTp, info.ServerType, cmt)
		require.Equal(t, expectVer == nil, info.ServerVersion == nil, cmt)
		if info.ServerVersion == nil {
			require.Nil(t, expectVer, cmt)
		} else {
			require.True(t, info.ServerVersion.Equal(*expectVer))
		}
		require.Nil(t, mock.ExpectationsWereMet(), cmt)
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
