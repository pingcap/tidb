// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package version

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	oldReleaseVersion := build.ReleaseVersion
	defer func() {
		build.ReleaseVersion = oldReleaseVersion
	}()

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
		require.Regexp(t, `^incompatible.*version v4.0.0-rc.1, try update it to 4.0.0`, err.Error())
	}

	{
		build.ReleaseVersion = "v3.0.14"
		mock.getAllStores = func() []*metapb.Store {
			return tiflash("v3.1.0-beta.1")
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.Error(t, err)
		require.Regexp(t, `^incompatible.*version v3.1.0-beta.1, try update it to 3.1.0`, err.Error())
	}

	{
		build.ReleaseVersion = "v3.1.1"
		mock.getAllStores = func() []*metapb.Store {
			return tiflash("v3.0.15")
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.Error(t, err)
		require.Regexp(t, `^incompatible.*version v3.0.15, try update it to 3.1.0`, err.Error())
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
		require.Regexp(t, "TiKV .* don't support BR, please upgrade cluster ", err.Error())
	}

	{
		build.ReleaseVersion = "v3.1.0"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v3.1.0-beta.2 is incompatible with BR v3.1.0
			return []*metapb.Store{{Version: minTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.Error(t, err)
		require.Regexp(t, "^TiKV .* mismatch, please ", err.Error())
	}

	{
		build.ReleaseVersion = "v3.1.0"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc major version mismatch with BR v3.1.0
			return []*metapb.Store{{Version: "v4.0.0-rc"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.Error(t, err)
		require.Regexp(t, "^TiKV .* major version mismatch, please ", err.Error())
	}

	{
		build.ReleaseVersion = "v4.0.0-rc.2"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc.2 is incompatible with BR v4.0.0-beta.1
			return []*metapb.Store{{Version: "v4.0.0-beta.1"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.Error(t, err)
		require.Regexp(t, "^TiKV .* mismatch, please ", err.Error())
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
		// Restore across one major version allowed.
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: "v4.0.0-rc.1"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBackup(semver.New("5.0.0-rc")))
		require.NoError(t, err)
	}

	{
		// Restore across two major versions isn't allowed.
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: "v4.0.0-rc.1"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBackup(semver.New("6.0.0")))
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

	{
		build.ReleaseVersion = "v6.0.0"
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: "v5.4.0"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.NoError(t, err)
	}

	{
		build.ReleaseVersion = "v6.0.0"
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: "v4.4.0"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.Error(t, err)
	}

}

func TestCompareVersion(t *testing.T) {
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
	oldReleaseVersion := build.ReleaseVersion
	defer func() {
		build.ReleaseVersion = oldReleaseVersion
	}()

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

	build.ReleaseVersion = "b7ed87d-dirty"
	_ = NextMajorVersion()
	//^ doesn't matter what is returned, just need to ensure it doesn't crash.
}

func TestExtractTiDBVersion(t *testing.T) {
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
	require.Regexp(t, "^not a valid TiDB version", err.Error())

	_, err = ExtractTiDBVersion("8.0.12")
	require.Error(t, err)
	require.Regexp(t, "^not a valid TiDB version", err.Error())

	_, err = ExtractTiDBVersion("not-a-valid-version")
	require.Error(t, err)
}

func TestCheckVersion(t *testing.T) {
	err := CheckVersion("TiNB", *semver.New("2.3.5"), *semver.New("2.1.0"), *semver.New("3.0.0"))
	require.NoError(t, err)

	err = CheckVersion("TiNB", *semver.New("2.1.0"), *semver.New("2.3.5"), *semver.New("3.0.0"))
	require.Error(t, err)
	require.Regexp(t, "^TiNB version too old", err.Error())

	err = CheckVersion("TiNB", *semver.New("3.1.0"), *semver.New("2.3.5"), *semver.New("3.0.0"))
	require.Error(t, err)
	require.Regexp(t, "^TiNB version too new", err.Error())

	err = CheckVersion("TiNB", *semver.New("3.0.0-beta"), *semver.New("2.3.5"), *semver.New("3.0.0"))
	require.Error(t, err)
	require.Regexp(t, "^TiNB version too new", err.Error())
}

func versionEqualCheck(source *semver.Version, target *semver.Version) (result bool) {

	if source == nil || target == nil {
		return target == source
	}

	return source.Equal(*target)
}

func TestNormalizeBackupVersion(t *testing.T) {
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
		require.Truef(t, result, "source=%v, target=%v", source, target)
	}
}

func TestDetectServerInfo(t *testing.T) {
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
		{7, "Release Version: v5.2.1\nEdition: Community\nGit Commit Hash: cd8fb24c5f7ebd9d479ed228bb41848bd5e97445", ServerTypeTiDB, mkVer(5, 2, 1, "")},
		{8, "Release Version: v5.4.0-alpha-21-g86caab907\nEdition: Community\nGit Commit Hash: 86caab907c481bbc4243b5a3346ec13907cc8721\nGit Branch: master", ServerTypeTiDB, mkVer(5, 4, 0, "alpha-21-g86caab907")},
	}
	dec := func(d []interface{}) (tag int, verStr string, tp ServerType, v *semver.Version) {
		return d[0].(int), d[1].(string), ServerType(d[2].(int)), d[3].(*semver.Version)
	}

	for _, datum := range data {
		tag, r, serverTp, expectVer := dec(datum)
		cmt := fmt.Sprintf("test case number: %d", tag)

		tidbVersionQuery := mock.ExpectQuery("SELECT tidb_version\\(\\);")
		if strings.HasPrefix(r, "Release Version:") {
			tidbVersionQuery.WillReturnRows(sqlmock.NewRows([]string{"tidb_version"}).AddRow(r))
		} else {
			tidbVersionQuery.WillReturnError(errors.New("mock error"))
			rows := sqlmock.NewRows([]string{"version"}).AddRow(r)
			mock.ExpectQuery("SELECT version\\(\\);").WillReturnRows(rows)
		}

		verStr, err := FetchVersion(context.Background(), db)
		require.NoError(t, err, cmt)

		info := ParseServerInfo(verStr)
		require.Equal(t, serverTp, info.ServerType, cmt)
		require.Equal(t, expectVer == nil, info.ServerVersion == nil, cmt)
		if info.ServerVersion == nil {
			require.Nil(t, expectVer, cmt)
		} else {
			fmt.Printf("%v, %v\n", *info.ServerVersion, *expectVer)
			require.True(t, info.ServerVersion.Equal(*expectVer))
		}
		require.NoError(t, mock.ExpectationsWereMet(), cmt)
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

func TestFetchVersion(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	tidbVersion := `Release Version: v5.2.1
Edition: Community
Git Commit Hash: cd8fb24c5f7ebd9d479ed228bb41848bd5e97445
Git Branch: heads/refs/tags/v5.2.1
UTC Build Time: 2021-09-08 02:32:56
GoVersion: go1.16.4
Race Enabled: false
TiKV Min Version: v3.0.0-60965b006877ca7234adaced7890d7b029ed1306
Check Table Before Drop: false`

	ctx := context.Background()
	mock.ExpectQuery("SELECT tidb_version\\(\\);").WillReturnRows(sqlmock.
		NewRows([]string{""}).AddRow(tidbVersion))
	versionStr, err := FetchVersion(ctx, db)
	require.NoError(t, err)
	require.Equal(t, tidbVersion, versionStr)

	mock.ExpectQuery("SELECT tidb_version\\(\\);").WillReturnError(errors.New("mock failure"))
	mock.ExpectQuery("SELECT version\\(\\);").WillReturnRows(sqlmock.
		NewRows([]string{""}).AddRow("5.7.25"))
	versionStr, err = FetchVersion(ctx, db)
	require.NoError(t, err)
	require.Equal(t, "5.7.25", versionStr)

	mock.ExpectQuery("SELECT tidb_version\\(\\);").WillReturnError(errors.New("mock failure"))
	mock.ExpectQuery("SELECT version\\(\\);").WillReturnError(errors.New("mock failure"))

	_, err = FetchVersion(ctx, db)
	require.Error(t, err)
	require.Regexp(t, "mock failure$", err.Error())

}
