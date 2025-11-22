// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importsdk

import (
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
)

func TestLongestCommonPrefix(t *testing.T) {
	strs := []string{"s3://bucket/foo/bar/baz1", "s3://bucket/foo/bar/baz2", "s3://bucket/foo/bar/baz"}
	p := longestCommonPrefix(strs)
	require.Equal(t, "s3://bucket/foo/bar/baz", p)

	// no common prefix
	require.Equal(t, "", longestCommonPrefix([]string{"a", "b"}))

	// empty inputs
	require.Equal(t, "", longestCommonPrefix(nil))
	require.Equal(t, "", longestCommonPrefix([]string{}))
}

func TestLongestCommonSuffix(t *testing.T) {
	strs := []string{"abcXYZ", "defXYZ", "XYZ"}
	s := longestCommonSuffix(strs, 0)
	require.Equal(t, "XYZ", s)

	// no common suffix
	require.Equal(t, "", longestCommonSuffix([]string{"a", "b"}, 0))

	// empty inputs
	require.Equal(t, "", longestCommonSuffix(nil, 0))
	require.Equal(t, "", longestCommonSuffix([]string{}, 0))

	// same prefix
	require.Equal(t, "", longestCommonSuffix([]string{"abc", "abc"}, 3))
	require.Equal(t, "f", longestCommonSuffix([]string{"abcdf", "abcef"}, 3))
}

func TestGeneratePrefixSuffixPattern(t *testing.T) {
	paths := []string{"pre_middle_suf", "pre_most_suf"}
	pattern := generatePrefixSuffixPattern(paths)
	// common prefix "pre_m", suffix "_suf"
	require.Equal(t, "pre_m*_suf", pattern)

	// empty inputs
	require.Equal(t, "", generatePrefixSuffixPattern(nil))
	require.Equal(t, "", generatePrefixSuffixPattern([]string{}))

	// only one file
	require.Equal(t, "pre_middle_suf", generatePrefixSuffixPattern([]string{"pre_middle_suf"}))

	// no common prefix/suffix
	paths2 := []string{"foo", "bar"}
	require.Equal(t, "*", generatePrefixSuffixPattern(paths2))

	// overlapping prefix/suffix
	paths3 := []string{"aaabaaa", "aaa"}
	require.Equal(t, "aaa*", generatePrefixSuffixPattern(paths3))
}

func generateFileMetas(t *testing.T, paths []string) []mydump.FileInfo {
	t.Helper()

	files := make([]mydump.FileInfo, 0, len(paths))
	fileRouter, err := mydump.NewDefaultFileRouter(log.L())
	require.NoError(t, err)
	for _, p := range paths {
		res, err := fileRouter.Route(p)
		require.NoError(t, err)
		files = append(files, mydump.FileInfo{
			TableName: res.Table,
			FileMeta: mydump.SourceFileMeta{
				Path:        p,
				Type:        res.Type,
				Compression: res.Compression,
				SortKey:     res.Key,
			},
		})
	}
	return files
}

func TestGenerateMydumperPattern(t *testing.T) {
	paths := []string{"db.tb.0001.sql", "db.tb.0002.sql"}
	p := generateMydumperPattern(generateFileMetas(t, paths)[0])
	require.Equal(t, "db.tb.*.sql", p)

	paths2 := []string{"s3://bucket/dir/db.tb.0001.sql", "s3://bucket/dir/db.tb.0002.sql"}
	p2 := generateMydumperPattern(generateFileMetas(t, paths2)[0])
	require.Equal(t, "s3://bucket/dir/db.tb.*.sql", p2)

	// not mydumper pattern
	require.Equal(t, "", generateMydumperPattern(mydump.FileInfo{
		TableName: filter.Table{},
	}))
}

func TestValidatePattern(t *testing.T) {
	tableFiles := map[string]struct{}{
		"a.txt": {}, "b.txt": {},
	}
	// only table files in allFiles
	smallAll := map[string]mydump.FileInfo{
		"a.txt": {}, "b.txt": {},
	}
	require.True(t, isValidPattern("*.txt", tableFiles, smallAll))

	// allFiles includes an extra file => invalid
	fullAll := map[string]mydump.FileInfo{
		"a.txt": {}, "b.txt": {}, "c.txt": {},
	}
	require.False(t, isValidPattern("*.txt", tableFiles, fullAll))

	// If pattern doesn't match our table's file, it's also invalid
	require.False(t, isValidPattern("*.csv", tableFiles, smallAll))

	// empty pattern => invalid
	require.False(t, isValidPattern("", tableFiles, smallAll))
}

func TestGenerateWildcardPath(t *testing.T) {
	// Helper to create allFiles map
	createAllFiles := func(paths []string) map[string]mydump.FileInfo {
		allFiles := make(map[string]mydump.FileInfo)
		for _, p := range paths {
			allFiles[p] = mydump.FileInfo{
				FileMeta: mydump.SourceFileMeta{Path: p},
			}
		}
		return allFiles
	}

	// No files
	files1 := []mydump.FileInfo{}
	allFiles1 := createAllFiles([]string{})
	_, err := generateWildcardPath(files1, allFiles1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no data files for table")

	// Single file
	files2 := generateFileMetas(t, []string{"db.tb.0001.sql"})
	allFiles2 := createAllFiles([]string{"db.tb.0001.sql"})
	path2, err := generateWildcardPath(files2, allFiles2)
	require.NoError(t, err)
	require.Equal(t, "db.tb.0001.sql", path2)

	// Mydumper pattern succeeds
	files3 := generateFileMetas(t, []string{"db.tb.0001.sql.gz", "db.tb.0002.sql.gz"})
	allFiles3 := createAllFiles([]string{"db.tb.0001.sql.gz", "db.tb.0002.sql.gz"})
	path3, err := generateWildcardPath(files3, allFiles3)
	require.NoError(t, err)
	require.Equal(t, "db.tb.*.sql.gz", path3)

	// Mydumper pattern fails, fallback to prefix/suffix succeeds
	files4 := []mydump.FileInfo{
		{
			TableName: filter.Table{Schema: "db", Name: "tb"},
			FileMeta: mydump.SourceFileMeta{
				Path:        "a.sql",
				Type:        mydump.SourceTypeSQL,
				Compression: mydump.CompressionNone,
			},
		},
		{
			TableName: filter.Table{Schema: "db", Name: "tb"},
			FileMeta: mydump.SourceFileMeta{
				Path:        "b.sql",
				Type:        mydump.SourceTypeSQL,
				Compression: mydump.CompressionNone,
			},
		},
	}
	allFiles4 := map[string]mydump.FileInfo{
		files4[0].FileMeta.Path: files4[0],
		files4[1].FileMeta.Path: files4[1],
	}
	path4, err := generateWildcardPath(files4, allFiles4)
	require.NoError(t, err)
	require.Equal(t, "*.sql", path4)

	allFiles4["db-schema.sql"] = mydump.FileInfo{
		FileMeta: mydump.SourceFileMeta{
			Path:        "db-schema.sql",
			Type:        mydump.SourceTypeSQL,
			Compression: mydump.CompressionNone,
		},
	}
	_, err = generateWildcardPath(files4, allFiles4)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot generate a unique wildcard pattern")
}
