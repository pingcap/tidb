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
	"path/filepath"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
)

// generateWildcardPath derives a wildcard that matches all and only the provided table data files.
func generateWildcardPath(files []mydump.FileInfo, allFiles map[string]mydump.FileInfo) (string, error) {
	tableFiles := make(map[string]struct{}, len(files))
	for _, df := range files {
		tableFiles[df.FileMeta.Path] = struct{}{}
	}

	if len(files) == 0 {
		return "", errors.Annotate(ErrNoTableDataFiles, "cannot generate wildcard pattern because the table has no data files")
	}

	if len(files) == 1 {
		return files[0].FileMeta.Path, nil
	}

	if p := generateMydumperPattern(files[0]); p != "" && isValidPattern(p, tableFiles, allFiles) {
		return p, nil
	}

	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.FileMeta.Path)
	}
	if p := generatePrefixSuffixPattern(paths); p != "" && isValidPattern(p, tableFiles, allFiles) {
		return p, nil
	}
	return "", errors.Annotatef(ErrWildcardNotSpecific, "failed to find a wildcard that matches all and only the table's files")
}

func isValidPattern(pattern string, tableFiles map[string]struct{}, allFiles map[string]mydump.FileInfo) bool {
	if pattern == "" {
		return false
	}

	for path := range allFiles {
		isMatch, err := filepath.Match(pattern, path)
		if err != nil {
			return false
		}
		_, isTableFile := tableFiles[path]
		if isMatch && !isTableFile {
			return false
		}
		if !isMatch && isTableFile {
			return false
		}
	}
	return true
}

func generateMydumperPattern(file mydump.FileInfo) string {
	dbName, tableName := file.TableName.Schema, file.TableName.Name
	if dbName == "" || tableName == "" {
		return ""
	}

	full := file.FileMeta.Path
	dirPrefix, name := "", full
	if idx := strings.LastIndex(full, "/"); idx >= 0 {
		dirPrefix = full[:idx+1]
		name = full[idx+1:]
	}

	compExt := ""
	if file.FileMeta.Compression != mydump.CompressionNone {
		compExt = filepath.Ext(name)
	}

	base := strings.TrimSuffix(name, compExt)
	dataExt := filepath.Ext(base)
	return dirPrefix + dbName + "." + tableName + ".*" + dataExt + compExt
}

func longestCommonPrefix(strs []string) string {
	if len(strs) == 0 {
		return ""
	}

	prefix := strs[0]
	for _, s := range strs[1:] {
		i := 0
		for i < len(prefix) && i < len(s) && prefix[i] == s[i] {
			i++
		}
		prefix = prefix[:i]
		if prefix == "" {
			break
		}
	}
	return prefix
}

func longestCommonSuffix(strs []string, prefixLen int) string {
	if len(strs) == 0 {
		return ""
	}

	suffix := strs[0][prefixLen:]
	for _, s := range strs[1:] {
		remaining := s[prefixLen:]
		i := 0
		for i < len(suffix) && i < len(remaining) && suffix[len(suffix)-i-1] == remaining[len(remaining)-i-1] {
			i++
		}
		suffix = suffix[len(suffix)-i:]
		if suffix == "" {
			break
		}
	}
	return suffix
}

func generatePrefixSuffixPattern(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	if len(paths) == 1 {
		return paths[0]
	}

	prefix := longestCommonPrefix(paths)
	suffix := longestCommonSuffix(paths, len(prefix))
	return prefix + "*" + suffix
}
