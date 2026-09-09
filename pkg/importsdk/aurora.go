// Copyright 2026 PingCAP, Inc.
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
	"context"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
)

var (
	auroraDataPattern = regexp.MustCompile(`^(?:(.*)/)?([^/]+)/([^/]+\.[^/]+)/(?:[0-9]+/)?(?i:part-[^/]+\.parquet)$`)
	dataFileSuffix    = regexp.MustCompile(`(?i)\.(sql|csv|parquet)(\.[^./]+)?$`)
)

// auroraSource validates raw paths during the loader's existing listing, before
// filters can hide mixed data or another export. Explicit file rules bypass it.
type auroraSource struct {
	store    storeapi.Storage
	fallback mydump.FileRouter
	limit    *int
	found    bool
}

func (s *auroraSource) IterateFiles(ctx context.Context, handle mydump.FileHandler) error {
	var root, unexpected string
	s.found = false
	count := 0
	err := s.store.WalkDir(ctx, &storeapi.WalkOption{}, func(path string, size int64) error {
		count++
		if s.limit != nil && *s.limit > 0 && count > *s.limit {
			return common.ErrTooManySourceFiles
		}
		_, fileRoot, candidate, err := parseAuroraFile(path)
		if err != nil {
			return err
		}
		if candidate {
			if s.found && root != fileRoot {
				return errors.New("multiple Aurora export roots; scope the source URL to one export")
			}
			root, s.found = fileRoot, true
		} else if dataFileSuffix.MatchString(path) {
			res, err := s.fallback.Route(filepath.ToSlash(path))
			if err != nil {
				return err
			}
			if res == nil || res.Type == mydump.SourceTypeSQL || res.Type == mydump.SourceTypeCSV || res.Type == mydump.SourceTypeParquet {
				unexpected = path
			}
		}
		return handle(ctx, path, size)
	})
	if err != nil {
		return errors.Annotate(err, "incomplete automatic source scan")
	}
	if s.found && unexpected != "" {
		return errors.Errorf("mixed or unmatched data in Aurora source: %s", unexpected)
	}
	return nil
}

// Native keys are raw, not URL-encoded. Remove the exact database prefix so
// dotted database/table names and literal percent sequences remain unchanged.
func parseAuroraFile(path string) (table filter.Table, root string, candidate bool, err error) {
	path = filepath.ToSlash(path)
	parts := auroraDataPattern.FindStringSubmatch(path)
	if parts == nil {
		dir, leaf := filepath.Split(path)
		if strings.HasPrefix(strings.ToLower(leaf), "part-") && dataFileSuffix.MatchString(leaf) && strings.Contains(dir, ".") {
			return table, "", true, errors.Errorf("unsupported or inconsistent Aurora directory: %s", path)
		}
		return table, "", false, nil
	}
	table.Schema = parts[2]
	table.Name, candidate = strings.CutPrefix(parts[3], table.Schema+".")
	if !candidate || table.Name == "" {
		return table, "", true, errors.Errorf("inconsistent Aurora database/table directory: %s", path)
	}
	// AWS's conversion to underscores is lossy; do not guess the original name.
	// Wildcard metacharacters cannot be represented safely in an import pattern.
	if strings.ContainsAny(table.Schema+table.Name, "_\\`\" *?[]") {
		return table, "", true, errors.Errorf("ambiguous Aurora identifier in %s; provide an explicit file route with the original name", path)
	}
	return table, parts[1], true, nil
}

func (s *auroraSource) Route(path string) (*mydump.RouteResult, error) {
	table, _, candidate, err := parseAuroraFile(path)
	if err != nil {
		return nil, err
	}
	if candidate {
		return &mydump.RouteResult{Table: table, Type: mydump.SourceTypeParquet}, nil
	}
	return s.fallback.Route(path)
}
