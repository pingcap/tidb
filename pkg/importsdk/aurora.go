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
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
)

var (
	auroraBatchPattern = regexp.MustCompile(`^[0-9]+$`)
	dataFileSuffix     = regexp.MustCompile(`(?i)\.(sql|csv|parquet)(\.[^./]+)?$`)
)

// parseAuroraFile uses raw object-key components, not URL decoding or the last
// dot in schema.table. An empty root is valid when the URL scopes one export.
// candidate distinguishes a malformed Aurora-like path from generic input.
func parseAuroraFile(path string, fallback mydump.FileRouter) (table filter.Table, root string, candidate bool, err error) {
	parts := strings.Split(filepath.ToSlash(path), "/")
	if len(parts) < 3 {
		return table, "", false, nil
	}
	leaf := parts[len(parts)-1]
	isPart := len(leaf) >= 5 && strings.EqualFold(leaf[:5], "part-")
	if !isPart {
		// Preserve explicit Mydumper basenames even below similarly shaped
		// directories. Native AWS parts must instead use their parent names.
		if res, routeErr := fallback.Route(leaf); routeErr != nil || res != nil {
			return table, "", false, nil
		}
	}
	tableIndex := -1
	for i := len(parts) - 2; i >= 1; i-- {
		if parts[i-1] != "" && strings.HasPrefix(parts[i], parts[i-1]+".") {
			tableIndex = i
			break
		}
	}
	if tableIndex < 0 {
		// A native part below a dotted table directory must not silently fall
		// back to the generic router when its database prefix is inconsistent.
		if isPart && dataFileSuffix.MatchString(leaf) {
			for i := len(parts) - 2; i >= 1; i-- {
				if strings.Contains(parts[i], ".") {
					return table, "", true, errors.Errorf("inconsistent Aurora database/table directory: %s", path)
				}
			}
		}
		return table, "", false, nil
	}

	tail := parts[tableIndex+1:]
	if (len(tail) != 1 && (len(tail) != 2 || !auroraBatchPattern.MatchString(tail[0]))) ||
		!strings.EqualFold(filepath.Ext(tail[len(tail)-1]), ".parquet") {
		return table, "", true, errors.Errorf("unsupported Aurora data path: %s", path)
	}
	table.Schema = parts[tableIndex-1]
	table.Name = strings.TrimPrefix(parts[tableIndex], table.Schema+".")
	if table.Name == "" {
		return table, "", true, errors.Errorf("unsupported empty Aurora table name: %s", path)
	}
	// AWS replaces backslash, backtick, double quote and space with underscore.
	// Without authoritative metadata an underscore cannot be distinguished from
	// that lossy conversion. Explicit file routers can supply the intended name.
	if strings.ContainsAny(table.Schema+table.Name, "_\\`\" ") {
		return table, "", true, errors.Errorf("ambiguous Aurora identifier in %s; provide an explicit file route with the original name", path)
	}
	if strings.ContainsAny(table.Schema+table.Name, "*?[]") {
		return table, "", true, errors.Errorf("unsupported wildcard character in Aurora identifier: %s", path)
	}
	return table, strings.Join(parts[:tableIndex-1], "/"), true, nil
}

// newAuroraFileRouter validates the complete listing before filtering, sampling
// or grouping. A nil router leaves non-Aurora sources on the existing defaults.
func newAuroraFileRouter(files []mydump.RawFile) (mydump.FileRouter, error) {
	fallback, err := mydump.NewDefaultFileRouter(log.L())
	if err != nil {
		return nil, err
	}
	var root, unexpectedPath string
	var found, haveRoot bool
	var invalid error
	for _, file := range files {
		_, fileRoot, candidate, parseErr := parseAuroraFile(file.Path, fallback)
		if candidate {
			found = true
			if parseErr != nil {
				if invalid == nil {
					invalid = parseErr
				}
				continue
			}
			if haveRoot && root != fileRoot {
				return nil, errors.Errorf("multiple Aurora export roots %q and %q; scope the source URL to one export", root, fileRoot)
			}
			root, haveRoot = fileRoot, true
			continue
		}

		res, routeErr := fallback.Route(filepath.ToSlash(file.Path))
		if routeErr == nil && res != nil {
			switch res.Type {
			case mydump.SourceTypeIgnore, mydump.SourceTypeSchemaSchema, mydump.SourceTypeTableSchema, mydump.SourceTypeViewSchema:
				continue
			}
		}
		if dataFileSuffix.MatchString(file.Path) && unexpectedPath == "" {
			unexpectedPath = file.Path
		}
	}
	if !found {
		return nil, nil
	}
	if invalid != nil {
		return nil, invalid
	}
	if unexpectedPath != "" {
		return nil, errors.Errorf("mixed or unmatched data in Aurora source: %s", unexpectedPath)
	}
	return &auroraFileRouter{fallback: fallback}, nil
}

type auroraFileRouter struct {
	fallback mydump.FileRouter
}

func (r *auroraFileRouter) Route(path string) (*mydump.RouteResult, error) {
	table, _, candidate, err := parseAuroraFile(path, r.fallback)
	if err != nil {
		return nil, err
	}
	if candidate {
		return &mydump.RouteResult{Table: table, Type: mydump.SourceTypeParquet}, nil
	}
	return r.fallback.Route(path)
}
