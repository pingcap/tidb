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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
)

const maxSourceScanErrorSamples = 5

type auroraSourceDetection struct {
	matched    bool
	exportRoot string
	pathForm   string
	files      map[string]*mydump.AuroraSnapshotFilePath
}

// SourceScanner provides the high-level automatic-mapping result used by
// Cloud Import. It is separate from FileScanner to preserve that interface for
// existing implementations.
type SourceScanner interface {
	ScanSource(ctx context.Context) (*SourceScanResult, error)
}

// ScanSource returns the complete source mapping and the inventory evidence
// used to produce it.
func (s *fileScanner) ScanSource(ctx context.Context) (*SourceScanResult, error) {
	report := s.loader.GetScanReport()
	if report != nil && !report.Complete {
		return nil, &SourceScanError{Code: SourceScanErrorIncompleteScan}
	}
	detection, err := s.detectAuroraSource(report)
	if err != nil {
		return nil, err
	}

	tables, err := s.GetTableMetas(ctx)
	if err != nil {
		return nil, err
	}

	result := &SourceScanResult{
		Layout: SourceLayoutDefault,
		Tables: tables,
		Inventory: SourceInventory{
			Complete: report == nil || report.Complete,
		},
	}

	if report != nil {
		result.Inventory.ScannedObjectCount = int64(len(report.Files))
	}
	for _, table := range tables {
		result.Inventory.MappedObjectCount += table.ObjectCount
		result.Inventory.TotalObjectBytes += table.TotalObjectSize
	}
	if result.Inventory.MappedObjectCount == 0 {
		return nil, &SourceScanError{Code: SourceScanErrorNoImportableFiles}
	}

	if !detection.matched {
		result.Inventory.ImportableObjectCount = result.Inventory.MappedObjectCount
		result.Inventory.Digest = digestTableObjects(tables)
		return result, nil
	}

	result.Layout = SourceLayoutAuroraRDSSnapshot
	result.Evidence.ExportRoot = detection.exportRoot
	result.Evidence.PathForm = detection.pathForm
	result.Inventory.ImportableObjectCount = int64(len(detection.files))
	result.Inventory.TotalObjectBytes = 0

	mappedPaths := make(map[string]struct{}, len(detection.files))
	for _, table := range tables {
		for _, file := range table.DataFiles {
			mappedPaths[file.Path] = struct{}{}
		}
	}

	unmapped := make([]string, 0)
	var objectBytes int64
	rawSizes := make(map[string]int64, len(detection.files))
	for _, rawFile := range report.Files {
		if _, ok := detection.files[rawFile.Path]; !ok {
			continue
		}
		rawSizes[rawFile.Path] = rawFile.Size
		objectBytes += rawFile.Size
		if _, ok := mappedPaths[rawFile.Path]; !ok {
			unmapped = append(unmapped, rawFile.Path)
		}
	}
	if len(unmapped) > 0 {
		return nil, newSourceScanError(
			SourceScanErrorUnsupportedPath,
			unmapped,
			nil,
		)
	}

	result.Inventory.MappedObjectCount = int64(len(detection.files))
	result.Inventory.TotalObjectBytes = objectBytes
	result.Inventory.Digest = digestRawObjects(rawSizes)
	return result, nil
}

func (s *fileScanner) detectAuroraSource(report *mydump.ScanReport) (*auroraSourceDetection, error) {
	result := &auroraSourceDetection{files: make(map[string]*mydump.AuroraSnapshotFilePath)}
	// Explicit file routers describe a user-selected layout and must retain
	// their existing semantics.
	if report == nil || len(s.config.fileRouteRules) > 0 {
		return result, nil
	}

	unmatched := make([]string, 0)
	exportRoots := make(map[string]struct{})
	pathForms := make(map[mydump.AuroraSnapshotPathForm]struct{})

	for _, file := range report.Files {
		if !strings.HasSuffix(strings.ToLower(file.Path), ".parquet") {
			if isNonParquetDataObject(file.Path) {
				unmatched = append(unmatched, file.Path)
			}
			continue
		}
		parsed, matched, err := mydump.ParseAuroraSnapshotFilePath(file.Path)
		if err != nil {
			code := SourceScanErrorUnsupportedPath
			if errors.ErrorEqual(err, mydump.ErrAmbiguousAuroraSnapshotPath) {
				code = SourceScanErrorAmbiguousLayout
			}
			return nil, newSourceScanError(
				code,
				[]string{file.Path},
				err,
			)
		}
		if !matched {
			unmatched = append(unmatched, file.Path)
			continue
		}
		result.files[file.Path] = parsed
		exportRoots[parsed.ExportRoot] = struct{}{}
		pathForms[parsed.Form] = struct{}{}
	}

	if len(result.files) == 0 {
		return result, nil
	}
	result.matched = true

	if len(unmatched) > 0 {
		return nil, newSourceScanError(SourceScanErrorMixedLayout, unmatched, nil)
	}
	if len(exportRoots) != 1 {
		roots := make([]string, 0, len(exportRoots))
		for root := range exportRoots {
			if root == "" {
				root = "."
			}
			roots = append(roots, root)
		}
		sort.Strings(roots)
		return nil, newSourceScanError(SourceScanErrorMultipleExportRoots, roots, nil)
	}
	for root := range exportRoots {
		result.exportRoot = root
	}
	if len(pathForms) == 1 {
		for form := range pathForms {
			result.pathForm = string(form)
		}
	} else {
		// Both AWS leaf conventions retain the same unambiguous table base
		// prefix and can safely coexist across tables.
		result.pathForm = "mixed"
	}
	return result, nil
}

func isNonParquetDataObject(path string) bool {
	name := strings.ToLower(filepath.Base(path))
	if compression := mydump.ParseCompressionOnFileExtension(name); compression != mydump.CompressionNone {
		name = strings.TrimSuffix(name, filepath.Ext(name))
	}

	if strings.HasSuffix(name, "-schema-create.sql") ||
		strings.HasSuffix(name, "-schema.sql") ||
		strings.HasSuffix(name, "-schema-view.sql") ||
		strings.HasSuffix(name, "-schema-trigger.sql") ||
		strings.HasSuffix(name, "-schema-post.sql") {
		return false
	}
	return strings.HasSuffix(name, ".sql") || strings.HasSuffix(name, ".csv")
}

func newSourceScanError(code SourceScanErrorCode, samples []string, cause error) *SourceScanError {
	count := len(samples)
	if len(samples) > maxSourceScanErrorSamples {
		samples = samples[:maxSourceScanErrorSamples]
	}
	return &SourceScanError{
		Code:    code,
		Count:   int64(count),
		Samples: samples,
		Cause:   cause,
	}
}

func digestTableObjects(tables []*TableMeta) string {
	objects := make(map[string]int64)
	for _, table := range tables {
		for _, file := range table.DataFiles {
			objects[file.Path] = file.ObjectSize
		}
	}
	return digestRawObjects(objects)
}

func digestRawObjects(objects map[string]int64) string {
	paths := make([]string, 0, len(objects))
	for path := range objects {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	hash := sha256.New()
	for _, path := range paths {
		_, _ = fmt.Fprintf(hash, "%s\x00%d\n", path, objects[path])
	}
	return hex.EncodeToString(hash.Sum(nil))
}
