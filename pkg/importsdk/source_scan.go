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
	tablefilter "github.com/pingcap/tidb/pkg/util/table-filter"
)

const maxSourceScanErrorSamples = 5

type auroraSourceDetection struct {
	matched    bool
	exportRoot string
	pathForm   mydump.AuroraSnapshotPathForm
	files      map[string]auroraDetectedFile
}

type auroraDetectedFile struct {
	schema string
	table  string
}

type sourceScanErrorSamples struct {
	count   int64
	samples []string
}

func (s *sourceScanErrorSamples) add(path string) {
	s.count++
	if len(s.samples) < maxSourceScanErrorSamples {
		s.samples = append(s.samples, path)
	}
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
		return nil, &SourceScanError{
			Code:  SourceScanErrorIncompleteScan,
			Count: int64(len(report.Files)),
			Cause: report.Err,
		}
	}
	detection, err := s.detectAuroraSource(ctx, report)
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
	if !detection.matched {
		importableCount, err := s.validateDefaultCoverage(ctx, report, tables)
		if err != nil {
			return nil, err
		}
		result.Inventory.ImportableObjectCount = importableCount
		result.Inventory.Digest = digestTableObjects(tables)
		if result.Inventory.MappedObjectCount == 0 {
			return nil, &SourceScanError{Code: SourceScanErrorNoImportableFiles}
		}
		return result, nil
	}

	result.Layout = SourceLayoutAuroraRDSSnapshot
	result.Evidence.ExportRoot = detection.exportRoot
	result.Evidence.PathForm = detection.pathForm
	result.Inventory.TotalObjectBytes = 0
	configuredFilter, err := tablefilter.Parse(s.config.filter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	configuredFilter = tablefilter.CaseInsensitive(configuredFilter)

	mappedPaths := make(map[string]struct{}, len(detection.files))
	for _, table := range tables {
		for _, file := range table.DataFiles {
			mappedPaths[file.Path] = struct{}{}
		}
	}

	var unmapped sourceScanErrorSamples
	var objectBytes int64
	rawObjects := make([]mydump.RawFile, 0, len(detection.files))
	for _, rawFile := range report.Files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		parsed, ok := detection.files[rawFile.Path]
		if !ok || !configuredFilter.MatchTable(parsed.schema, parsed.table) {
			continue
		}
		result.Inventory.ImportableObjectCount++
		rawObjects = append(rawObjects, rawFile)
		objectBytes += rawFile.Size
		if _, ok := mappedPaths[rawFile.Path]; !ok {
			unmapped.add(rawFile.Path)
		}
	}
	if unmapped.count > 0 {
		return nil, newSourceScanErrorWithCount(
			SourceScanErrorUnsupportedPath,
			unmapped.count,
			unmapped.samples,
			nil,
		)
	}
	if result.Inventory.ImportableObjectCount == 0 {
		return nil, &SourceScanError{Code: SourceScanErrorNoImportableFiles}
	}

	result.Inventory.MappedObjectCount = result.Inventory.ImportableObjectCount
	result.Inventory.TotalObjectBytes = objectBytes
	result.Inventory.Digest = digestRawFileObjects(rawObjects)
	return result, nil
}

func (s *fileScanner) validateDefaultCoverage(
	ctx context.Context,
	report *mydump.ScanReport,
	tables []*TableMeta,
) (int64, error) {
	// Explicit routers intentionally define what is importable. Preserve their
	// established behavior instead of second-guessing them with default rules.
	if report == nil || len(s.config.fileRouteRules) > 0 {
		var count int64
		for _, table := range tables {
			count += table.ObjectCount
		}
		return count, nil
	}

	router, err := mydump.NewDefaultFileRouter(s.config.logger)
	if err != nil {
		return 0, errors.Trace(err)
	}
	configuredFilter, err := tablefilter.Parse(s.config.filter)
	if err != nil {
		return 0, errors.Trace(err)
	}
	configuredFilter = tablefilter.CaseInsensitive(configuredFilter)

	mapped := make(map[string]struct{})
	for _, table := range tables {
		for _, file := range table.DataFiles {
			mapped[file.Path] = struct{}{}
		}
	}

	var importableCount int64
	var unmapped sourceScanErrorSamples
	for _, raw := range report.Files {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		routed, routeErr := router.Route(filepath.ToSlash(raw.Path))
		if routeErr != nil {
			return 0, newSourceScanError(SourceScanErrorUnsupportedPath, []string{raw.Path}, routeErr)
		}
		if routed == nil {
			if isDataObject(raw.Path) {
				unmapped.add(raw.Path)
			}
			continue
		}
		if routed.Type != mydump.SourceTypeSQL &&
			routed.Type != mydump.SourceTypeCSV &&
			routed.Type != mydump.SourceTypeParquet {
			continue
		}
		if !configuredFilter.MatchTable(routed.Schema, routed.Name) {
			continue
		}
		importableCount++
		if _, ok := mapped[raw.Path]; !ok {
			unmapped.add(raw.Path)
		}
	}
	if unmapped.count > 0 {
		return 0, newSourceScanErrorWithCount(
			SourceScanErrorUnsupportedPath, unmapped.count, unmapped.samples, nil,
		)
	}
	return importableCount, nil
}

func (s *fileScanner) detectAuroraSource(
	ctx context.Context,
	report *mydump.ScanReport,
) (*auroraSourceDetection, error) {
	result := &auroraSourceDetection{files: make(map[string]auroraDetectedFile)}
	// Explicit file routers describe a user-selected layout and must retain
	// their existing semantics.
	if report == nil || len(s.config.fileRouteRules) > 0 {
		return result, nil
	}

	defaultRouter, err := mydump.NewDefaultFileRouter(s.config.logger)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var unmatched sourceScanErrorSamples
	var parseFailures sourceScanErrorSamples
	var parseFailureCause error
	parseFailureCode := SourceScanErrorUnsupportedPath
	exportRoots := make(map[string]struct{})
	pathForms := make(map[mydump.AuroraSnapshotPathForm]struct{})

	for _, file := range report.Files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !strings.EqualFold(filepath.Ext(file.Path), ".parquet") {
			isData, routeErr := isDefaultDataObject(defaultRouter, file.Path)
			if routeErr != nil {
				return nil, newSourceScanError(SourceScanErrorUnsupportedPath, []string{file.Path}, routeErr)
			}
			if isData {
				unmatched.add(file.Path)
			}
			continue
		}
		parsed, matched, parseErr := mydump.ParseAuroraSnapshotFilePath(file.Path)
		if parseErr != nil {
			parseFailures.add(file.Path)
			if parseFailureCause == nil {
				parseFailureCause = parseErr
			}
			if errors.ErrorEqual(parseErr, mydump.ErrAmbiguousAuroraSnapshotPath) {
				parseFailureCode = SourceScanErrorAmbiguousLayout
			}
			continue
		}
		if !matched {
			unmatched.add(file.Path)
			continue
		}
		result.files[file.Path] = auroraDetectedFile{
			schema: parsed.Schema,
			table:  parsed.Table,
		}
		exportRoots[parsed.ExportRoot] = struct{}{}
		pathForms[parsed.Form] = struct{}{}
	}

	if parseFailures.count > 0 {
		return nil, newSourceScanErrorWithCount(
			parseFailureCode, parseFailures.count, parseFailures.samples, parseFailureCause,
		)
	}
	if len(result.files) == 0 {
		return result, nil
	}
	result.matched = true
	if unmatched.count > 0 {
		return nil, newSourceScanErrorWithCount(
			SourceScanErrorMixedLayout, unmatched.count, unmatched.samples, nil,
		)
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
			result.pathForm = form
		}
	} else {
		// Both AWS leaf conventions retain the same unambiguous table base
		// prefix and can safely coexist across tables.
		result.pathForm = mydump.AuroraSnapshotPathFormMixed
	}
	return result, nil
}

func isDefaultDataObject(router mydump.FileRouter, path string) (bool, error) {
	routed, err := router.Route(filepath.ToSlash(path))
	if err != nil {
		return false, err
	}
	if routed == nil {
		return isDataObject(path), nil
	}
	return routed.Type == mydump.SourceTypeSQL ||
		routed.Type == mydump.SourceTypeCSV ||
		routed.Type == mydump.SourceTypeParquet, nil
}

func isDataObject(path string) bool {
	name := strings.ToLower(filepath.Base(path))
	if compression := mydump.ParseCompressionOnFileExtension(name); compression != mydump.CompressionNone {
		name = strings.TrimSuffix(name, filepath.Ext(name))
	}
	return strings.HasSuffix(name, ".sql") ||
		strings.HasSuffix(name, ".csv") ||
		strings.HasSuffix(name, ".parquet")
}

func newSourceScanError(code SourceScanErrorCode, samples []string, cause error) *SourceScanError {
	return newSourceScanErrorWithCount(code, int64(len(samples)), samples, cause)
}

func newSourceScanErrorWithCount(
	code SourceScanErrorCode,
	count int64,
	samples []string,
	cause error,
) *SourceScanError {
	if len(samples) > maxSourceScanErrorSamples {
		samples = samples[:maxSourceScanErrorSamples]
	}
	return &SourceScanError{
		Code:    code,
		Count:   count,
		Samples: append([]string(nil), samples...),
		Cause:   cause,
	}
}

func digestRawFileObjects(objects []mydump.RawFile) string {
	sort.Slice(objects, func(i, j int) bool { return objects[i].Path < objects[j].Path })
	hash := sha256.New()
	for _, object := range objects {
		_, _ = fmt.Fprintf(hash, "%s\x00%d\n", object.Path, object.Size)
	}
	return hex.EncodeToString(hash.Sum(nil))
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
