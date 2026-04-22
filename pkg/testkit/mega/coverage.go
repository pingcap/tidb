// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mega

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/tools/cover"
)

// CoverageCollector manages per-test coverage profile files and merges them
// into a single final profile.
type CoverageCollector struct {
	mu      sync.Mutex
	tmpDir  string
	counter int
}

// NewCoverageCollector creates a temp directory for per-test coverage files.
func NewCoverageCollector() (*CoverageCollector, error) {
	tmpDir, err := os.MkdirTemp("", "mega-cov")
	if err != nil {
		return nil, fmt.Errorf("create coverage temp dir: %w", err)
	}
	return &CoverageCollector{tmpDir: tmpDir}, nil
}

// TempFile returns a unique temp file path for a single test's coverage output.
func (c *CoverageCollector) TempFile(pkg, name string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counter++
	safePkg := strings.ReplaceAll(pkg, "/", "_")
	safeName := strings.ReplaceAll(name, "/", "_")
	return filepath.Join(c.tmpDir, fmt.Sprintf("%s_%s_%d.cov", safePkg, safeName, c.counter))
}

// Cleanup removes the temp directory. Call this after merging.
func (c *CoverageCollector) Cleanup() {
	os.RemoveAll(c.tmpDir)
}

// MergeProfiles merges all per-test coverage files into a single profile file.
// The output file starts with "mode: set\n" followed by merged profile blocks.
func (c *CoverageCollector) MergeProfiles(outputPath string) error {
	files, err := os.ReadDir(c.tmpDir)
	if err != nil {
		return fmt.Errorf("read coverage temp dir: %w", err)
	}

	result := make(map[string]*cover.Profile)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if err := mergeOneCoverageFile(result, filepath.Join(c.tmpDir, file.Name())); err != nil {
			return fmt.Errorf("merge coverage file %s: %w", file.Name(), err)
		}
	}

	w, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create coverage output file: %w", err)
	}
	defer w.Close()

	// Write header
	if _, err := w.WriteString("mode: set\n"); err != nil {
		return err
	}

	bw := bufio.NewWriter(w)
	for _, prof := range result {
		for _, block := range prof.Blocks {
			fmt.Fprintf(bw, "%s:%d.%d,%d.%d %d %d\n",
				prof.FileName,
				block.StartLine,
				block.StartCol,
				block.EndLine,
				block.EndCol,
				block.NumStmt,
				block.Count,
			)
		}
	}
	return bw.Flush()
}

// mergeOneCoverageFile reads a single coverage profile file and merges it into result.
func mergeOneCoverageFile(result map[string]*cover.Profile, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	profs, err := cover.ParseProfilesFromReader(f)
	if err != nil {
		return err
	}
	for _, p := range profs {
		if existing, ok := result[p.FileName]; ok {
			mergeProfileBlocks(existing, p)
		} else {
			result[p.FileName] = p
		}
	}
	return nil
}

// mergeProfileBlocks merges blocks from src into dst.
// This is a simplified merge — blocks at the same position have counts summed.
func mergeProfileBlocks(dst, src *cover.Profile) {
	// Build a lookup from dst blocks
	for _, sb := range src.Blocks {
		found := false
		for i := range dst.Blocks {
			db := &dst.Blocks[i]
			if db.StartLine == sb.StartLine &&
				db.StartCol == sb.StartCol &&
				db.EndLine == sb.EndLine &&
				db.EndCol == sb.EndCol {
				db.Count += sb.Count
				found = true
				break
			}
		}
		if !found {
			dst.Blocks = append(dst.Blocks, sb)
		}
	}
}