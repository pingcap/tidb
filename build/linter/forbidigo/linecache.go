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

package forbidigo

import (
	"bytes"
	"fmt"
	"os"
	"sync"
)

type fileLinesCache [][]byte

// LineCache is a cache for file lines.
type LineCache struct {
	files sync.Map
}

// GetLine returns the index1-th (1-based index) line from the file on filePath
func (lc *LineCache) GetLine(filePath string, index1 int) (string, error) {
	rawLine, err := lc.getRawLine(filePath, index1-1)
	if err != nil {
		return "", err
	}

	return string(bytes.Trim(rawLine, "\r")), nil
}

func (lc *LineCache) getRawLine(filePath string, index0 int) ([]byte, error) {
	fc, err := lc.getFileCache(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get file %s lines cache: %w", filePath, err)
	}

	if index0 < 0 {
		return nil, fmt.Errorf("invalid file line index0 < 0: %d", index0)
	}

	if index0 >= len(fc) {
		return nil, fmt.Errorf("invalid file line index0 (%d) >= len(fc) (%d)", index0, len(fc))
	}

	return fc[index0], nil
}

func (lc *LineCache) getFileCache(filePath string) (fileLinesCache, error) {
	loadedFc, ok := lc.files.Load(filePath)
	if ok {
		return loadedFc.(fileLinesCache), nil
	}

	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("can't read file %s: %w", filePath, err)
	}

	fc := bytes.Split(fileBytes, []byte("\n"))
	lc.files.Store(filePath, fileLinesCache(fc))
	return fc, nil
}
