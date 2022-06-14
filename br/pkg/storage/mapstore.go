// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"bytes"
	"context"
	"io"
	"path"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"go.uber.org/atomic"
)

type mapFile struct {
	Data atomic.Value // the atomic value is a byte slice, which can only be get/set atomically
}

// GetData gets the underlying byte slice of the atomic value
func (f *mapFile) GetData() []byte {
	var fileData []byte
	fileDataVal := f.Data.Load()
	if fileDataVal != nil {
		fileData = fileDataVal.([]byte)
	}
	return fileData
}

type mapStorage struct {
	rwm       sync.RWMutex
	dataStore map[string]*mapFile
}

func NewMapStorage() *mapStorage {
	return &mapStorage{
		dataStore: make(map[string]*mapFile),
	}
}

func (s *mapStorage) loadMap(name string) (*mapFile, bool) {
	s.rwm.RLock()
	defer s.rwm.RUnlock()
	theFile, ok := s.dataStore[name]
	return theFile, ok
}

// DeleteFile delete the file in storage
// It implements the `ExternalStorage` interface
func (s *mapStorage) DeleteFile(ctx context.Context, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// continue on
	}
	if !path.IsAbs(name) {
		return errors.Errorf("file name is not an absolute path: %s", name)
	}
	s.rwm.Lock()
	defer s.rwm.Unlock()
	if _, ok := s.dataStore[name]; !ok {
		return errors.Errorf("cannot find the file: %s", name)
	}
	delete(s.dataStore, name)
	return nil
}

// WriteFile file to storage.
// It implements the `ExternalStorage` interface
func (s *mapStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// continue on
	}
	if !path.IsAbs(name) {
		return errors.Errorf("file name is not an absolute path: %s", name)
	}
	fileData := append([]byte{}, data...)
	s.rwm.Lock()
	defer s.rwm.Unlock()
	theFile, ok := s.dataStore[name]
	if ok {
		theFile.Data.Store(fileData)
	} else {
		theFile := new(mapFile)
		theFile.Data.Store(fileData)
		s.dataStore[name] = theFile
	}
	return nil
}

// ReadFile reads the storage file.
// It implements the `ExternalStorage` interface
func (s *mapStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// continue on
	}
	if !path.IsAbs(name) {
		return nil, errors.Errorf("file name is not an absolute path: %s", name)
	}
	theFile, ok := s.loadMap(name)
	if !ok {
		return nil, errors.Errorf("cannot find the file: %s", name)
	}
	fileData := theFile.GetData()
	return append([]byte{}, fileData...), nil
}

// FileExists return true if file exists.
// It implements the `ExternalStorage` interface
func (s *mapStorage) FileExists(ctx context.Context, name string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
		// continue on
	}
	if !path.IsAbs(name) {
		return false, errors.Errorf("file name is not an absolute path: %s", name)
	}
	_, ok := s.loadMap(name)
	return ok, nil
}

// Open opens a Reader by file path.
// It implements the `ExternalStorage` interface
func (s *mapStorage) Open(ctx context.Context, filePath string) (ExternalFileReader, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// continue on
	}
	if !path.IsAbs(filePath) {
		return nil, errors.Errorf("file name is not an absolute path: %s", filePath)
	}
	theFile, ok := s.loadMap(filePath)
	if !ok {
		return nil, errors.Errorf("cannot find the file: %s", filePath)
	}
	r := bytes.NewReader(theFile.GetData())
	return &mapFileReader{
		br: r,
	}, nil
}

// WalkDir traverse all the files in a dir.
// It implements the `ExternalStorage` interface
func (s *mapStorage) WalkDir(ctx context.Context, opt *WalkOption, fn func(string, int64) error) error {
	allFileNames := func() []string {
		fileNames := []string{}
		s.rwm.RLock()
		defer s.rwm.RUnlock()
		for fileName := range s.dataStore {
			if opt != nil {
				if len(opt.SubDir) > 0 {
					if !strings.HasPrefix(fileName, opt.SubDir) {
						continue
					}
				}
				if len(opt.ObjPrefix) > 0 {
					baseName := path.Base(fileName)
					if !strings.HasPrefix(baseName, opt.ObjPrefix) {
						continue
					}
				}
			}
			fileNames = append(fileNames, fileName)
		}
		return fileNames
	}()

	for _, fileName := range allFileNames {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// continue on
		}
		theFile, ok := s.loadMap(fileName)
		if !ok {
			continue
		}
		fileSize := len(theFile.GetData())
		if err := fn(fileName, int64(fileSize)); err != nil {
			return err
		}
	}
	return nil
}

func (s *mapStorage) URI() string {
	return "mapstore://"
}

// Create creates a file and returning a writer to write data into.
// When the writer is closed, the data is stored in the file.
// It implements the `ExternalStorage` interface
func (s *mapStorage) Create(ctx context.Context, name string) (ExternalFileWriter, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// continue on
	}
	if !path.IsAbs(name) {
		return nil, errors.Errorf("file name is not an absolute path: %s", name)
	}
	s.rwm.Lock()
	defer s.rwm.Unlock()
	if _, ok := s.dataStore[name]; ok {
		return nil, errors.Errorf("the file already exists: %s", name)
	}
	theFile := new(mapFile)
	s.dataStore[name] = theFile
	return &mapFileWriter{
		file: theFile,
	}, nil
}

// Rename renames a file name to another file name.
// It implements the `ExternalStorage` interface
func (s *mapStorage) Rename(ctx context.Context, oldFileName, newFileName string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// continue on
	}
	if !path.IsAbs(newFileName) {
		return errors.Errorf("new file name is not an absolute path: %s", newFileName)
	}
	s.rwm.Lock()
	defer s.rwm.Unlock()
	theFile, ok := s.dataStore[oldFileName]
	if !ok {
		return errors.Errorf("the file doesn't exist: %s", oldFileName)
	}
	s.dataStore[newFileName] = theFile
	delete(s.dataStore, oldFileName)
	return nil
}

// mapFileReader is the struct to read data from an opend map storage file
type mapFileReader struct {
	br       *bytes.Reader
	isClosed atomic.Bool
}

// Read reads the map storage file data
// It implements the `io.Reader` interface
func (r *mapFileReader) Read(p []byte) (int, error) {
	if r.isClosed.Load() {
		return 0, io.EOF
	}
	return r.br.Read(p)
}

// Close closes the map storage file data
// It implements the `io.Closer` interface
func (r *mapFileReader) Close() error {
	r.isClosed.Store(true)
	return nil
}

// Seeker seekds the offset inside the map storage file
// It implements the `io.Seeker` interface
func (r *mapFileReader) Seek(offset int64, whence int) (int64, error) {
	if r.isClosed.Load() {
		return -1, errors.New("reader closed")
	}
	return r.br.Seek(offset, whence)
}

// mapFileReader is the struct to write data into the opened map storage file
type mapFileWriter struct {
	buf      bytes.Buffer
	file     *mapFile
	isClosed atomic.Bool
}

// Write writes the data into the map storage file buffer.
// It implements the `ExternalFileWriter` interface
func (w *mapFileWriter) Write(ctx context.Context, p []byte) (int, error) {
	select {
	case <-ctx.Done():
		return -1, ctx.Err()
	default:
		// continue on
	}
	if w.isClosed.Load() {
		return -1, errors.New("writer closed")
	}
	return w.buf.Write(p)
}

func (w *mapFileWriter) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// continue on
	}
	fileData := append([]byte{}, w.buf.Bytes()...)
	w.file.Data.Store(fileData)
	w.isClosed.Store(true)
	return nil
}
