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
	"sort"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"go.uber.org/atomic"
)

type memFile struct {
	Data atomic.Pointer[[]byte]
}

// GetData gets the underlying byte slice of the atomic pointer
func (f *memFile) GetData() []byte {
	var fileData []byte
	if p := f.Data.Load(); p != nil {
		fileData = *p
	}
	return fileData
}

// MemStorage represents a in-memory storage.
type MemStorage struct {
	rwm       sync.RWMutex
	dataStore map[string]*memFile
}

// NewMemStorage creates a new in-memory storage.
func NewMemStorage() *MemStorage {
	return &MemStorage{
		dataStore: make(map[string]*memFile),
	}
}

func (s *MemStorage) loadMap(name string) (*memFile, bool) {
	s.rwm.RLock()
	defer s.rwm.RUnlock()
	theFile, ok := s.dataStore[name]
	return theFile, ok
}

// DeleteFile delete the file in storage
// It implements the `ExternalStorage` interface
func (s *MemStorage) DeleteFile(ctx context.Context, name string) error {
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

// DeleteFiles delete the files in storage
// It implements the `ExternalStorage` interface
func (s *MemStorage) DeleteFiles(ctx context.Context, names []string) error {
	for _, name := range names {
		err := s.DeleteFile(ctx, name)
		if err != nil {
			return err
		}
	}
	return nil
}

// WriteFile file to storage.
// It implements the `ExternalStorage` interface
func (s *MemStorage) WriteFile(ctx context.Context, name string, data []byte) error {
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
		theFile.Data.Store(&fileData)
	} else {
		theFile := new(memFile)
		theFile.Data.Store(&fileData)
		s.dataStore[name] = theFile
	}
	return nil
}

// ReadFile reads the storage file.
// It implements the `ExternalStorage` interface
func (s *MemStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
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
func (s *MemStorage) FileExists(ctx context.Context, name string) (bool, error) {
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
func (s *MemStorage) Open(ctx context.Context, filePath string, o *ReaderOption) (ExternalFileReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	if !path.IsAbs(filePath) {
		return nil, errors.Errorf("file name is not an absolute path: %s", filePath)
	}
	theFile, ok := s.loadMap(filePath)
	if !ok {
		return nil, errors.Errorf("cannot find the file: %s", filePath)
	}
	data := theFile.GetData()
	// just for simplicity, different from other implementation, MemStorage can't
	// seek beyond [o.StartOffset, o.EndOffset)
	start, end := 0, len(data)
	if o != nil {
		if o.StartOffset != nil {
			start = int(*o.StartOffset)
		}
		if o.EndOffset != nil {
			end = int(*o.EndOffset)
		}
	}
	r := bytes.NewReader(data[start:end])
	return &memFileReader{
		br:   r,
		size: int64(len(data)),
	}, nil
}

// WalkDir traverse all the files in a dir.
// It implements the `ExternalStorage` interface
func (s *MemStorage) WalkDir(ctx context.Context, opt *WalkOption, fn func(string, int64) error) error {
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
	sort.Strings(allFileNames)

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

// URI returns the URI of the storage.
func (*MemStorage) URI() string {
	return "memstore://"
}

// Create creates a file and returning a writer to write data into.
// When the writer is closed, the data is stored in the file.
// It implements the `ExternalStorage` interface
func (s *MemStorage) Create(ctx context.Context, name string, _ *WriterOption) (ExternalFileWriter, error) {
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
	theFile := new(memFile)
	s.dataStore[name] = theFile
	return &memFileWriter{
		file: theFile,
	}, nil
}

// Rename renames a file name to another file name.
// It implements the `ExternalStorage` interface
func (s *MemStorage) Rename(ctx context.Context, oldFileName, newFileName string) error {
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

// Close implements ExternalStorage interface.
func (s *MemStorage) Close() {
	s.dataStore = nil
}

// memFileReader is the struct to read data from an opend mem storage file
type memFileReader struct {
	br       *bytes.Reader
	size     int64
	isClosed atomic.Bool
}

// Read reads the mem storage file data
// It implements the `io.Reader` interface
func (r *memFileReader) Read(p []byte) (int, error) {
	if r.isClosed.Load() {
		return 0, io.EOF
	}
	return r.br.Read(p)
}

// Close closes the mem storage file data
// It implements the `io.Closer` interface
func (r *memFileReader) Close() error {
	r.isClosed.Store(true)
	return nil
}

// Seek seeks the offset inside the mem storage file
// It implements the `io.Seeker` interface
func (r *memFileReader) Seek(offset int64, whence int) (int64, error) {
	if r.isClosed.Load() {
		return 0, errors.New("reader closed")
	}
	return r.br.Seek(offset, whence)
}

func (r *memFileReader) GetFileSize() (int64, error) {
	return r.size, nil
}

// memFileReader is the struct to write data into the opened mem storage file
type memFileWriter struct {
	buf      bytes.Buffer
	file     *memFile
	isClosed atomic.Bool
}

// Write writes the data into the mem storage file buffer.
// It implements the `ExternalFileWriter` interface
func (w *memFileWriter) Write(ctx context.Context, p []byte) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		// continue on
	}
	if w.isClosed.Load() {
		return 0, errors.New("writer closed")
	}
	return w.buf.Write(p)
}

func (w *memFileWriter) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// continue on
	}
	fileData := append([]byte{}, w.buf.Bytes()...)
	w.file.Data.Store(&fileData)
	w.isClosed.Store(true)
	return nil
}
