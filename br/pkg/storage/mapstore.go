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
	"io/ioutil"
	"path"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"go.uber.org/atomic"
)

type mapFile struct {
	sync.RWMutex
	Data []byte
}

type mapStorage struct {
	sync.RWMutex
	dataStore map[string]*mapFile
}

func NewMapStorage() *mapStorage {
	return &mapStorage{
		dataStore: make(map[string]*mapFile),
	}
}

// DeleteFile delete the file in storage
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
	s.Lock()
	defer s.Unlock()
	if _, ok := s.dataStore[name]; !ok {
		return errors.Errorf("cannot find the file: %s", name)
	}
	delete(s.dataStore, name)
	return nil
}

// WriteFile file to storage.
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
	r := bytes.NewReader(data)
	fileData, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	s.Lock()
	defer s.Unlock()
	theFile, ok := s.dataStore[name]
	if ok {
		theFile.Lock()
		theFile.Data = fileData
		theFile.Unlock()
	} else {
		s.dataStore[name] = &mapFile{
			Data: fileData,
		}
	}
	return nil
}

// ReadFile storage file.
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
	s.RLock()
	defer s.RUnlock()
	theFile, ok := s.dataStore[name]
	if !ok {
		return nil, errors.Errorf("cannot find the file: %s", name)
	}
	theFile.RLock()
	defer theFile.RUnlock()
	r := bytes.NewReader(theFile.Data)
	retData, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return retData, nil
}

// FileExists return true if file exists.
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
	s.RLock()
	defer s.RUnlock()
	_, ok := s.dataStore[name]
	return ok, nil
}

// Open a Reader by file path.
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
	s.RLock()
	defer s.RUnlock()
	theFile, ok := s.dataStore[filePath]
	if !ok {
		return nil, errors.Errorf("cannot find the file: %s", filePath)
	}
	theFile.RLock()
	defer theFile.RUnlock()
	r := bytes.NewReader(theFile.Data)
	return &mapFileReader{
		Reader: r,
	}, nil
}

// WalkDir traverse all the files in a dir.
func (s *mapStorage) WalkDir(ctx context.Context, opt *WalkOption, fn func(string, int64) error) error {
	s.RLock()
	defer s.RUnlock()
	for fileName, file := range s.dataStore {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// continue on
		}
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
		file.RLock()
		fileSize := len(file.Data)
		file.RUnlock()
		if err := fn(fileName, int64(fileSize)); err != nil {
			return err
		}
	}
	return nil
}

func (s *mapStorage) URI() string {
	return "mapstore://"
}

// Create implements ExternalStorage interface.
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
	s.Lock()
	defer s.Unlock()
	if _, ok := s.dataStore[name]; ok {
		return nil, errors.Errorf("the file already exists: %s", name)
	}
	theFile := &mapFile{}
	s.dataStore[name] = theFile
	return &mapFileWriter{
		file: theFile,
	}, nil
}

// Rename implements ExternalStorage interface.
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
	s.Lock()
	defer s.Unlock()
	theFile, ok := s.dataStore[oldFileName]
	if !ok {
		return errors.Errorf("the file doesn't exist: %s", oldFileName)
	}
	if _, ok := s.dataStore[newFileName]; ok {
		return errors.Errorf("the new file has already existed: %s", newFileName)
	}
	s.dataStore[newFileName] = theFile
	delete(s.dataStore, oldFileName)
	return nil
}

type mapFileReader struct {
	*bytes.Reader
	isClosed atomic.Bool
}

func (r *mapFileReader) Read(p []byte) (int, error) {
	if r.isClosed.Load() {
		return -1, errors.New("reader closed")
	}
	return r.Reader.Read(p)
}

func (r *mapFileReader) Close() error {
	r.isClosed.Store(true)
	return nil
}

func (r *mapFileReader) Seek(offset int64, whence int) (int64, error) {
	if r.isClosed.Load() {
		return -1, errors.New("reader closed")
	}
	return r.Reader.Seek(offset, whence)
}

type mapFileWriter struct {
	buf  bytes.Buffer
	file *mapFile
}

func (w *mapFileWriter) Write(ctx context.Context, p []byte) (int, error) {
	select {
	case <-ctx.Done():
		return -1, ctx.Err()
	default:
		// continue on
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
	fileData, err := ioutil.ReadAll(&w.buf)
	if err != nil {
		return err
	}
	w.file.Lock()
	w.file.Data = fileData
	w.file.Unlock()
	return nil
}
