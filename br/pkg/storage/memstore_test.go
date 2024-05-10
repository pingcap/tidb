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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestMemStoreBasic(t *testing.T) {
	store := NewMemStorage()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var err error
	// write and then read
	require.Nil(t, store.WriteFile(ctx, "/hello.txt", []byte("hello world")))
	fileContent, err := store.ReadFile(ctx, "/hello.txt")
	require.Nil(t, err)
	require.True(t, bytes.Equal([]byte("hello world"), fileContent))

	// write the same file and then read
	require.Nil(t, store.WriteFile(ctx, "/hello.txt", []byte("hello world 2")))
	fileContent, err = store.ReadFile(ctx, "/hello.txt")
	require.Nil(t, err)
	require.True(t, bytes.Equal([]byte("hello world 2"), fileContent))

	// delete the file and then delete & read
	require.Nil(t, store.DeleteFile(ctx, "/hello.txt"))
	require.NotNil(t, store.DeleteFile(ctx, "/hello.txt"))
	_, err = store.ReadFile(ctx, "/hello.txt")
	require.NotNil(t, err)

	// create a writer to write
	w, err := store.Create(ctx, "/hello.txt", nil)
	require.Nil(t, err)
	_, err = w.Write(ctx, []byte("hello world 3"))
	require.Nil(t, err)
	// read the file content before writer closing
	fileContent, err = store.ReadFile(ctx, "/hello.txt")
	require.Nil(t, err)
	require.True(t, bytes.Equal([]byte(""), fileContent))
	require.Nil(t, w.Close(ctx))
	// read the file content after writer closing
	fileContent, err = store.ReadFile(ctx, "/hello.txt")
	require.Nil(t, err)
	require.True(t, bytes.Equal([]byte("hello world 3"), fileContent))

	// simultaneously create two readers on the same file
	r, err := store.Open(ctx, "/hello.txt", nil)
	require.Nil(t, err)
	r2, err := store.Open(ctx, "/hello.txt", nil)
	require.Nil(t, err)
	fileContent, err = io.ReadAll(r)
	require.Nil(t, err)
	require.True(t, bytes.Equal([]byte("hello world 3"), fileContent))
	require.Nil(t, r.Close())
	// operations after close
	_, err = r.Read(make([]byte, 3))
	require.NotNil(t, err)

	// delete the file, but there is an existing reader on this file
	require.Nil(t, store.DeleteFile(ctx, "/hello.txt"))

	_, err = r2.Seek(5, io.SeekStart)
	require.Nil(t, err)
	fileContent, err = io.ReadAll(r2)
	require.Nil(t, err)
	require.True(t, bytes.Equal([]byte(" world 3"), fileContent))

	// rename the file
	var exists bool
	require.Nil(t, store.WriteFile(ctx, "/hello.txt", []byte("hello world 3")))
	require.Nil(t, store.WriteFile(ctx, "/hello2.txt", []byte("hello world 2")))
	exists, err = store.FileExists(ctx, "/hello.txt")
	require.Nil(t, err)
	require.True(t, exists)
	exists, err = store.FileExists(ctx, "/hello2.txt")
	require.Nil(t, err)
	require.True(t, exists)
	require.NotNil(t, store.Rename(ctx, "/NOT_EXIST.txt", "/NEW_FILE.txt"))
	require.Nil(t, store.Rename(ctx, "/hello2.txt", "/hello3.txt"))
	require.Nil(t, store.Rename(ctx, "/hello.txt", "/hello3.txt"))
	exists, err = store.FileExists(ctx, "/hello.txt")
	require.Nil(t, err)
	require.False(t, exists)
	exists, err = store.FileExists(ctx, "/hello2.txt")
	require.Nil(t, err)
	require.False(t, exists)
	exists, err = store.FileExists(ctx, "/hello3.txt")
	require.Nil(t, err)
	require.True(t, exists)
}

type iterFileInfo struct {
	Name    string
	Size    int64
	Content []byte
}

func TestMemStoreWalkDir(t *testing.T) {
	store := NewMemStorage()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	allTestFiles := map[string][]byte{
		"/hello.txt":     []byte("hello world"),
		"/hello2.txt":    []byte("hello world 2"),
		"/aaa/hello.txt": []byte("aaa: hello world"),
		"/aaa/world.txt": []byte("aaa: world"),
		"/dummy.txt":     []byte("dummy"),
	}
	for fileName, content := range allTestFiles {
		require.Nil(t, store.WriteFile(ctx, fileName, content))
	}

	iterFileInfos := []*iterFileInfo{}
	iterFn := func(fileName string, fileSize int64) error {
		fileContent, err := store.ReadFile(ctx, fileName)
		if err != nil {
			return err
		}
		iterFileInfos = append(iterFileInfos, &iterFileInfo{
			Name:    fileName,
			Size:    fileSize,
			Content: fileContent,
		})
		return nil
	}
	require.Nil(t, store.WalkDir(ctx, &WalkOption{}, iterFn))
	require.Equal(t, len(allTestFiles), len(iterFileInfos))
	for _, info := range iterFileInfos {
		expectContent, ok := allTestFiles[info.Name]
		require.True(t, ok, info.Name)
		require.Equal(t, int64(len(info.Content)), info.Size, info.Name)
		require.True(t, bytes.Equal(expectContent, info.Content), info.Name)
	}

	iterFileInfos = iterFileInfos[:0]
	expectFiles := make(map[string][]byte)
	expectFiles["/aaa/hello.txt"] = allTestFiles["/aaa/hello.txt"]
	expectFiles["/aaa/world.txt"] = allTestFiles["/aaa/world.txt"]
	require.Nil(t, store.WalkDir(ctx, &WalkOption{
		SubDir: "/aaa",
	}, iterFn))
	require.Equal(t, len(expectFiles), len(iterFileInfos))
	for _, info := range iterFileInfos {
		expectContent, ok := expectFiles[info.Name]
		require.True(t, ok, info.Name)
		require.Equal(t, int64(len(info.Content)), info.Size, info.Name)
		require.True(t, bytes.Equal(expectContent, info.Content), info.Name)
	}

	iterFileInfos = iterFileInfos[:0]
	expectFiles = make(map[string][]byte)
	expectFiles["/hello.txt"] = allTestFiles["/hello.txt"]
	expectFiles["/hello2.txt"] = allTestFiles["/hello2.txt"]
	expectFiles["/aaa/hello.txt"] = allTestFiles["/aaa/hello.txt"]
	require.Nil(t, store.WalkDir(ctx, &WalkOption{
		ObjPrefix: "hello",
	}, iterFn))
	require.Equal(t, len(expectFiles), len(iterFileInfos))
	for _, info := range iterFileInfos {
		expectContent, ok := expectFiles[info.Name]
		require.True(t, ok, info.Name)
		require.Equal(t, int64(len(info.Content)), info.Size, info.Name)
		require.True(t, bytes.Equal(expectContent, info.Content), info.Name)
	}
}

func TestMemStoreManipulateBytes(t *testing.T) {
	store := NewMemStorage()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testStr := "aaa1"
	testBytes := []byte(testStr)
	require.Nil(t, store.WriteFile(ctx, "/aaa.txt", testBytes))
	testBytes[3] = '2'
	require.Equal(t, testStr, string(*store.dataStore["/aaa.txt"].Data.Load()))

	readBytes, err := store.ReadFile(ctx, "/aaa.txt")
	require.Nil(t, err)
	require.Equal(t, testStr, string(readBytes))
	readBytes[3] = '2'
	require.Equal(t, testStr, string(*store.dataStore["/aaa.txt"].Data.Load()))
}

func TestMemStoreWriteDuringWalkDir(t *testing.T) {
	store := NewMemStorage()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var deleteFileName string
	allTestFiles := map[string][]byte{
		"/hello1.txt": []byte("hello world 1"),
		"/hello2.txt": []byte("hello world 2"),
		"/hello3.txt": []byte("hello world 3"),
	}
	var remainFilesMap sync.Map
	for fileName, content := range allTestFiles {
		require.Nil(t, store.WriteFile(ctx, fileName, content))
		remainFilesMap.Store(fileName, true)
	}

	pendingCh := make(chan struct{})
	ch1 := make(chan error)
	ch2 := make(chan error)
	go func() {
		<-pendingCh
		ch1 <- store.WalkDir(ctx, nil, func(fileName string, size int64) error {
			t.Logf("iterating: %s", fileName)
			remainFilesMap.Delete(fileName)
			time.Sleep(1 * time.Second)
			return nil
		})
		close(ch1)
	}()
	go func() {
		defer close(ch2)
		<-pendingCh
		time.Sleep(100 * time.Millisecond) // set some lag, to let 'WalkDir' go-routine run first
		err := store.WriteFile(ctx, "/hello4.txt", []byte("hello world4"))
		if err != nil {
			ch2 <- err
			return
		}
		t.Log("new file written")
		remainFilesMap.Range(func(k any, v any) bool {
			deleteFileName = k.(string)
			return false
		})
		ch2 <- store.DeleteFile(ctx, deleteFileName)
		t.Logf("%s deleted", deleteFileName)
	}()
	close(pendingCh)
	// see how long does the write returns
	var err error
	select {
	case <-time.After(1 * time.Second):
		err = errors.New("writing file timeout")
	case err = <-ch2:
		//continue on
	}
	require.Nil(t, err)
	require.Nil(t, <-ch1)
	_, ok := remainFilesMap.Load(deleteFileName)
	require.True(t, ok)
}
