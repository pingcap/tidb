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
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMapStoreBasic(t *testing.T) {
	store := NewMapStorage()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	w, err := store.Create(ctx, "/hello.txt")
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
	r, err := store.Open(ctx, "/hello.txt")
	require.Nil(t, err)
	r2, err := store.Open(ctx, "/hello.txt")
	require.Nil(t, err)
	fileContent, err = ioutil.ReadAll(r)
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
	fileContent, err = ioutil.ReadAll(r2)
	require.Nil(t, err)
	require.True(t, bytes.Equal([]byte(" world 3"), fileContent))

	// rename the file
	require.Nil(t, store.WriteFile(ctx, "/hello.txt", []byte("hello world 3")))
	require.Nil(t, store.WriteFile(ctx, "/hello2.txt", []byte("hello world 2")))
	require.NotNil(t, store.Rename(ctx, "/NOT_EXIST.txt", "/NEW_FILE.txt"))
	require.NotNil(t, store.Rename(ctx, "/hello.txt", "/hello2.txt"))
	require.Nil(t, store.Rename(ctx, "/hello.txt", "/hello3.txt"))

}

type iterFileInfo struct {
	Name    string
	Size    int64
	Content []byte
}

func TestMapStoreWalkDir(t *testing.T) {
	store := NewMapStorage()
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
