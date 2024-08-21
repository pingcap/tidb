// Copyright 2023 PingCAP, Inc.
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

package external

import (
	"context"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

// mockExtStore is only used for test.
type mockExtStore struct {
	src []byte
	idx uint64
}

func (s *mockExtStore) Read(p []byte) (n int, err error) {
	// Read from src to p.
	if s.idx >= uint64(len(s.src)) {
		return 0, io.EOF
	}
	n = copy(p, s.src[s.idx:])
	s.idx += uint64(n)
	return n, nil
}

func (s *mockExtStore) Seek(_ int64, _ int) (int64, error) {
	return 0, errors.Errorf("unsupported operation")
}

func (*mockExtStore) Close() error {
	return nil
}

func (s *mockExtStore) GetFileSize() (int64, error) {
	return int64(len(s.src)), nil
}

func TestByteReader(t *testing.T) {
	st, clean := NewS3WithBucketAndPrefix(t, "test", "testprefix")
	defer clean()

	// Prepare
	err := st.WriteFile(context.Background(), "testfile", []byte("abcde"))
	require.NoError(t, err)

	newRsc := func() storage.ExternalFileReader {
		rsc, err := st.Open(context.Background(), "testfile", nil)
		require.NoError(t, err)
		return rsc
	}

	// Test basic next() usage.
	br, err := newByteReader(context.Background(), newRsc(), 3)
	require.NoError(t, err)
	n, bs := br.next(1)
	require.Equal(t, 1, n)
	require.Equal(t, [][]byte{{'a'}}, bs)
	n, bs = br.next(2)
	require.Equal(t, 2, n)
	require.Equal(t, [][]byte{{'b', 'c'}}, bs)
	require.NoError(t, br.Close())

	// Test basic readNBytes() usage.
	br, err = newByteReader(context.Background(), newRsc(), 3)
	require.NoError(t, err)
	x, err := br.readNBytes(2)
	require.NoError(t, err)
	require.Equal(t, 2, len(x))
	require.Equal(t, byte('a'), x[0])
	require.Equal(t, byte('b'), x[1])
	require.NoError(t, br.Close())

	br, err = newByteReader(context.Background(), newRsc(), 3)
	require.NoError(t, err)
	x, err = br.readNBytes(5) // Read all the data.
	require.NoError(t, err)
	require.Equal(t, 5, len(x))
	require.Equal(t, byte('e'), x[4])
	_, err = br.readNBytes(1) // EOF
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, br.Close())

	br, err = newByteReader(context.Background(), newRsc(), 3)
	require.NoError(t, err)
	_, err = br.readNBytes(7) // EOF
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	err = st.WriteFile(context.Background(), "testfile", []byte("abcdef"))
	require.NoError(t, err)

	ms := &mockExtStore{src: []byte("abcdef")}
	br, err = newByteReader(context.Background(), ms, 2)
	require.NoError(t, err)
	x, err = br.readNBytes(3)
	require.NoError(t, err)
	// Pollute mockExtStore to verify if the slice is not affected.
	copy(ms.src, "xyz")
	require.Equal(t, 3, len(x))
	require.Equal(t, byte('c'), x[2])
	require.NoError(t, br.Close())

	ms = &mockExtStore{src: []byte("abcdef")}
	br, err = newByteReader(context.Background(), ms, 2)
	require.NoError(t, err)
	x, err = br.readNBytes(2)
	require.NoError(t, err)
	// Pollute mockExtStore to verify if the slice is not affected.
	copy(ms.src, "xyz")
	require.Equal(t, 2, len(x))
	require.Equal(t, byte('b'), x[1])
	require.NoError(t, br.Close())
}

func TestByteReaderAuxBuf(t *testing.T) {
	ms := &mockExtStore{src: []byte("0123456789")}
	br, err := newByteReader(context.Background(), ms, 1)
	require.NoError(t, err)
	y1, err := br.readNBytes(1)
	require.NoError(t, err)
	require.Equal(t, []byte("0"), y1)
	y2, err := br.readNBytes(2)
	require.NoError(t, err)
	require.Equal(t, []byte("12"), y2)

	y3, err := br.readNBytes(1)
	require.NoError(t, err)
	require.Equal(t, []byte("3"), y3)
	y4, err := br.readNBytes(2)
	require.NoError(t, err)
	require.Equal(t, []byte("45"), y4)
}

func TestUnexpectedEOF(t *testing.T) {
	st, clean := NewS3WithBucketAndPrefix(t, "test", "testprefix")
	defer func() {
		clean()
	}()

	// Prepare
	err := st.WriteFile(context.Background(), "testfile", []byte("0123456789"))
	require.NoError(t, err)

	newRsc := func() storage.ExternalFileReader {
		rsc, err := st.Open(context.Background(), "testfile", nil)
		require.NoError(t, err)
		return rsc
	}

	br, err := newByteReader(context.Background(), newRsc(), 3)
	require.NoError(t, err)
	_, err = br.readNBytes(100)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	br, err = newByteReader(context.Background(), newRsc(), 3)
	require.NoError(t, err)
	_, err = br.readNBytes(100)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestEmptyContent(t *testing.T) {
	ms := &mockExtStore{src: []byte{}}
	_, err := newByteReader(context.Background(), ms, 100)
	require.Equal(t, io.EOF, err)

	st, clean := NewS3WithBucketAndPrefix(t, "test", "testprefix")
	defer clean()

	// Prepare
	err = st.WriteFile(context.Background(), "testfile", []byte(""))
	require.NoError(t, err)

	newRsc := func() storage.ExternalFileReader {
		rsc, err := st.Open(context.Background(), "testfile", nil)
		require.NoError(t, err)
		return rsc
	}
	_, err = newByteReader(context.Background(), newRsc(), 100)
	require.Equal(t, io.EOF, err)
}

func TestSwitchMode(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	st := storage.NewMemStorage()
	// Prepare
	ctx := context.Background()
	writer := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		BuildOneFile(st, "/test", "0")

	err := writer.Init(ctx, 5*1024*1024)
	require.NoError(t, err)

	kvCnt := 1000000
	kvs := make([]common.KvPair, kvCnt)
	for i := 0; i < kvCnt; i++ {
		randLen := rand.Intn(10) + 1
		kvs[i].Key = make([]byte, randLen)
		_, err := rand.Read(kvs[i].Key)
		require.NoError(t, err)
		randLen = rand.Intn(10) + 1
		kvs[i].Val = make([]byte, randLen)
		_, err = rand.Read(kvs[i].Val)
		require.NoError(t, err)
	}

	for _, item := range kvs {
		err := writer.WriteRow(ctx, item.Key, item.Val)
		require.NoError(t, err)
	}

	err = writer.Close(ctx)
	require.NoError(t, err)
	pool := membuf.NewPool()
	ConcurrentReaderBufferSizePerConc = rand.Intn(100) + 1
	kvReader, err := newKVReader(context.Background(), "/test/0/one-file", st, 0, 64*1024)
	require.NoError(t, err)
	kvReader.byteReader.enableConcurrentRead(st, "/test/0/one-file", 100, ConcurrentReaderBufferSizePerConc, pool.NewBuffer())
	modeUseCon := false
	i := 0
	for {
		if rand.Intn(5) == 0 {
			if modeUseCon {
				kvReader.byteReader.switchConcurrentMode(false)
				modeUseCon = false
			} else {
				kvReader.byteReader.switchConcurrentMode(true)
				modeUseCon = true
			}
		}
		key, val, err := kvReader.nextKV()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, kvs[i].Key, key)
		require.Equal(t, kvs[i].Val, val)
		i++
	}
}

// NewS3WithBucketAndPrefix creates a new S3Storage for testing.
func NewS3WithBucketAndPrefix(t *testing.T, bucketName, prefixName string) (*storage.S3Storage, func()) {
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	err := backend.CreateBucket("test")
	require.NoError(t, err)

	config := aws.NewConfig()
	config.WithEndpoint(ts.URL)
	config.WithRegion("region")
	config.WithCredentials(credentials.NewStaticCredentials("dummy-access", "dummy-secret", ""))
	config.WithS3ForcePathStyle(true) // Removes need for subdomain
	svc := s3.New(session.New(), config)

	st := storage.NewS3StorageForTest(svc, &backuppb.S3{
		Region:       "region",
		Bucket:       bucketName,
		Prefix:       prefixName,
		Acl:          "acl",
		Sse:          "sse",
		StorageClass: "sc",
	})
	return st, ts.Close
}
