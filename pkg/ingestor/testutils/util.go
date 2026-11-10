// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutils

import (
	"context"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/atomic"
)

// TrackOpenMemStorage is a wrapper of MemStorage that tracks the number of opened files.
type TrackOpenMemStorage struct {
	*objstore.MemStorage
	Opened      atomic.Int32
	TotalOpened atomic.Int32
}

// Open a file.
func (s *TrackOpenMemStorage) Open(ctx context.Context, path string, opt *storeapi.ReaderOption) (objectio.Reader, error) {
	s.Opened.Inc()
	s.TotalOpened.Inc()
	r, err := s.MemStorage.Open(ctx, path, opt)
	if err != nil {
		s.Opened.Dec()
		return nil, err
	}
	return &TrackOpenFileReader{r, s}, nil
}

// TrackOpenFileReader is a wrapper of objectio.Reader.
type TrackOpenFileReader struct {
	objectio.Reader
	store *TrackOpenMemStorage
}

// Close the reader.
func (r *TrackOpenFileReader) Close() error {
	err := r.Reader.Close()
	if err != nil {
		return err
	}
	r.store.Opened.Dec()
	return nil
}
