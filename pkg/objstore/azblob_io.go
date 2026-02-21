// Copyright 2021 PingCAP, Inc.
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

package objstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)
type azblobObjectReader struct {
	blobClient *blockblob.Client

	pos       int64
	endPos    int64
	totalSize int64

	ctx context.Context

	cpkInfo *blob.CPKInfo
	// opened lazily
	reader io.ReadCloser
}

// Read implement the io.Reader interface.
func (r *azblobObjectReader) Read(p []byte) (n int, err error) {
	maxCnt := min(r.endPos-r.pos, int64(len(p)))
	if maxCnt == 0 {
		return 0, io.EOF
	}
	if r.reader == nil {
		if err2 := r.reopenReader(); err2 != nil {
			return 0, err2
		}
	}
	buf := p[:maxCnt]
	n, err = r.reader.Read(buf)
	if err != nil && err != io.EOF {
		return 0, errors.Annotatef(err, "Failed to read data from azure blob response, data info: pos='%d', count='%d'", r.pos, maxCnt)
	}
	r.pos += int64(n)
	return n, nil
}

// Close implement the io.Closer interface.
func (r *azblobObjectReader) Close() error {
	if r.reader != nil {
		err := errors.Trace(r.reader.Close())
		r.reader = nil
		return err
	}
	return nil
}

func (r *azblobObjectReader) Seek(offset int64, whence int) (int64, error) {
	var realOffset int64
	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return 0, errors.Annotatef(berrors.ErrInvalidArgument, "Seek: offset '%v' out of range.", offset)
		}
		realOffset = offset
	case io.SeekCurrent:
		realOffset = r.pos + offset
		if r.pos < 0 && realOffset >= 0 {
			return 0, errors.Annotatef(berrors.ErrInvalidArgument, "Seek: offset '%v' out of range. current pos is '%v'.", offset, r.pos)
		}
	case io.SeekEnd:
		if offset > 0 {
			return 0, errors.Annotatef(berrors.ErrInvalidArgument, "Seek: offset '%v' should be negative.", offset)
		}
		realOffset = offset + r.totalSize
	default:
		return 0, errors.Annotatef(berrors.ErrStorageUnknown, "Seek: invalid whence '%d'", whence)
	}

	if realOffset < 0 || realOffset > r.totalSize {
		return 0, errors.Annotatef(berrors.ErrInvalidArgument, "Seek: offset is %d, but length of content is only %d", realOffset, r.totalSize)
	}
	if realOffset == r.pos {
		return r.pos, nil
	}
	r.pos = realOffset
	// azblob reader can only read forward, so we need to reopen the reader
	if err := r.reopenReader(); err != nil {
		return 0, err
	}
	return r.pos, nil
}

func (r *azblobObjectReader) reopenReader() error {
	if r.reader != nil {
		err := errors.Trace(r.reader.Close())
		if err != nil {
			log.Warn("failed to close azblob reader", zap.Error(err))
		}
	}

	if r.pos == r.totalSize {
		r.reader = io.NopCloser(bytes.NewReader(nil))
		return nil
	}

	resp, err := r.blobClient.DownloadStream(r.ctx, &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{
			Offset: r.pos,
		},
		CPKInfo: r.cpkInfo,
	})
	if err != nil {
		return errors.Annotatef(err, "Failed to read data from azure blob, data info: pos='%d'", r.pos)
	}
	body := resp.NewRetryReader(r.ctx, &blob.RetryReaderOptions{
		MaxRetries: azblobRetryTimes,
	})
	r.reader = body
	return nil
}

func (r *azblobObjectReader) GetFileSize() (int64, error) {
	return r.totalSize, nil
}

type nopCloser struct {
	io.ReadSeeker
}

func newNopCloser(r io.ReadSeeker) nopCloser {
	return nopCloser{r}
}

func (nopCloser) Close() error {
	return nil
}

type azblobUploader struct {
	blobClient *blockblob.Client

	blockIDList []string

	accessTier blob.AccessTier

	cpkScope *blob.CPKScopeInfo
	cpkInfo  *blob.CPKInfo
}

func (u *azblobUploader) Write(ctx context.Context, data []byte) (int, error) {
	generatedUUID, err := uuid.NewUUID()
	if err != nil {
		return 0, errors.Annotate(err, "Fail to generate uuid")
	}
	blockID := base64.StdEncoding.EncodeToString([]byte(generatedUUID.String()))

	_, err = u.blobClient.StageBlock(ctx, blockID, newNopCloser(bytes.NewReader(data)), &blockblob.StageBlockOptions{
		CPKScopeInfo: u.cpkScope,
		CPKInfo:      u.cpkInfo,
	})
	if err != nil {
		return 0, errors.Annotate(err, "Failed to upload block to azure blob")
	}
	u.blockIDList = append(u.blockIDList, blockID)

	return len(data), nil
}

func (u *azblobUploader) Close(ctx context.Context) error {
	// the encryption scope and the access tier can not be both in the HTTP headers
	options := &blockblob.CommitBlockListOptions{
		CPKScopeInfo: u.cpkScope,
		CPKInfo:      u.cpkInfo,
	}

	if len(u.accessTier) > 0 {
		options.Tier = &u.accessTier
	}
	_, err := u.blobClient.CommitBlockList(ctx, u.blockIDList, options)
	return errors.Trace(err)
}
