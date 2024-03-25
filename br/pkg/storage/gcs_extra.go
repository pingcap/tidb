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

// Learned from https://github.com/liqiuqing/gcsmpu

package storage

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/go-resty/resty/v2"
	"go.uber.org/atomic"
)

// GCSWriter uses XML multipart upload API to upload a single file.
// https://cloud.google.com/storage/docs/multipart-uploads.
// GCSWriter will attempt to cancel uploads that fail due to an exception.
// If the upload fails in a way that precludes cancellation, such as a
// hardware failure, process termination, or power outage, then the incomplete
// upload may persist indefinitely. To mitigate this, set the
// `AbortIncompleteMultipartUpload` with a nonzero `Age` in bucket lifecycle
// rules, or refer to the XML API documentation linked above to learn more
// about how to list and delete individual downloads.
type GCSWriter struct {
	uploadBase
	mutex       sync.Mutex
	xmlMPUParts []*xmlMPUPart
	wg          sync.WaitGroup
	err         atomic.Error
	chunkSize   int64
	workers     int
	totalSize   int64
	uploadID    string
	chunkCh     chan chunk
	curPart     int
}

// NewGCSWriter returns a GCSWriter which uses GCS multipart upload API behind the scene.
func NewGCSWriter(
	ctx context.Context,
	cli *storage.Client,
	uri string,
	partSize int64,
	parallelCnt int,
	bucketName string,
) (*GCSWriter, error) {
	if partSize < gcsMinimumChunkSize || partSize > gcsMaximumChunkSize {
		return nil, fmt.Errorf(
			"invalid chunk size: %d. Chunk size must be between %d and %d",
			partSize, gcsMinimumChunkSize, gcsMaximumChunkSize,
		)
	}

	w := &GCSWriter{
		uploadBase: uploadBase{
			ctx:             ctx,
			cli:             cli,
			bucket:          bucketName,
			blob:            uri,
			retry:           defaultRetry,
			signedURLExpiry: defaultSignedURLExpiry,
		},
		chunkSize: partSize,
		workers:   parallelCnt,
	}
	if err := w.init(); err != nil {
		return nil, fmt.Errorf("failed to initiate GCSWriter: %w", err)
	}

	return w, nil
}

func (w *GCSWriter) init() error {
	opts := &storage.SignedURLOptions{
		Scheme:          storage.SigningSchemeV4,
		Method:          "POST",
		Expires:         time.Now().Add(w.signedURLExpiry),
		QueryParameters: url.Values{mpuInitiateQuery: []string{""}},
	}
	u, err := w.cli.Bucket(w.bucket).SignedURL(w.blob, opts)
	if err != nil {
		return fmt.Errorf("Bucket(%q).SignedURL: %s", w.bucket, err)
	}

	client := resty.New()
	resp, err := client.R().Post(u)
	if err != nil {
		return fmt.Errorf("POST request failed: %s", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("POST request returned non-OK status: %d", resp.StatusCode())
	}
	body := resp.Body()

	result := InitiateMultipartUploadResult{}
	err = xml.Unmarshal(body, &result)
	if err != nil {
		return fmt.Errorf("failed to unmarshal response body: %s", err)
	}

	uploadID := result.UploadId
	w.uploadID = uploadID
	w.chunkCh = make(chan chunk)
	for i := 0; i < w.workers; i++ {
		w.wg.Add(1)
		go w.readChunk(w.chunkCh)
	}
	w.curPart = 1
	return nil
}

func (w *GCSWriter) readChunk(ch chan chunk) {
	defer w.wg.Done()
	for {
		data, ok := <-ch
		if !ok {
			break
		}

		func() {
			activeUploadWorkerCnt.Add(1)
			defer activeUploadWorkerCnt.Add(-1)

			select {
			case <-w.ctx.Done():
				data.cleanup()
				w.err.CompareAndSwap(nil, w.ctx.Err())
			default:
				part := &xmlMPUPart{
					uploadBase: w.uploadBase,
					uploadID:   w.uploadID,
					buf:        data.buf,
					partNumber: data.num,
				}
				if w.err.Load() == nil {
					if err := part.Upload(); err != nil {
						w.err.Store(err)
					}
				}
				part.buf = nil
				w.appendMPUPart(part)
				data.cleanup()
			}
		}()
	}
}

// Write uploads given bytes as a part to Google Cloud Storage. Write is not
// concurrent safe.
func (w *GCSWriter) Write(p []byte) (n int, err error) {
	if w.curPart > gcsMaximumParts {
		err = fmt.Errorf("exceed maximum parts %d", gcsMaximumParts)
		if w.err.Load() == nil {
			w.err.Store(err)
		}
		return 0, err
	}
	buf := make([]byte, len(p))
	copy(buf, p)
	w.chunkCh <- chunk{
		buf:     buf,
		num:     w.curPart,
		cleanup: func() {},
	}
	w.curPart++
	return len(p), nil
}

// Close finishes the upload.
func (w *GCSWriter) Close() error {
	close(w.chunkCh)
	w.wg.Wait()

	if err := w.err.Load(); err != nil {
		return err
	}

	if len(w.xmlMPUParts) == 0 {
		return nil
	}
	err := w.finalizeXMLMPU()
	if err == nil {
		return nil
	}
	errC := w.cancel()
	if errC != nil {
		return fmt.Errorf("failed to finalize multipart upload: %s, Failed to cancel multipart upload: %s", err, errC)
	}
	return fmt.Errorf("failed to finalize multipart upload: %s", err)
}

const (
	mpuInitiateQuery   = "uploads"
	mpuPartNumberQuery = "partNumber"
	mpuUploadIDQuery   = "uploadId"
)

type uploadBase struct {
	cli             *storage.Client
	ctx             context.Context
	bucket          string
	blob            string
	retry           int
	signedURLExpiry time.Duration
}

const (
	defaultRetry           = 3
	defaultSignedURLExpiry = 6 * time.Hour

	gcsMinimumChunkSize = 5 * 1024 * 1024        // 5 MB
	gcsMaximumChunkSize = 5 * 1024 * 1024 * 1024 // 5 GB
	gcsMaximumParts     = 10000
)

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Text     string   `xml:",chardata"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type Part struct {
	Text       string `xml:",chardata"`
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type CompleteMultipartUpload struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Text    string   `xml:",chardata"`
	Parts   []Part   `xml:"Part"`
}

func (w *GCSWriter) finalizeXMLMPU() error {
	finalXMLRoot := CompleteMultipartUpload{
		Parts: make([]Part, 0, len(w.xmlMPUParts)),
	}
	slices.SortFunc(w.xmlMPUParts, func(a, b *xmlMPUPart) int {
		return a.partNumber - b.partNumber
	})
	for _, part := range w.xmlMPUParts {
		part := Part{
			PartNumber: part.partNumber,
			ETag:       part.etag,
		}
		finalXMLRoot.Parts = append(finalXMLRoot.Parts, part)
	}

	xmlBytes, err := xml.Marshal(finalXMLRoot)
	if err != nil {
		return fmt.Errorf("failed to encode XML: %v", err)
	}

	opts := &storage.SignedURLOptions{
		Scheme:          storage.SigningSchemeV4,
		Method:          "POST",
		Expires:         time.Now().Add(w.signedURLExpiry),
		QueryParameters: url.Values{mpuUploadIDQuery: []string{w.uploadID}},
	}
	u, err := w.cli.Bucket(w.bucket).SignedURL(w.blob, opts)
	if err != nil {
		return fmt.Errorf("Bucket(%q).SignedURL: %s", w.bucket, err)
	}

	client := resty.New()
	resp, err := client.R().SetBody(xmlBytes).Post(u)
	if err != nil {
		return fmt.Errorf("POST request failed: %s", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("POST request returned non-OK status: %d, body: %s", resp.StatusCode(), resp.String())
	}
	return nil
}

type chunk struct {
	buf     []byte
	num     int
	cleanup func()
}

func (w *GCSWriter) appendMPUPart(part *xmlMPUPart) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.xmlMPUParts = append(w.xmlMPUParts, part)
}

func (w *GCSWriter) cancel() error {
	opts := &storage.SignedURLOptions{
		Scheme:          storage.SigningSchemeV4,
		Method:          "DELETE",
		Expires:         time.Now().Add(w.signedURLExpiry),
		QueryParameters: url.Values{mpuUploadIDQuery: []string{w.uploadID}},
	}
	u, err := w.cli.Bucket(w.bucket).SignedURL(w.blob, opts)
	if err != nil {
		return fmt.Errorf("Bucket(%q).SignedURL: %s", w.bucket, err)
	}

	client := resty.New()
	resp, err := client.R().Delete(u)
	if err != nil {
		return fmt.Errorf("DELETE request failed: %s", err)
	}

	if resp.StatusCode() != http.StatusNoContent {
		return fmt.Errorf("DELETE request returned non-204 status: %d", resp.StatusCode())
	}

	return nil
}

type xmlMPUPart struct {
	uploadBase
	buf        []byte
	uploadID   string
	partNumber int
	etag       string
}

func (p *xmlMPUPart) Clone() *xmlMPUPart {
	return &xmlMPUPart{
		uploadBase: p.uploadBase,
		uploadID:   p.uploadID,
		buf:        p.buf,
		partNumber: p.partNumber,
	}
}

func (p *xmlMPUPart) Upload() error {
	var err error
	for i := 0; i < p.retry; i++ {
		err = p.upload()
		if err == nil {
			return nil
		}
	}

	return fmt.Errorf("failed to upload part %d: %w", p.partNumber, err)
}

func (p *xmlMPUPart) upload() error {
	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "PUT",
		Expires: time.Now().Add(p.signedURLExpiry),
		QueryParameters: url.Values{
			mpuUploadIDQuery:   []string{p.uploadID},
			mpuPartNumberQuery: []string{strconv.Itoa(p.partNumber)},
		},
	}

	u, err := p.cli.Bucket(p.bucket).SignedURL(p.blob, opts)
	if err != nil {
		return fmt.Errorf("Bucket(%q).SignedURL: %s", p.bucket, err)
	}

	req, err := http.NewRequest("PUT", u, bytes.NewReader(p.buf))
	if err != nil {
		return fmt.Errorf("PUT request failed: %s", err)
	}
	req = req.WithContext(p.ctx)

	client := &http.Client{
		Transport: createTransport(nil),
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("PUT request failed: %s", err)
	}
	defer resp.Body.Close()

	p.etag = resp.Header.Get("ETag")

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT request returned non-OK status: %d", resp.StatusCode)
	}
	return nil
}

func createTransport(localAddr net.Addr) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	if localAddr != nil {
		dialer.LocalAddr = localAddr
	}
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   runtime.GOMAXPROCS(0) + 1,
	}
}
