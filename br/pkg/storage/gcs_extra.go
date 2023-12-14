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
type GCSWriter struct {
	UploadBase
	mutex       sync.Mutex
	XMLMPUParts []*XMLMPUPart
	wg          sync.WaitGroup
	err         atomic.Error
	chunkSize   int64
	workers     int
	totalSize   int64
	// only available for input reader which implements io.Seeker
	readerPos int64
	uploadID  string
	chunkCh   chan chunk
	curPart   int
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
	if partSize < MinimumChunkSize || partSize > MaximumChunkSize {
		return nil, fmt.Errorf(
			"invalid chunk size: %d. Chunk size must be between %d and %d",
			partSize, MinimumChunkSize, MaximumChunkSize,
		)
	}

	w := &GCSWriter{
		UploadBase: UploadBase{
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
		QueryParameters: url.Values{MPUInitiateQuery: []string{""}},
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
	w.chunkCh = make(chan chunk, w.workers)
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

		select {
		case <-w.ctx.Done():
			data.cleanup()
			return
		default:
			part := &XMLMPUPart{
				UploadBase: w.UploadBase,
				UploadID:   w.uploadID,
				buf:        data.buf,
				PartNumber: data.num,
				Checksum:   "",
			}
			if w.err.Load() == nil {
				if err := part.Upload(); err != nil {
					w.err.Store(err)
				}
			}
			w.appendMPUPart(part)
			data.cleanup()
		}
	}
}

// Write uploads given bytes as a part to Google Cloud Storage. Write is not
// concurrent safe.
func (w *GCSWriter) Write(p []byte) (n int, err error) {
	if w.curPart > MaximumParts {
		err = fmt.Errorf("exceed maximum parts %d", MaximumParts)
		if w.err.Load() == nil {
			w.err.Store(err)
		}
		return 0, err
	}
	w.chunkCh <- chunk{
		buf:     p,
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

	err := w.finalizeXMLMPU()
	if err == nil {
		return nil
	}
	errC := w.Cancel()
	if errC != nil {
		return fmt.Errorf("failed to finalize multipart upload: %s, Failed to cancel multipart upload: %s", err, errC)
	}
	return fmt.Errorf("failed to finalize multipart upload: %s", err)
}

const (
	MPUInitiateQuery   = "uploads"
	MPUPartNumberQuery = "partNumber"
	MPUUploadIDQuery   = "uploadId"
)

type UploadBase struct {
	cli             *storage.Client
	ctx             context.Context
	bucket          string
	blob            string
	retry           int
	signedURLExpiry time.Duration
}

const (
	defaultRetry           = 3
	defaultSignedURLExpiry = 1 * time.Hour

	MinimumChunkSize = 5 * 1024 * 1024        // 5 MB
	MaximumChunkSize = 5 * 1024 * 1024 * 1024 // 5 GB
	MaximumParts     = 10000
)

// NewXMLMPU uploads a single file in chunks, concurrently.
// This function uses the XML MPU API to initialize an upload and upload a
// file in chunks, concurrently with a worker pool.
// https://cloud.google.com/storage/quotas#requests
//
// The XML MPU API is significantly different from other uploads; please review
// the documentation at `https://cloud.google.com/storage/docs/multipart-uploads`
// before using this feature.
//
// The library will attempt to cancel uploads that fail due to an exception.
// If the upload fails in a way that precludes cancellation, such as a
// hardware failure, process termination, or power outage, then the incomplete
// upload may persist indefinitely. To mitigate this, set the
// `AbortIncompleteMultipartUpload` with a nonzero `Age` in bucket lifecycle
// rules, or refer to the XML API documentation linked above to learn more
// about how to list and delete individual downloads.

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

type FinalizeXMLMPUResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult" json:"-"`
	Text     string   `xml:",chardata" json:"-"`
	Xmlns    string   `xml:"xmlns,attr" json:"-"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

func (w *GCSWriter) finalizeXMLMPU() error {
	finalXMLRoot := CompleteMultipartUpload{
		Parts: []Part{},
	}
	slices.SortFunc(w.XMLMPUParts, func(a, b *XMLMPUPart) int {
		return a.PartNumber - b.PartNumber
	})
	for _, part := range w.XMLMPUParts {
		part := Part{
			PartNumber: part.PartNumber,
			ETag:       part.etag,
		}
		finalXMLRoot.Parts = append(finalXMLRoot.Parts, part)
	}

	xmlBytes, err := xml.Marshal(finalXMLRoot)
	if err != nil {
		return fmt.Errorf("Failed to encode XML: %v\n", err)
	}

	opts := &storage.SignedURLOptions{
		Scheme:          storage.SigningSchemeV4,
		Method:          "POST",
		Expires:         time.Now().Add(w.signedURLExpiry),
		QueryParameters: url.Values{MPUUploadIDQuery: []string{w.uploadID}},
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
		return fmt.Errorf("POST request returned non-OK status: %d", resp.StatusCode())
	}
	return nil
}

type chunk struct {
	buf     []byte
	num     int
	cleanup func()
}

func (w *GCSWriter) appendMPUPart(part *XMLMPUPart) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.XMLMPUParts = append(w.XMLMPUParts, part)
}

func (w *GCSWriter) Cancel() error {
	opts := &storage.SignedURLOptions{
		Scheme:          storage.SigningSchemeV4,
		Method:          "DELETE",
		Expires:         time.Now().Add(w.signedURLExpiry),
		QueryParameters: url.Values{MPUUploadIDQuery: []string{w.uploadID}},
	}
	u, err := w.cli.Bucket(w.bucket).SignedURL(w.blob, opts)
	if err != nil {
		return fmt.Errorf("Bucket(%q).SignedURL: %s", w.bucket, err)
	}

	client := resty.New()
	resp, err := client.R().Delete(u)
	if err != nil {
		return fmt.Errorf("POST request failed: %s", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("POST request returned non-OK status: %d", resp.StatusCode())
	}

	return nil
}

type XMLMPUPart struct {
	UploadBase
	buf        []byte
	UploadID   string
	PartNumber int
	Checksum   string
	etag       string
	finished   bool
}

func (p *XMLMPUPart) Clone() *XMLMPUPart {
	return &XMLMPUPart{
		UploadBase: p.UploadBase,
		UploadID:   p.UploadID,
		buf:        p.buf,
		PartNumber: p.PartNumber,
		Checksum:   p.Checksum,
	}
}

func (p *XMLMPUPart) Upload() error {
	err := p.upload()
	if err == nil {
		return nil
	}

	for i := 0; i < p.retry; i++ {
		err := p.upload()
		if err == nil {
			return nil
		}
	}

	return fmt.Errorf("failed to upload part %d", p.PartNumber)
}

func (p *XMLMPUPart) upload() error {
	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "PUT",
		Expires: time.Now().Add(p.signedURLExpiry),
		QueryParameters: url.Values{
			MPUUploadIDQuery:   []string{p.UploadID},
			MPUPartNumberQuery: []string{strconv.Itoa(p.PartNumber)},
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
	p.finished = true

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
