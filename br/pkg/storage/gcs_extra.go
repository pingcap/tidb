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

package storage

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/go-resty/resty/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	uploadPartTryCnt   = 3 // if uploading a part fails, we should try 3 times in total.
	mpuInitiateQuery   = "uploads"
	mpuPartNumberQuery = "partNumber"
	mpuUploadIDQuery   = "uploadId"

	defaultSignedURLExpiry = 15 * time.Minute
)

// InitiateMultipartUploadResult is the result of initiating a multipart upload.
type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Text     string   `xml:",chardata"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

// GCSWriter is GCSWriter following GCS multipart upload protocol.
type GCSWriter struct {
	ctx context.Context

	objURI     string
	bucketName string
	uploadID   string

	partSize          int   // in bytes
	parallelCnt       int   // number of workers
	writtenTotalBytes int64 // the total number of bytes uploaded by workers so far

	workers       []*uploadWorker
	currentWorker *uploadWorker // the worker who is buffering the data. Once the buffer is full,
	// the data will be uploaded as a part.
	idleWorkers chan *uploadWorker
	cli         *storage.Client
}

func InitAndGetUploadID(cli *storage.Client, bucket, uri string) (string, error) {
	opts := &storage.SignedURLOptions{
		Scheme:          storage.SigningSchemeV4,
		Method:          "POST",
		Expires:         time.Now().Add(defaultSignedURLExpiry),
		QueryParameters: url.Values{mpuInitiateQuery: []string{""}},
	}
	u, err := cli.Bucket(bucket).SignedURL(uri, opts)
	if err != nil {
		return "", errors.Errorf("Bucket(%q).SignedURL: %s", bucket, err.Error())
	}

	client := resty.New()
	resp, err := client.R().Post(u)
	if err != nil {
		return "", errors.Errorf("POST request failed: %s", err.Error())
	}

	if resp.StatusCode() != http.StatusOK {
		return "", errors.Errorf("POST request returned non-OK status: %d", resp.StatusCode())
	}
	body := resp.Body()

	result := InitiateMultipartUploadResult{}
	err = xml.Unmarshal(body, &result)
	if err != nil {
		return "", errors.Errorf("failed to unmarshal response body: %s", err.Error())
	}

	return result.UploadId, nil
}

// NewGCSWriter returns a GCSWriter which uses GCS multipart upload API behind the scene.
func NewGCSWriter(ctx context.Context, cli *storage.Client, uri string, partSize int, parallelCnt int, bucketName string) (*GCSWriter, error) {
	uploadID, err := InitAndGetUploadID(cli, bucketName, uri)
	if err != nil {
		return nil, errors.Errorf("Failed to initiate and get upload ID: %s", err.Error())
	}

	w := &GCSWriter{
		ctx:           ctx,
		objURI:        uri,
		bucketName:    bucketName,
		partSize:      partSize,
		parallelCnt:   parallelCnt,
		workers:       make([]*uploadWorker, parallelCnt),
		currentWorker: nil,
		idleWorkers:   make(chan *uploadWorker, parallelCnt),
		uploadID:      uploadID,
		cli:           cli,
	}

	for i := 0; i < parallelCnt; i++ {
		w.workers[i] = newUploadWorker(cli, uri, bucketName, w.uploadID, i, partSize, w.idleWorkers)
		w.idleWorkers <- w.workers[i]
	}

	return w, nil
}

// Write transfer data to GCS by using multipart upload API.
func (w *GCSWriter) Write(p []byte) (n int, err error) {
	i := 0
	for i < len(p) {
		if w.currentWorker == nil {
			// pick a worker from the idle pool
			w.currentWorker = <-w.idleWorkers
		}

		n := w.currentWorker.bufferData(w.partNum(), p[i:]) // the part number doesn't change while buffering
		i += n
		w.writtenTotalBytes += int64(n)

		if w.currentWorker.full() {
			err := w.currentWorker.uploadPartAsync()
			if err != nil {
				// The TTL for a uploaded but not completed part is 7 days: https://screenshot.googleplex.com/A6wGvcx5hYRhUd7
				return i, err
			}
			w.currentWorker = nil // this worker is uploading a part, will find another idle worker.
		}
	}
	return i, nil
}

// Close waits for the completion of all transfer and generate the final GCS object.
func (w *GCSWriter) Close() error {
	if w.currentWorker != nil {
		// uploads the last part, which is not a full part
		err := w.currentWorker.uploadPartAsync()
		if err != nil {
			return err
		}
	}

	for i := 0; i < w.parallelCnt; i++ {
		<-w.idleWorkers
	}

	// merge all parts
	parts := make(map[int]objectPart)
	for i := 0; i < w.parallelCnt; i++ {
		for k, v := range w.workers[i].parts {
			parts[k] = v
		}
	}

	return w.completeMultipartUpload(parts)
}

func (w *GCSWriter) partNum() int {
	return int((w.writtenTotalBytes / int64(w.partSize)) + 1) // Starts from 1, instead of 0
}

func (w *GCSWriter) completeMultipartUpload(parts map[int]objectPart) error {
	bodyXML, err := buildCompleteMultipartUploadXML(parts)
	if err != nil {
		return errors.Errorf("failed to build complete multipart upload XML for %v, err: %s", w.objURI, err.Error())
	}

	values := url.Values{}
	values.Add(mpuUploadIDQuery, w.uploadID)
	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "POST",
		Expires: time.Now().Add(defaultSignedURLExpiry),
		//ContentType:     "application/xml",
		QueryParameters: values,
	}
	u, err := w.cli.Bucket(w.bucketName).SignedURL(w.objURI, opts)
	if err != nil {
		return errors.Errorf("Bucket(%q).SignedURL: %s", w.bucketName, err.Error())
	}

	client := resty.New()
	resp, err := client.R().SetBody(bodyXML).Post(u)
	if err != nil {
		return errors.Errorf("POST request failed: %s", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return errors.Errorf("POST request returned non-OK status: %d", resp.StatusCode())
	}

	return nil
}

func buildCompleteMultipartUploadXML(parts map[int]objectPart) (string, error) {
	xmlStr := strings.Builder{}
	encoder := xml.NewEncoder(&xmlStr)
	encoder.Indent("", "  ") // Indent with 2 spaces.

	upload := struct {
		XMLName xml.Name     `xml:"CompleteMultipartUpload"`
		Parts   []objectPart `xml:"Part"`
	}{}
	// Order parts in the XML request.
	upload.Parts = make([]objectPart, 0, len(parts))
	for partNum := 1; partNum <= len(parts); partNum++ {
		part, ok := parts[partNum]
		if !ok {
			return "", errors.Errorf("part %v not contained in parts", partNum)
		}
		upload.Parts = append(upload.Parts, part)
	}

	if err := encoder.Encode(upload); err != nil {
		return "", err
	}
	return xmlStr.String(), nil
}

type objectPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type uploadWorker struct {
	cli   *storage.Client
	uri   string
	index int // the index of the worker

	bucketName, uploadID string

	buffer            []byte // buffer capacity is equal to partSize
	offset            int
	partSize          int // 5 MiB <= partSize <= 5 GiB
	currentPartNumber int
	parts             map[int]objectPart

	idleWorkerPool chan *uploadWorker

	err    error // the error got from the last upload, will retry this part
	tryCnt int
}

func newUploadWorker(cli *storage.Client, uri string, bucketName, uploadID string, index, partSize int, idleWorkerPool chan *uploadWorker) *uploadWorker {
	return &uploadWorker{
		uri:            uri,
		cli:            cli,
		index:          index,
		bucketName:     bucketName,
		uploadID:       uploadID,
		buffer:         make([]byte, partSize),
		partSize:       partSize,
		parts:          make(map[int]objectPart),
		idleWorkerPool: idleWorkerPool,
	}
}

func (uw *uploadWorker) bufferData(partNumber int, p []byte) int {
	uw.currentPartNumber = partNumber
	n := copy(uw.buffer[uw.offset:], p)
	uw.offset += n
	return n
}

func (uw *uploadWorker) full() bool {
	return uw.offset >= uw.partSize // if true, this worker is ready to upload the full part
}

func (uw *uploadWorker) uploadPartAsync() error {
	if uw.err != nil && uw.tryCnt >= uploadPartTryCnt {
		return errors.Errorf("failed to upload part %v too many times, err: %s", uw.index, uw.err.Error())
	}
	go func() {
		err := uw.uploadPart(uw.currentPartNumber, uw.buffer[:uw.offset]) // timeout in few seconds
		if err != nil {
			uw.err = err
			uw.tryCnt++
			log.Warn("upload worker failed to upload part for object", zap.Int("worker", uw.index), zap.Int("part number", uw.currentPartNumber), zap.String("uri", uw.uri), zap.Int("retry count", uw.tryCnt), zap.Error(err))
		} else {
			// Upload succeeded. Reset
			uw.offset = 0
			uw.err = nil
			uw.currentPartNumber = -1
			uw.tryCnt = 0
		}
		uw.idleWorkerPool <- uw
	}()
	return nil
}

// UploadPart upload a part.
func (uw *uploadWorker) uploadPart(partNumber int, data []byte) error {
	if partNumber < 1 {
		return fmt.Errorf("invalid partNumber: %v", partNumber)
	}

	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "PUT",
		Expires: time.Now().Add(defaultSignedURLExpiry),
		QueryParameters: url.Values{
			mpuUploadIDQuery:   []string{uw.uploadID},
			mpuPartNumberQuery: []string{strconv.Itoa(partNumber)},
		},
	}

	u, err := uw.cli.Bucket(uw.bucketName).SignedURL(uw.uri, opts)
	if err != nil {
		return errors.Errorf("Bucket(%q).SignedURL: %s", uw.bucketName, err.Error())
	}

	client := resty.New()
	resp, err := client.R().SetBody(bytes.NewReader(data)).Put(u) // Set payload as request body
	if err != nil {
		return errors.Errorf("PUT request failed: %s", err.Error())
	}

	etag := resp.Header().Get("ETag")

	if resp.StatusCode() != http.StatusOK {
		return errors.Errorf("PUT request returned non-OK status: %d", resp.StatusCode())
	}

	uw.parts[partNumber] = objectPart{PartNumber: partNumber, ETag: etag}
	return nil
}
