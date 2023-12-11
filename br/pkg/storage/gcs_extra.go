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
	"github.com/pingcap/log"
)

const (
	uploadPartTryCnt   = 3 // if uploading a part fails, we should try 3 times in total.
	MPUInitiateQuery   = "uploads"
	MPUPartNumberQuery = "partNumber"
	MPUUploadIDQuery   = "uploadId"

	defaultChunkSize       = 5 * 1024 * 1024 // 5 MB
	defaultRetry           = 3               // 建议默认3~5次，不要太大，否则将可能会导致成为僵尸任务。
	defaultSignedURLExpiry = 15 * time.Minute
)

type XMLMPU struct {
	cli             *storage.Client
	bucket          string
	uri             string
	retry           int
	signedURLExpiry time.Duration
	uploadID        string
}

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
	m           *XMLMPU
}

func (m *XMLMPU) InitiateXMLMPU() error {
	opts := &storage.SignedURLOptions{
		Scheme:          storage.SigningSchemeV4,
		Method:          "POST",
		Expires:         time.Now().Add(m.signedURLExpiry),
		QueryParameters: url.Values{MPUInitiateQuery: []string{""}},
	}
	u, err := m.cli.Bucket(m.bucket).SignedURL(m.uri, opts)
	if err != nil {
		return fmt.Errorf("Bucket(%q).SignedURL: %s", m.bucket, err)
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
	m.uploadID = uploadID
	return nil
}

// NewGCSWriter returns a GCSWriter which uses GCS multipart upload API behind the scene.
func NewGCSWriter(ctx context.Context, cli *storage.Client, uri string, partSize int, parallelCnt int, bucketName string) (*GCSWriter, error) {
	// multipart upload protocol: go/gcs-pb-multipart-upload#performing-a-multipart-upload
	m := XMLMPU{
		cli:             cli,
		retry:           defaultRetry,
		uri:             uri,
		signedURLExpiry: defaultSignedURLExpiry,
		bucket:          bucketName,
	}
	err := m.InitiateXMLMPU()
	if err != nil {
		err = fmt.Errorf("Failed to initiate XMLMPU: %s", err)
		return nil, err
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
		m:             &m,
	}

	for i := 0; i < parallelCnt; i++ {
		w.workers[i] = newUploadWorker(cli, uri, bucketName, m.uploadID, i, partSize, w.idleWorkers)
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
		w.currentWorker.uploadPartAsync()
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
		// log.Info(fmt.Sprintf("worker %v uploaded %v parts for %v", i, len(w.workers[i].parts), w.objURI))
	}

	return w.completeMultipartUpload(parts)
}

func (w *GCSWriter) partNum() int {
	return int((w.writtenTotalBytes / int64(w.partSize)) + 1) // Starts from 1, instead of 0
}

func (w *GCSWriter) completeMultipartUpload(parts map[int]objectPart) error {
	bodyXML, err := buildCompleteMultipartUploadXML(parts)
	if err != nil {
		return fmt.Errorf("failed to build complete multipart upload XML for %v, err: %w", w.objURI, err)
	}

	values := url.Values{}
	values.Add(MPUUploadIDQuery, w.m.uploadID)
	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "POST",
		Expires: time.Now().Add(defaultSignedURLExpiry),
		//ContentType:     "application/xml",
		QueryParameters: values,
	}
	u, err := w.m.cli.Bucket(w.m.bucket).SignedURL(w.objURI, opts)
	if err != nil {
		err = fmt.Errorf("Bucket(%q).SignedURL: %s", w.m.bucket, err)
		return err
	}

	client := resty.New()
	resp, err := client.R().SetBody(bodyXML).Post(u)
	if err != nil {
		err = fmt.Errorf("POST request failed: %s", err)
		return err
	}

	if resp.StatusCode() != http.StatusOK {
		err = fmt.Errorf("POST request returned non-OK status: %d", resp.StatusCode())
		return err
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
		if ok {
			upload.Parts = append(upload.Parts, part)
		} else {
			return "", fmt.Errorf("part %v not contained in parts", partNum)
		}
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
		return fmt.Errorf("failed to upload part %v too many times, err: %w", uw.index, uw.err)
	}
	go func() {
		err := uw.uploadPart(uw.currentPartNumber, uw.buffer[:uw.offset]) // timeout in few seconds
		if err != nil {
			uw.err = err
			uw.tryCnt++
			log.Error(fmt.Sprintf("upload worker %v failed to upload part %v for object %v, tryCnt: %v, err: %v", uw.index, uw.currentPartNumber, uw.uri, uw.tryCnt, err))
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
		return fmt.Errorf("Invalid partNumber: %v", partNumber)
	}

	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "PUT",
		Expires: time.Now().Add(defaultSignedURLExpiry),
		QueryParameters: url.Values{
			MPUUploadIDQuery:   []string{uw.uploadID},
			MPUPartNumberQuery: []string{strconv.Itoa(partNumber)},
		},
	}

	u, err := uw.cli.Bucket(uw.bucketName).SignedURL(uw.uri, opts)
	if err != nil {
		return fmt.Errorf("Bucket(%q).SignedURL: %s", uw.bucketName, err)
	}

	client := resty.New()
	resp, err := client.R().SetBody(bytes.NewReader(data)).Put(u) // Set payload as request body
	if err != nil {
		return fmt.Errorf("PUT request failed: %s", err)
	}

	etag := resp.Header().Get("ETag")

	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("PUT request returned non-OK status: %d", resp.StatusCode())
	}

	uw.parts[partNumber] = objectPart{PartNumber: partNumber, ETag: etag}
	return nil
}
