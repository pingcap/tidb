package storage

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/pingcap/log"
	"golang.org/x/oauth2"
)

const (
	httpClientTimeout = 5 * time.Second
	uploadPartTimeout = 5 * time.Second
	uploadPartTryCnt  = 3 // if uploading a part fails, we should try 3 times in total.
)

var (
	newHTTPClient = func(timeout time.Duration, trans *http.Transport) httpClient {
		c := &http.Client{Timeout: timeout}
		if trans != nil {
			c.Transport = trans
		}
		return c
	}

	clock = clockwork.NewRealClock()
)

type authToken interface {
	SetAuthHeader(r *http.Request)
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// GCSWriter is GCSWriter following GCS multipart upload protocol.
type GCSWriter struct {
	ctx context.Context

	token      authToken
	httpClient httpClient

	objURI       string
	bucketName   string
	objName      string
	uploadID     string
	bucketRegion string

	partSize          int   // in bytes
	parallelCnt       int   // number of workers
	writtenTotalBytes int64 // the total number of bytes uploaded by workers so far

	workers       []*uploadWorker
	currentWorker *uploadWorker // the worker who is buffering the data. Once the buffer is full,
	// the data will be uploaded as a part.
	idleWorkers chan *uploadWorker
}

// NewGCSWriter returns a GCSWriter which uses GCS multipart upload API behind the scene.
func NewGCSWriter(ctx context.Context, uri string, partSize int, parallelCnt int, bucketRegion string) (*GCSWriter, error) {
	// multipart upload protocol: go/gcs-pb-multipart-upload#performing-a-multipart-upload

	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GCS URI: '%v', err: %w", uri, err)
	}

	bucketName := u.Host
	objName := strings.TrimLeft(u.Path, "/")

	token := &oauth2.Token{}

	httpClient := newHTTPClient(httpClientTimeout, nil)

	uploadID, err := initMultipartUpload(httpClient, token, bucketName, objName, bucketRegion)
	if err != nil {
		return nil, fmt.Errorf("failed to init multipart upload, err: %w", err)
	}

	w := &GCSWriter{
		ctx:           ctx,
		token:         token,
		httpClient:    httpClient,
		objURI:        uri,
		bucketName:    bucketName,
		objName:       objName,
		bucketRegion:  bucketRegion,
		partSize:      partSize,
		parallelCnt:   parallelCnt,
		workers:       make([]*uploadWorker, parallelCnt),
		currentWorker: nil,
		idleWorkers:   make(chan *uploadWorker, parallelCnt),
		uploadID:      uploadID,
	}

	for i := 0; i < parallelCnt; i++ {
		// Needs to use dedicated Transport for each worker, otherwise the combined throughput is throttled somehow.
		trans := &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			MaxConnsPerHost:       100,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       10 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
		w.workers[i] = newUploadWorker(token, bucketName, objName, uploadID, bucketRegion, i, partSize, w.idleWorkers, trans)
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
		log.Info(fmt.Sprintf("worker %v uploaded %v parts for %v", i, len(w.workers[i].parts), w.objURI))
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
	completeURL := fmt.Sprintf("https://%s.storage.googleapis.com/%s?uploadId=%s", w.bucketName, w.objName, w.uploadID)
	if strings.HasPrefix(w.bucketRegion, "us") {
		completeURL = fmt.Sprintf("https://%s.%s-storage.googleapis.com/%s?uploadId=%s", w.bucketName, w.bucketRegion, w.objName, w.uploadID)
	}

	req, err := http.NewRequest("POST", completeURL, strings.NewReader(bodyXML))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Length", fmt.Sprintf("%v", len(bodyXML)))
	req.Header.Add("Date", clock.Now().Format(http.TimeFormat))
	req.Header.Add("Content-Type", "application/xml")
	w.token.SetAuthHeader(req)
	resp, err := w.httpClient.Do(req)
	defer resp.Body.Close()
	if err != nil {
		return err
	}
	if err := checkResponse(resp); err != nil {
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

func initMultipartUpload(httpClient httpClient, token authToken, bucketName, objName, bucketRegion string) (string, error) {
	initialPostURL := fmt.Sprintf("https://%s.storage.googleapis.com/%s?uploads", bucketName, objName)
	if strings.HasPrefix(bucketRegion, "us") {
		initialPostURL = fmt.Sprintf("https://%s.%s-storage.googleapis.com/%s?uploads", bucketName, bucketRegion, objName)
	}
	req, err := http.NewRequest("POST", initialPostURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request, err: %w", err)
	}
	req.Header.Add("Content-Length", "0")
	req.Header.Add("Date", clock.Now().Format(http.TimeFormat))
	req.Header.Add("Content-Type", "application/octet-stream")
	token.SetAuthHeader(req)

	resp, err := httpClient.Do(req)
	defer resp.Body.Close()
	if err != nil {
		return "", fmt.Errorf("failed to init multipart upload, err: %w", err)
	}
	if err := checkResponse(resp); err != nil {
		return "", fmt.Errorf("multipart upload failed, err: %w", err)
	}

	// Extract the uploadID of the multipart upload
	parsedResult := &struct {
		UploadID string `xml:"UploadId"`
	}{}

	xml := xml.NewDecoder(resp.Body)
	if err := xml.Decode(parsedResult); err != nil {
		return "", fmt.Errorf("failed to decode multipart upload result, err: %w", err)
	}

	return parsedResult.UploadID, nil
}

func checkResponse(resp *http.Response) error {
	if resp.StatusCode == 200 {
		return nil
	}

	// Fetch the scotty upload ID.
	debugID := resp.Header.Get("x-guploader-uploadid")

	// Default to a basic message if there is no body.
	errStr := http.StatusText(resp.StatusCode)
	if resp.Body != nil {
		body, readErr := ioutil.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("%w (failed to read response body); %s; x-guploader-uploadid=%s", readErr, errStr, debugID)
		}
		if bodyStr := string(body); bodyStr != "" {
			errStr = bodyStr
		}
	}

	return fmt.Errorf("%s; x-guploader-uploadid=%s", errStr, debugID)
}

type objectPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type uploadWorker struct {
	index int // the index of the worker

	token      authToken
	httpClient httpClient

	bucketName, objName, uploadID, bucketRegion string

	buffer            []byte // buffer capacity is equal to partSize
	offset            int
	partSize          int // 5 MiB <= partSize <= 5 GiB
	currentPartNumber int
	parts             map[int]objectPart

	idleWorkerPool chan *uploadWorker

	err    error // the error got from the last upload, will retry this part
	tryCnt int
}

func newUploadWorker(token authToken, bucketName, objName, uploadID, bucketRegion string, index, partSize int, idleWorkerPool chan *uploadWorker, trans *http.Transport) *uploadWorker {
	return &uploadWorker{
		index:          index,
		token:          token,
		bucketName:     bucketName,
		objName:        objName,
		uploadID:       uploadID,
		bucketRegion:   bucketRegion,
		buffer:         make([]byte, partSize),
		partSize:       partSize,
		parts:          make(map[int]objectPart),
		httpClient:     newHTTPClient(uploadPartTimeout, trans),
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
			log.Error(fmt.Sprintf("upload worker %v failed to upload part %v for object %v, tryCnt: %v, err: %v", uw.index, uw.currentPartNumber, uw.objName, uw.tryCnt, err))
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

	url := uw.uploadPartURL(partNumber)
	req, err := http.NewRequest("PUT", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Length", fmt.Sprintf("%d", len(data)))
	req.Header.Add("Date", clock.Now().Format(http.TimeFormat))
	uw.token.SetAuthHeader(req)
	resp, err := uw.httpClient.Do(req)
	defer resp.Body.Close()
	if err != nil {
		return err
	}

	if err := checkResponse(resp); err != nil {
		return err
	}

	etag := resp.Header.Get("ETag")
	if etag == "" {
		respStr := &strings.Builder{}
		resp.Write(respStr)
		return fmt.Errorf("uploadPart did not return in the expected format. Unable to get the ETag for part %v. Resp: %v", partNumber, respStr.String())
	}
	uw.parts[partNumber] = objectPart{PartNumber: partNumber, ETag: etag}
	return nil
}

func (uw *uploadWorker) uploadPartURL(partNumber int) string {
	if strings.HasPrefix(uw.bucketRegion, "us-") {
		return fmt.Sprintf(
			"https://%s.%s-storage.googleapis.com/%s?uploadId=%s&partNumber=%v",
			uw.bucketName, uw.bucketRegion, uw.objName, uw.uploadID, partNumber)
	}
	return fmt.Sprintf(
		"https://%s.storage.googleapis.com/%s?uploadId=%s&partNumber=%v",
		uw.bucketName, uw.objName, uw.uploadID, partNumber)
}
