package lightstep

import (
	"bytes"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/lightstep/lightstep-tracer-go/collectorpb"
	"golang.org/x/net/context"
	"golang.org/x/net/http2"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
	"errors"
)

var (
	acceptHeader      = http.CanonicalHeaderKey("Accept")
	contentTypeHeader = http.CanonicalHeaderKey("Content-Type")
)

const (
	collectorHttpMethod = "POST"
	collectorHttpPath   = "/api/v2/reports"
	protoContentType    = "application/octet-stream"
)

// grpcCollectorClient specifies how to send reports back to a LightStep
// collector via grpc.
type httpCollectorClient struct {
	// auth and runtime information
	reporterID  uint64
	accessToken string // accessToken is the access token used for explicit trace collection requests.
	attributes  map[string]string

	reportTimeout time.Duration

	// Remote service that will receive reports.
	url    *url.URL
	client *http.Client

	// converters
	converter *protoConverter
}

type HttpRequest struct {
	http.Request
}

type transportCloser struct {
	transport http2.Transport
}

func (closer *transportCloser) Close() error {
	closer.transport.CloseIdleConnections()

	return nil
}

func newHttpCollectorClient(
	opts Options,
	reporterID uint64,
	attributes map[string]string,
) (*httpCollectorClient, error) {
	url, err := url.Parse(opts.Collector.URL())
	if err != nil {
		fmt.Println("collector config does not produce valid url", err)
		return nil, err
	}
	url.Path = collectorHttpPath

	return &httpCollectorClient{
		reporterID:    reporterID,
		accessToken:   opts.AccessToken,
		attributes:    attributes,
		reportTimeout: opts.ReportTimeout,
		url:           url,
		converter:     newProtoConverter(opts),
	}, nil
}

func (client *httpCollectorClient) ConnectClient() (Connection, error) {
	// The golang http2 client implementation doesn't support plaintext http2 (a.k.a h2c) out of the box.
	// According to https://github.com/golang/go/issues/14141, they don't have plans to.
	// For now, we are falling back to http1 for plaintext.
	// In the future, we might want to add out own h2c implementation (see https://github.com/hkwi/h2c).
	var transport http.RoundTripper
	if client.url.Scheme == "https" {
		transport = &http2.Transport{}
	} else {
		transport = &http.Transport{}
	}

	client.client = &http.Client{
		Transport: transport,
		Timeout:   client.reportTimeout,
	}

	return &transportCloser{}, nil
}

func (client *httpCollectorClient) ShouldReconnect() bool {
	// http2 will handle connection reuse under the hood
	return false
}

func (client *httpCollectorClient) Report(context context.Context, req reportRequest) (collectorResponse, error) {

	httpResponse, err := client.client.Do(req.httpRequest)
	if err != nil {
		return nil, err
	}
	defer httpResponse.Body.Close()

	response, err := client.toResponse(httpResponse)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (client *httpCollectorClient) Translate(ctx context.Context, buffer *reportBuffer) (reportRequest, error) {

	httpRequest, err := client.toRequest(ctx, buffer)
	if err != nil {
		return reportRequest{}, err
	}
	return reportRequest{
		httpRequest: httpRequest,
	}, nil
}

func (client *httpCollectorClient) toRequest(
	context context.Context,
	buffer *reportBuffer,
) (*http.Request, error) {
	protoRequest := client.converter.toReportRequest(
		client.reporterID,
		client.attributes,
		client.accessToken,
		buffer,
	)

	buf, err := proto.Marshal(protoRequest)
	if err != nil {
		return nil, err
	}

	requestBody := bytes.NewReader(buf)

	request, err := http.NewRequest(collectorHttpMethod, client.url.String(), requestBody)
	if err != nil {
		return nil, err
	}
	request = request.WithContext(context)
	request.Header.Set(contentTypeHeader, protoContentType)
	request.Header.Set(acceptHeader, protoContentType)

	return request, nil
}

func (client *httpCollectorClient) toResponse(response *http.Response) (collectorResponse, error) {
	if response.StatusCode != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("status code (%d) is not ok", response.StatusCode))
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	protoResponse := &collectorpb.ReportResponse{}
	if err := proto.Unmarshal(body, protoResponse); err != nil {
		return nil, err
	}

	return protoResponse, nil
}
