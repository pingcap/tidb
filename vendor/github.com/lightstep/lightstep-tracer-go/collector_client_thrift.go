package lightstep

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/lightstep/lightstep-tracer-go/lightstep_thrift"
	"github.com/lightstep/lightstep-tracer-go/thrift_0_9_2/lib/go/thrift"
)

// thriftCollectorClient specifies how to send reports back to a LightStep
// collector via thrift
type thriftCollectorClient struct {
	// auth and runtime information
	auth       *lightstep_thrift.Auth
	attributes map[string]string
	startTime  time.Time
	reporterID uint64

	lastReportAttempt  time.Time
	maxReportingPeriod time.Duration
	reportInFlight     bool
	// Remote service that will receive reports
	thriftClient lightstep_thrift.ReportingService

	// apiURL is the base URL of the LightStep web API, used for
	// explicit trace collection requests.
	collectorURL string

	// AccessToken is the access token used for explicit trace
	// collection requests.
	AccessToken string

	verbose bool

	// flags replacement
	maxLogMessageLen int
	maxLogKeyLen     int

	reportTimeout time.Duration

	thriftConnectorFactory ConnectorFactory
}

func newThriftCollectorClient(opts Options, guid uint64, attributes map[string]string) *thriftCollectorClient {
	reportTimeout := 60 * time.Second
	if opts.ReportTimeout > 0 {
		reportTimeout = opts.ReportTimeout
	}

	now := time.Now()
	rec := &thriftCollectorClient{
		auth: &lightstep_thrift.Auth{
			AccessToken: thrift.StringPtr(opts.AccessToken),
		},
		attributes:             attributes,
		startTime:              now,
		maxReportingPeriod:     opts.ReportingPeriod,
		verbose:                opts.Verbose,
		collectorURL:           opts.Collector.URL(),
		AccessToken:            opts.AccessToken,
		maxLogMessageLen:       opts.MaxLogValueLen,
		maxLogKeyLen:           opts.MaxLogKeyLen,
		reportTimeout:          reportTimeout,
		thriftConnectorFactory: opts.ConnFactory,
		reporterID:             guid,
	}
	return rec
}

func (client *thriftCollectorClient) ConnectClient() (Connection, error) {
	var conn Connection

	if client.thriftConnectorFactory != nil {
		unchecked_client, transport, err := client.thriftConnectorFactory()
		if err != nil {
			return nil, err
		}

		thriftClient, ok := unchecked_client.(lightstep_thrift.ReportingService)
		if !ok {
			return nil, fmt.Errorf("Thrift connector factory did not provide valid client!")
		}

		conn = transport
		client.thriftClient = thriftClient
	} else {
		transport, err := thrift.NewTHttpPostClient(client.collectorURL, client.reportTimeout)
		if err != nil {
			return nil, err
		}

		conn = transport
		client.thriftClient = lightstep_thrift.NewReportingServiceClientFactory(
			transport,
			thrift.NewTBinaryProtocolFactoryDefault(),
		)
	}
	return conn, nil
}

func (*thriftCollectorClient) ShouldReconnect() bool {
	return false
}

func (client *thriftCollectorClient) Report(_ context.Context, req reportRequest) (collectorResponse, error) {
	if req.thriftRequest == nil {
		return nil, fmt.Errorf("thriftRequest cannot be null")
	}
	resp, err := client.thriftClient.Report(client.auth, req.thriftRequest)
	if err != nil {
		return nil, err
	}

	return resp, err
}

func (client *thriftCollectorClient) Translate(_ context.Context, buffer *reportBuffer) (reportRequest, error) {
	rawSpans := buffer.rawSpans
	// Convert them to thrift.
	recs := make([]*lightstep_thrift.SpanRecord, len(rawSpans))
	// TODO: could pool lightstep_thrift.SpanRecords
	for i, raw := range rawSpans {
		var joinIds []*lightstep_thrift.TraceJoinId
		var attributes []*lightstep_thrift.KeyValue
		for key, value := range raw.Tags {
			// Note: the gRPC tracer uses Sprintf("%#v") for non-scalar non-string
			// values, differs from the treatment here:
			if strings.HasPrefix(key, "join:") {
				joinIds = append(joinIds, &lightstep_thrift.TraceJoinId{key, fmt.Sprint(value)})
			} else {
				attributes = append(attributes, &lightstep_thrift.KeyValue{key, fmt.Sprint(value)})
			}
		}
		logs := make([]*lightstep_thrift.LogRecord, len(raw.Logs))
		for j, log := range raw.Logs {
			thriftLogRecord := &lightstep_thrift.LogRecord{
				TimestampMicros: thrift.Int64Ptr(log.Timestamp.UnixNano() / 1000),
			}
			// In the deprecated thrift case, we can reuse a single "field"
			// encoder across all of the N log fields.
			lfe := thriftLogFieldEncoder{thriftLogRecord, client}
			for _, f := range log.Fields {
				f.Marshal(&lfe)
			}
			logs[j] = thriftLogRecord
		}

		// TODO implement baggage
		if raw.ParentSpanID != 0 {
			attributes = append(attributes, &lightstep_thrift.KeyValue{ParentSpanGUIDKey,
				strconv.FormatUint(raw.ParentSpanID, 16)})
		}

		recs[i] = &lightstep_thrift.SpanRecord{
			SpanGuid:       thrift.StringPtr(strconv.FormatUint(raw.Context.SpanID, 16)),
			TraceGuid:      thrift.StringPtr(strconv.FormatUint(raw.Context.TraceID, 16)),
			SpanName:       thrift.StringPtr(raw.Operation),
			JoinIds:        joinIds,
			OldestMicros:   thrift.Int64Ptr(raw.Start.UnixNano() / 1000),
			YoungestMicros: thrift.Int64Ptr(raw.Start.Add(raw.Duration).UnixNano() / 1000),
			Attributes:     attributes,
			LogRecords:     logs,
		}
	}

	metrics := lightstep_thrift.Metrics{
		Counts: []*lightstep_thrift.MetricsSample{
			&lightstep_thrift.MetricsSample{
				Name:       "spans.dropped",
				Int64Value: &buffer.droppedSpanCount,
			},
		},
	}

	req := &lightstep_thrift.ReportRequest{
		OldestMicros:    thrift.Int64Ptr(buffer.reportEnd.UnixNano() / 1000),
		YoungestMicros:  thrift.Int64Ptr(buffer.reportStart.UnixNano() / 1000),
		Runtime:         client.thriftRuntime(),
		SpanRecords:     recs,
		InternalMetrics: &metrics,
	}
	return reportRequest{
		thriftRequest: req,
	}, nil
}

// caller must hold r.lock
func (r *thriftCollectorClient) thriftRuntime() *lightstep_thrift.Runtime {
	guid := strconv.FormatUint(r.reporterID, 10)
	runtimeAttrs := []*lightstep_thrift.KeyValue{}
	for k, v := range r.attributes {
		runtimeAttrs = append(runtimeAttrs, &lightstep_thrift.KeyValue{k, v})
	}
	return &lightstep_thrift.Runtime{
		StartMicros: thrift.Int64Ptr(r.startTime.UnixNano() / 1000),
		Attrs:       runtimeAttrs,
		Guid:        &guid,
	}
}
