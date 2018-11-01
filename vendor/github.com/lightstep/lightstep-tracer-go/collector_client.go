package lightstep

import (
	"io"
	"net/http"

	"golang.org/x/net/context"

	cpb "github.com/lightstep/lightstep-tracer-go/collectorpb"
	"github.com/lightstep/lightstep-tracer-go/lightstep_thrift"
)

// Connection describes a closable connection. Exposed for testing.
type Connection interface {
	io.Closer
}

// ConnectorFactory is for testing purposes.
type ConnectorFactory func() (interface{}, Connection, error)

// collectorResponse encapsulates internal thrift/grpc responses.
type collectorResponse interface {
	GetErrors() []string
	Disable() bool
}

type reportRequest struct {
	thriftRequest *lightstep_thrift.ReportRequest
	protoRequest  *cpb.ReportRequest
	httpRequest   *http.Request
}

// collectorClient encapsulates internal thrift/grpc transports.
type collectorClient interface {
	Report(context.Context, reportRequest) (collectorResponse, error)
	Translate(context.Context, *reportBuffer) (reportRequest, error)
	ConnectClient() (Connection, error)
	ShouldReconnect() bool
}

func newCollectorClient(opts Options, reporterId uint64, attributes map[string]string) (collectorClient, error) {
	if opts.UseThrift {
		return newThriftCollectorClient(opts, reporterId, attributes), nil
	}

	if opts.UseHttp {
		return newHttpCollectorClient(opts, reporterId, attributes)
	}

	if opts.UseGRPC {
		return newGrpcCollectorClient(opts, reporterId, attributes), nil
	}

	// No transport specified, defaulting to GRPC
	return newGrpcCollectorClient(opts, reporterId, attributes), nil
}
