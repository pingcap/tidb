package lightstep

import (
	"encoding/base64"
	"io"
	"io/ioutil"

	"github.com/golang/protobuf/proto"
	lightstep "github.com/lightstep/lightstep-tracer-go/lightsteppb"
	opentracing "github.com/opentracing/opentracing-go"
)

// BinaryCarrier is used as the format parameter in inject/extract for lighstep binary propagation.
const BinaryCarrier = opentracing.Binary

var theBinaryPropagator binaryPropagator

type binaryPropagator struct{}

func (binaryPropagator) Inject(
	spanContext opentracing.SpanContext,
	opaqueCarrier interface{},
) error {
	sc, ok := spanContext.(SpanContext)
	if !ok {
		return opentracing.ErrInvalidSpanContext
	}
	data, err := proto.Marshal(&lightstep.BinaryCarrier{
		BasicCtx: &lightstep.BasicTracerCarrier{
			TraceId:      sc.TraceID,
			SpanId:       sc.SpanID,
			Sampled:      true,
			BaggageItems: sc.Baggage,
		},
	})
	if err != nil {
		return err
	}

	switch carrier := opaqueCarrier.(type) {
	case io.Writer:
		buf := make([]byte, base64.StdEncoding.EncodedLen(len(data)))
		base64.StdEncoding.Encode(buf, data)
		_, err = carrier.Write(buf)
		return err
	case *string:
		*carrier = base64.StdEncoding.EncodeToString(data)
	case *[]byte:
		*carrier = make([]byte, base64.StdEncoding.EncodedLen(len(data)))
		base64.StdEncoding.Encode(*carrier, data)
	default:
		return opentracing.ErrInvalidCarrier
	}
	return nil
}

func (binaryPropagator) Extract(
	opaqueCarrier interface{},
) (opentracing.SpanContext, error) {
	var data []byte
	var err error

	// Decode from string, *string, *[]byte, or []byte
	switch carrier := opaqueCarrier.(type) {
	case io.Reader:
		buf, err := ioutil.ReadAll(carrier)
		if err != nil {
			return nil, err
		}
		data, err = decodeBase64Bytes(buf)
	case *string:
		if carrier != nil {
			data, err = base64.StdEncoding.DecodeString(*carrier)
		}
	case string:
		data, err = base64.StdEncoding.DecodeString(carrier)
	case *[]byte:
		if carrier != nil {
			data, err = decodeBase64Bytes(*carrier)
		}
	case []byte:
		data, err = decodeBase64Bytes(carrier)
	default:
		return nil, opentracing.ErrInvalidCarrier
	}
	if err != nil {
		return nil, err
	}
	pb := &lightstep.BinaryCarrier{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return nil, err
	}
	if pb.BasicCtx == nil {
		return nil, opentracing.ErrInvalidCarrier
	}

	return SpanContext{
		TraceID: pb.BasicCtx.TraceId,
		SpanID:  pb.BasicCtx.SpanId,
		Baggage: pb.BasicCtx.BaggageItems,
	}, nil
}

func decodeBase64Bytes(in []byte) ([]byte, error) {
	data := make([]byte, base64.StdEncoding.DecodedLen(len(in)))
	n, err := base64.StdEncoding.Decode(data, in)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}
