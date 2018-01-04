# grpc_logrus
`import "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"`

* [Overview](#pkg-overview)
* [Imported Packages](#pkg-imports)
* [Index](#pkg-index)
* [Examples](#pkg-examples)

## <a name="pkg-overview">Overview</a>
`grpc_logrus` is a gRPC logging middleware backed by Logrus loggers

It accepts a user-configured `logrus.Entry` that will be used for logging completed gRPC calls. The same
`logrus.Entry` will be used for logging completed gRPC calls, and be populated into the `context.Context` passed into gRPC handler code.

You can use `Extract` to log into a request-scoped `logrus.Entry` instance in your handler code. The fields set on the
logger correspond to the grpc_ctxtags.Tags attached to the context.

This package also implements request and response *payload* logging, both for server-side and client-side. These will be
logged as structured `jsonbp` fields for every message received/sent (both unary and streaming). For that please use
`Payload*Interceptor` functions for that. Please note that the user-provided function that determines whetether to log
the full request/response payload needs to be written with care, this can significantly slow down gRPC.

Logrus can also be made as a backend for gRPC library internals. For that use `ReplaceGrpcLogger`.

Please see examples and tests for examples of use.

#### Example:

<details>
<summary>Click to expand code.</summary>

```go
x := func(ctx context.Context, ping *pb_testproto.PingRequest) (*pb_testproto.PingResponse, error) {
	    // Add fields the ctxtags of the request which will be added to all extracted loggers.
	    grpc_ctxtags.Extract(ctx).Set("custom_tags.string", "something").Set("custom_tags.int", 1337)
	    // Extract a request-scoped zap.Logger and log a message.
	    grpc_logrus.Extract(ctx).Info("some ping")
	    return &pb_testproto.PingResponse{Value: ping.Value}, nil
	}
	return x
```

</details>

#### Example:

<details>
<summary>Click to expand code.</summary>

```go
// Logrus entry is used, allowing pre-definition of certain fields by the user.
	logrusEntry := logrus.NewEntry(logrusLogger)
	// Shared options for the logger, with a custom gRPC code to log level function.
	opts := []grpc_logrus.Option{
	    grpc_logrus.WithLevels(customFunc),
	}
	// Make sure that log statements internal to gRPC library are logged using the zapLogger as well.
	grpc_logrus.ReplaceGrpcLogger(logrusEntry)
	// Create a server, make sure we put the grpc_ctxtags context before everything else.
	server := grpc.NewServer(
	    grpc_middleware.WithUnaryServerChain(
	        grpc_ctxtags.UnaryServerInterceptor(grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor)),
	        grpc_logrus.UnaryServerInterceptor(logrusEntry, opts...),
	    ),
	    grpc_middleware.WithStreamServerChain(
	        grpc_ctxtags.StreamServerInterceptor(grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor)),
	        grpc_logrus.StreamServerInterceptor(logrusEntry, opts...),
	    ),
	)
	return server
```

</details>

#### Example:

<details>
<summary>Click to expand code.</summary>

```go
// Logrus entry is used, allowing pre-definition of certain fields by the user.
	logrusEntry := logrus.NewEntry(logrusLogger)
	// Shared options for the logger, with a custom duration to log field function.
	opts := []grpc_logrus.Option{
	    grpc_logrus.WithDurationField(func(duration time.Duration) (key string, value interface{}) {
	        return "grpc.time_ns", duration.Nanoseconds()
	    }),
	}
	server := grpc.NewServer(
	    grpc_middleware.WithUnaryServerChain(
	        grpc_ctxtags.UnaryServerInterceptor(),
	        grpc_logrus.UnaryServerInterceptor(logrusEntry, opts...),
	    ),
	    grpc_middleware.WithStreamServerChain(
	        grpc_ctxtags.StreamServerInterceptor(),
	        grpc_logrus.StreamServerInterceptor(logrusEntry, opts...),
	    ),
	)
	return server
```

</details>

## <a name="pkg-imports">Imported Packages</a>

- github.com/sirupsen/logrus
- [github.com/golang/protobuf/jsonpb](https://godoc.org/github.com/golang/protobuf/jsonpb)
- [github.com/golang/protobuf/proto](https://godoc.org/github.com/golang/protobuf/proto)
- [github.com/grpc-ecosystem/go-grpc-middleware](./../..)
- [github.com/grpc-ecosystem/go-grpc-middleware/logging](./..)
- [github.com/grpc-ecosystem/go-grpc-middleware/tags](./../../tags)
- [golang.org/x/net/context](https://godoc.org/golang.org/x/net/context)
- [google.golang.org/grpc](https://godoc.org/google.golang.org/grpc)
- [google.golang.org/grpc/codes](https://godoc.org/google.golang.org/grpc/codes)
- [google.golang.org/grpc/grpclog](https://godoc.org/google.golang.org/grpc/grpclog)

## <a name="pkg-index">Index</a>
* [Variables](#pkg-variables)
* [func DefaultClientCodeToLevel(code codes.Code) logrus.Level](#DefaultClientCodeToLevel)
* [func DefaultCodeToLevel(code codes.Code) logrus.Level](#DefaultCodeToLevel)
* [func DurationToDurationField(duration time.Duration) (key string, value interface{})](#DurationToDurationField)
* [func DurationToTimeMillisField(duration time.Duration) (key string, value interface{})](#DurationToTimeMillisField)
* [func Extract(ctx context.Context) \*logrus.Entry](#Extract)
* [func PayloadStreamClientInterceptor(entry \*logrus.Entry, decider grpc\_logging.ClientPayloadLoggingDecider) grpc.StreamClientInterceptor](#PayloadStreamClientInterceptor)
* [func PayloadStreamServerInterceptor(entry \*logrus.Entry, decider grpc\_logging.ServerPayloadLoggingDecider) grpc.StreamServerInterceptor](#PayloadStreamServerInterceptor)
* [func PayloadUnaryClientInterceptor(entry \*logrus.Entry, decider grpc\_logging.ClientPayloadLoggingDecider) grpc.UnaryClientInterceptor](#PayloadUnaryClientInterceptor)
* [func PayloadUnaryServerInterceptor(entry \*logrus.Entry, decider grpc\_logging.ServerPayloadLoggingDecider) grpc.UnaryServerInterceptor](#PayloadUnaryServerInterceptor)
* [func ReplaceGrpcLogger(logger \*logrus.Entry)](#ReplaceGrpcLogger)
* [func StreamClientInterceptor(entry \*logrus.Entry, opts ...Option) grpc.StreamClientInterceptor](#StreamClientInterceptor)
* [func StreamServerInterceptor(entry \*logrus.Entry, opts ...Option) grpc.StreamServerInterceptor](#StreamServerInterceptor)
* [func UnaryClientInterceptor(entry \*logrus.Entry, opts ...Option) grpc.UnaryClientInterceptor](#UnaryClientInterceptor)
* [func UnaryServerInterceptor(entry \*logrus.Entry, opts ...Option) grpc.UnaryServerInterceptor](#UnaryServerInterceptor)
* [type CodeToLevel](#CodeToLevel)
* [type DurationToField](#DurationToField)
* [type Option](#Option)
  * [func WithCodes(f grpc\_logging.ErrorToCode) Option](#WithCodes)
  * [func WithDurationField(f DurationToField) Option](#WithDurationField)
  * [func WithLevels(f CodeToLevel) Option](#WithLevels)

#### <a name="pkg-examples">Examples</a>
* [Package (HandlerUsageUnaryPing)](#example__handlerUsageUnaryPing)
* [Package (Initialization)](#example__initialization)
* [Package (InitializationWithDurationFieldOverride)](#example__initializationWithDurationFieldOverride)

#### <a name="pkg-files">Package files</a>
[client_interceptors.go](./client_interceptors.go) [context.go](./context.go) [doc.go](./doc.go) [grpclogger.go](./grpclogger.go) [noop.go](./noop.go) [options.go](./options.go) [payload_interceptors.go](./payload_interceptors.go) [server_interceptors.go](./server_interceptors.go) 

## <a name="pkg-variables">Variables</a>
``` go
var (
    // SystemField is used in every log statement made through grpc_logrus. Can be overwritten before any initialization code.
    SystemField = "system"

    // KindField describes the log gield used to incicate whether this is a server or a client log statment.
    KindField = "span.kind"
)
```
``` go
var DefaultDurationToField = DurationToTimeMillisField
```
DefaultDurationToField is the default implementation of converting request duration to a log field (key and value).

``` go
var (
    // JsonPBMarshaller is the marshaller used for serializing protobuf messages.
    JsonPbMarshaller = &jsonpb.Marshaler{}
)
```

## <a name="DefaultClientCodeToLevel">func</a> [DefaultClientCodeToLevel](./options.go#L120)
``` go
func DefaultClientCodeToLevel(code codes.Code) logrus.Level
```
DefaultClientCodeToLevel is the default implementation of gRPC return codes to log levels for client side.

## <a name="DefaultCodeToLevel">func</a> [DefaultCodeToLevel](./options.go#L78)
``` go
func DefaultCodeToLevel(code codes.Code) logrus.Level
```
DefaultCodeToLevel is the default implementation of gRPC return codes to log levels for server side.

## <a name="DurationToDurationField">func</a> [DurationToDurationField](./options.go#L170)
``` go
func DurationToDurationField(duration time.Duration) (key string, value interface{})
```
DurationToDurationField uses the duration value to log the request duration.

## <a name="DurationToTimeMillisField">func</a> [DurationToTimeMillisField](./options.go#L165)
``` go
func DurationToTimeMillisField(duration time.Duration) (key string, value interface{})
```
DurationToTimeMillisField converts the duration to milliseconds and uses the key `grpc.time_ms`.

## <a name="Extract">func</a> [Extract](./context.go#L22)
``` go
func Extract(ctx context.Context) *logrus.Entry
```
Extract takes the call-scoped logrus.Entry from grpc_logrus middleware.

If the grpc_logrus middleware wasn't used, a no-op `logrus.Entry` is returned. This makes it safe to
use regardless.

## <a name="PayloadStreamClientInterceptor">func</a> [PayloadStreamClientInterceptor](./payload_interceptors.go#L77)
``` go
func PayloadStreamClientInterceptor(entry *logrus.Entry, decider grpc_logging.ClientPayloadLoggingDecider) grpc.StreamClientInterceptor
```
PayloadStreamServerInterceptor returns a new streaming client interceptor that logs the paylods of requests and responses.

## <a name="PayloadStreamServerInterceptor">func</a> [PayloadStreamServerInterceptor](./payload_interceptors.go#L48)
``` go
func PayloadStreamServerInterceptor(entry *logrus.Entry, decider grpc_logging.ServerPayloadLoggingDecider) grpc.StreamServerInterceptor
```
PayloadUnaryServerInterceptor returns a new server server interceptors that logs the payloads of requests.

This *only* works when placed *after* the `grpc_logrus.StreamServerInterceptor`. However, the logging can be done to a
separate instance of the logger.

## <a name="PayloadUnaryClientInterceptor">func</a> [PayloadUnaryClientInterceptor](./payload_interceptors.go#L61)
``` go
func PayloadUnaryClientInterceptor(entry *logrus.Entry, decider grpc_logging.ClientPayloadLoggingDecider) grpc.UnaryClientInterceptor
```
PayloadUnaryClientInterceptor returns a new unary client interceptor that logs the paylods of requests and responses.

## <a name="PayloadUnaryServerInterceptor">func</a> [PayloadUnaryServerInterceptor](./payload_interceptors.go#L28)
``` go
func PayloadUnaryServerInterceptor(entry *logrus.Entry, decider grpc_logging.ServerPayloadLoggingDecider) grpc.UnaryServerInterceptor
```
PayloadUnaryServerInterceptor returns a new unary server interceptors that logs the payloads of requests.

This *only* works when placed *after* the `grpc_logrus.UnaryServerInterceptor`. However, the logging can be done to a
separate instance of the logger.

## <a name="ReplaceGrpcLogger">func</a> [ReplaceGrpcLogger](./grpclogger.go#L13)
``` go
func ReplaceGrpcLogger(logger *logrus.Entry)
```
ReplaceGrpcLogger sets the given logrus.Logger as a gRPC-level logger.
This should be called *before* any other initialization, preferably from init() functions.

## <a name="StreamClientInterceptor">func</a> [StreamClientInterceptor](./client_interceptors.go#L28)
``` go
func StreamClientInterceptor(entry *logrus.Entry, opts ...Option) grpc.StreamClientInterceptor
```
StreamServerInterceptor returns a new streaming client interceptor that optionally logs the execution of external gRPC calls.

## <a name="StreamServerInterceptor">func</a> [StreamServerInterceptor](./server_interceptors.go#L50)
``` go
func StreamServerInterceptor(entry *logrus.Entry, opts ...Option) grpc.StreamServerInterceptor
```
StreamServerInterceptor returns a new streaming server interceptor that adds logrus.Entry to the context.

## <a name="UnaryClientInterceptor">func</a> [UnaryClientInterceptor](./client_interceptors.go#L16)
``` go
func UnaryClientInterceptor(entry *logrus.Entry, opts ...Option) grpc.UnaryClientInterceptor
```
UnaryClientInterceptor returns a new unary client interceptor that optionally logs the execution of external gRPC calls.

## <a name="UnaryServerInterceptor">func</a> [UnaryServerInterceptor](./server_interceptors.go#L25)
``` go
func UnaryServerInterceptor(entry *logrus.Entry, opts ...Option) grpc.UnaryServerInterceptor
```
PayloadUnaryServerInterceptor returns a new unary server interceptors that adds logrus.Entry to the context.

## <a name="CodeToLevel">type</a> [CodeToLevel](./options.go#L51)
``` go
type CodeToLevel func(code codes.Code) logrus.Level
```
CodeToLevel function defines the mapping between gRPC return codes and interceptor log level.

## <a name="DurationToField">type</a> [DurationToField](./options.go#L54)
``` go
type DurationToField func(duration time.Duration) (key string, value interface{})
```
DurationToField function defines how to produce duration fields for logging

## <a name="Option">type</a> [Option](./options.go#L48)
``` go
type Option func(*options)
```

### <a name="WithCodes">func</a> [WithCodes](./options.go#L64)
``` go
func WithCodes(f grpc_logging.ErrorToCode) Option
```
WithCodes customizes the function for mapping errors to error codes.

### <a name="WithDurationField">func</a> [WithDurationField](./options.go#L71)
``` go
func WithDurationField(f DurationToField) Option
```
WithDurationField customizes the function for mapping request durations to log fields.

### <a name="WithLevels">func</a> [WithLevels](./options.go#L57)
``` go
func WithLevels(f CodeToLevel) Option
```
WithLevels customizes the function for mapping gRPC return codes and interceptor log level statements.

- - -
Generated by [godoc2ghmd](https://github.com/GandalfUK/godoc2ghmd)