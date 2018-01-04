# grpc_ctxtags
`import "github.com/grpc-ecosystem/go-grpc-middleware/tags"`

* [Overview](#pkg-overview)
* [Imported Packages](#pkg-imports)
* [Index](#pkg-index)
* [Examples](#pkg-examples)

## <a name="pkg-overview">Overview</a>
`grpc_ctxtags` adds a Tag object to the context that can be used by other middleware to add context about a request.

### Request Context Tags
Tags describe information about the request, and can be set and used by other middleware, or handlers. Tags are used
for logging and tracing of requests. Tags are populated both upwards, *and* downwards in the interceptor-handler stack.

You can automatically extract tags (in `grpc.request.<field_name>`) from request payloads (in unary and server-streaming)
by passing in the `WithFieldExtractor` option.

If a user doesn't use the interceptors that initialize the `Tags` object, all operations following from an `Extract(ctx)`
will be no-ops. This is to ensure that code doesn't panic if the interceptors weren't used.

Tags fields are typed, and shallow and should follow the OpenTracing semantics convention:
<a href="https://github.com/opentracing/specification/blob/master/semantic_conventions.md">https://github.com/opentracing/specification/blob/master/semantic_conventions.md</a>

#### Example:

<details>
<summary>Click to expand code.</summary>

```go
opts := []grpc_ctxtags.Option{
	    grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.TagBasedRequestFieldExtractor("log_fields")),
	}
	server := grpc.NewServer(
	    grpc.StreamInterceptor(grpc_ctxtags.StreamServerInterceptor(opts...)),
	    grpc.UnaryInterceptor(grpc_ctxtags.UnaryServerInterceptor(opts...)),
	)
	return server
```

</details>

## <a name="pkg-imports">Imported Packages</a>

- [github.com/grpc-ecosystem/go-grpc-middleware](./..)
- [golang.org/x/net/context](https://godoc.org/golang.org/x/net/context)
- [google.golang.org/grpc](https://godoc.org/google.golang.org/grpc)
- [google.golang.org/grpc/peer](https://godoc.org/google.golang.org/grpc/peer)

## <a name="pkg-index">Index</a>
* [func CodeGenRequestFieldExtractor(fullMethod string, req interface{}) map[string]interface{}](#CodeGenRequestFieldExtractor)
* [func StreamServerInterceptor(opts ...Option) grpc.StreamServerInterceptor](#StreamServerInterceptor)
* [func UnaryServerInterceptor(opts ...Option) grpc.UnaryServerInterceptor](#UnaryServerInterceptor)
* [type Option](#Option)
  * [func WithFieldExtractor(f RequestFieldExtractorFunc) Option](#WithFieldExtractor)
* [type RequestFieldExtractorFunc](#RequestFieldExtractorFunc)
  * [func TagBasedRequestFieldExtractor(tagName string) RequestFieldExtractorFunc](#TagBasedRequestFieldExtractor)
* [type Tags](#Tags)
  * [func Extract(ctx context.Context) \*Tags](#Extract)
  * [func (t \*Tags) Has(key string) bool](#Tags.Has)
  * [func (t \*Tags) Set(key string, value interface{}) \*Tags](#Tags.Set)
  * [func (t \*Tags) Values() map[string]interface{}](#Tags.Values)

#### <a name="pkg-examples">Examples</a>
* [Package (Initialization)](#example__initialization)

#### <a name="pkg-files">Package files</a>
[context.go](./context.go) [doc.go](./doc.go) [fieldextractor.go](./fieldextractor.go) [interceptors.go](./interceptors.go) [options.go](./options.go) 

## <a name="CodeGenRequestFieldExtractor">func</a> [CodeGenRequestFieldExtractor](./fieldextractor.go#L23)
``` go
func CodeGenRequestFieldExtractor(fullMethod string, req interface{}) map[string]interface{}
```
CodeGenRequestFieldExtractor is a function that relies on code-generated functions that export log fields from requests.
These are usually coming from a protoc-plugin that generates additional information based on custom field options.

## <a name="StreamServerInterceptor">func</a> [StreamServerInterceptor](./interceptors.go#L26)
``` go
func StreamServerInterceptor(opts ...Option) grpc.StreamServerInterceptor
```
StreamServerInterceptor returns a new streaming server interceptor that sets the values for request tags.

## <a name="UnaryServerInterceptor">func</a> [UnaryServerInterceptor](./interceptors.go#L14)
``` go
func UnaryServerInterceptor(opts ...Option) grpc.UnaryServerInterceptor
```
UnaryServerInterceptor returns a new unary server interceptors that sets the values for request tags.

## <a name="Option">type</a> [Option](./options.go#L25)
``` go
type Option func(*options)
```

### <a name="WithFieldExtractor">func</a> [WithFieldExtractor](./options.go#L28)
``` go
func WithFieldExtractor(f RequestFieldExtractorFunc) Option
```
WithFieldExtractor customizes the function for extracting log fields from protobuf messages.

## <a name="RequestFieldExtractorFunc">type</a> [RequestFieldExtractorFunc](./fieldextractor.go#L13)
``` go
type RequestFieldExtractorFunc func(fullMethod string, req interface{}) map[string]interface{}
```
RequestFieldExtractorFunc is a user-provided function that extracts field information from a gRPC request.
It is called from tags middleware on arrival of unary request or a server-stream request.
Keys and values will be added to the context tags of the request. If there are no fields, you should return a nil.

### <a name="TagBasedRequestFieldExtractor">func</a> [TagBasedRequestFieldExtractor](./fieldextractor.go#L43)
``` go
func TagBasedRequestFieldExtractor(tagName string) RequestFieldExtractorFunc
```
TagedRequestFiledExtractor is a function that relies on Go struct tags to export log fields from requests.
These are usualy coming from a protoc-plugin, such as Gogo protobuf.

	message Metadata {
	   repeated string tags = 1 [ (gogoproto.moretags) = "log_field:\"meta_tags\"" ];
	}

The tagName is configurable using the tagName variable. Here it would be "log_field".

## <a name="Tags">type</a> [Tags](./context.go#L15-L17)
``` go
type Tags struct {
    // contains filtered or unexported fields
}
```
Tags is the struct used for storing request tags between Context calls.
This object is *not* thread safe, and should be handled only in the context of the request.

### <a name="Extract">func</a> [Extract](./context.go#L39)
``` go
func Extract(ctx context.Context) *Tags
```
Extracts returns a pre-existing Tags object in the Context.
If the context wasn't set in a tag interceptor, a no-op Tag storage is returned that will *not* be propagated in context.

### <a name="Tags.Has">func</a> (\*Tags) [Has](./context.go#L26)
``` go
func (t *Tags) Has(key string) bool
```
Has checks if the given key exists.

### <a name="Tags.Set">func</a> (\*Tags) [Set](./context.go#L20)
``` go
func (t *Tags) Set(key string, value interface{}) *Tags
```
Set sets the given key in the metadata tags.

### <a name="Tags.Values">func</a> (\*Tags) [Values](./context.go#L33)
``` go
func (t *Tags) Values() map[string]interface{}
```
Values returns a map of key to values.
Do not modify the underlying map, please use Set instead.

- - -
Generated by [godoc2ghmd](https://github.com/GandalfUK/godoc2ghmd)