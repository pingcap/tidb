// Copyright 2017 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

//
/*
grpc_logging is a "parent" package for gRPC logging middlewares

General functionality of all middleware

All logging middleware have an `Extract(ctx)` function that provides a request-scoped logger with gRPC-related fields
(service and method names). Moreover, that logger will have fields populated from the `grpc_ctxtags.Tags` of the
context.

All logging middleware will emit a final log statement. It is based on the error returned by the handler function,
the gRPC status code, an error (if any) and it will emit at a level controlled via `WithLevels`.

This parent package

This particular package is intended for use by other middleware, logging or otherwise. It contains interfaces that other
logging middlewares *could* share . This allows code to be shared between different implementations.

Field names

All field names of loggers follow the OpenTracing semantics definitions, with `grpc.` prefix if needed:
https://github.com/opentracing/specification/blob/master/semantic_conventions.md

Implementations

There are two implementations at the moment: logrus and zap

See relevant packages below.
*/
package grpc_logging
