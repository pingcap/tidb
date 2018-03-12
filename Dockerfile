# Builder image
FROM golang:1.9-alpine as builder

RUN apk add --no-cache \
    make \
    git

COPY . /go/src/github.com/pingcap/tidb

WORKDIR /go/src/github.com/pingcap/tidb/

RUN make

# Executable image
FROM scratch

COPY --from=builder /go/src/github.com/pingcap/tidb/bin/tidb-server /tidb-server

WORKDIR /

EXPOSE 4000

ENTRYPOINT ["/tidb-server"]

