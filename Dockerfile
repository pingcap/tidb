# Builder image
FROM golang:1.9 as builder

COPY . /go/src/github.com/pingcap/tidb

WORKDIR /go/src/github.com/pingcap/tidb/

RUN make

RUN chmod +x /go/src/github.com/pingcap/tidb/bin/tidb-server

# Executable image
FROM scratch

COPY --from=builder /go/src/github.com/pingcap/tidb/bin/tidb-server /tidb-server

WORKDIR /

EXPOSE 4000

ENTRYPOINT ["/tidb-server"]

