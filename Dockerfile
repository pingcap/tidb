# Builder image
FROM golang:1.11.2-alpine as builder

RUN apk add --no-cache \
    wget \
    make \
    git

RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 \
 && chmod +x /usr/local/bin/dumb-init

COPY . /go/src/github.com/pingcap/tidb

WORKDIR /go/src/github.com/pingcap/tidb/

RUN make

# Executable image
FROM scratch

COPY --from=builder /go/src/github.com/pingcap/tidb/bin/tidb-server /tidb-server
COPY --from=builder /usr/local/bin/dumb-init /usr/local/bin/dumb-init



WORKDIR /

EXPOSE 4000

ENTRYPOINT ["/usr/local/bin/dumb-init", "/tidb-server"]

