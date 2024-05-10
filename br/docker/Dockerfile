# For loading data to TiDB
FROM golang:1.18.9-alpine as go-ycsb-builder
WORKDIR /go/src/github.com/pingcap/

RUN apk add --no-cache \
    wget \
    make \
    git \
    binutils-gold \
    gcc \
    musl-dev

RUN git clone https://github.com/pingcap/go-ycsb.git --depth=1 && \
    cd go-ycsb && \
    make

# For operating minio S3 compatible storage
FROM minio/mc as mc-builder

FROM golang:1.18.9-alpine

WORKDIR /go/src/github.com/pingcap/br
COPY . .

COPY --from=go-ycsb-builder /go/src/github.com/pingcap/go-ycsb/bin/go-ycsb /go/bin/go-ycsb
COPY --from=mc-builder /usr/bin/mc /usr/bin/mc

ENTRYPOINT ["/bin/bash"]
