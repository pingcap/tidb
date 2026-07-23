# For loading data to TiDB
FROM golang:1.18.9-alpine@sha256:0494a1050388f866e09e59540408ead2a2968953e7093f6bfb048317c5d1f70a as go-ycsb-builder
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
FROM minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727 as mc-builder

FROM golang:1.18.9-alpine@sha256:0494a1050388f866e09e59540408ead2a2968953e7093f6bfb048317c5d1f70a

WORKDIR /go/src/github.com/pingcap/br
COPY . .

COPY --from=go-ycsb-builder /go/src/github.com/pingcap/go-ycsb/bin/go-ycsb /go/bin/go-ycsb
COPY --from=mc-builder /usr/bin/mc /usr/bin/mc

ENTRYPOINT ["/bin/bash"]
