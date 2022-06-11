# For loading data to TiDB
FROM golang:1.18-buster as go-ycsb-builder
WORKDIR /go/src/github.com/pingcap/
RUN git clone https://github.com/pingcap/go-ycsb.git --depth=1 && \
    cd go-ycsb && \
    make

# For operating minio S3 compatible storage
FROM minio/mc as mc-builder

FROM golang:1.18-buster

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    vim \
    less \
    default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /go/src/github.com/pingcap/br
COPY . .

COPY --from=go-ycsb-builder /go/src/github.com/pingcap/go-ycsb/bin/go-ycsb /go/bin/go-ycsb
COPY --from=mc-builder /usr/bin/mc /usr/bin/mc

ENTRYPOINT ["/bin/bash"]
