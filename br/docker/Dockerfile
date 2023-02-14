# For loading data to TiDB
<<<<<<< HEAD
FROM golang:1.18-buster as go-ycsb-builder
=======
FROM golang:1.18.9-alpine as go-ycsb-builder
>>>>>>> 5999b2cebe (br: security update br image:1.18.1-alpine to 1.18.9-alpine (#40863))
WORKDIR /go/src/github.com/pingcap/
RUN git clone https://github.com/pingcap/go-ycsb.git --depth=1 && \
    cd go-ycsb && \
    make

# For operating minio S3 compatible storage
FROM minio/mc as mc-builder

<<<<<<< HEAD
FROM golang:1.18-buster

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    vim \
    less \
    default-mysql-client \
    && rm -rf /var/lib/apt/lists/*
=======
FROM golang:1.18.9-alpine
>>>>>>> 5999b2cebe (br: security update br image:1.18.1-alpine to 1.18.9-alpine (#40863))

WORKDIR /go/src/github.com/pingcap/br
COPY . .

COPY --from=go-ycsb-builder /go/src/github.com/pingcap/go-ycsb/bin/go-ycsb /go/bin/go-ycsb
COPY --from=mc-builder /usr/bin/mc /usr/bin/mc

ENTRYPOINT ["/bin/bash"]
