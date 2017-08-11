FROM golang:1.6

MAINTAINER siddontang

COPY . /go/src/github.com/pingcap/pd

RUN cd /go/src/github.com/pingcap/pd/ && \
    make && \
    cp -f ./bin/pd-server /go/bin/pd-server

EXPOSE 2379 2380

ENTRYPOINT ["pd-server"]

