FROM golang:1.6

COPY . /go/src/github.com/pingcap/tidb

RUN cd /go/src/github.com/pingcap/tidb && \
    make parser && make server && \
    mv tidb-server/tidb-server /tidb-server && \
    make clean

EXPOSE 4000

ENTRYPOINT ["/tidb-server"]

