FROM golang:1.6

COPY . /go/src/github.com/pingcap/tidb

RUN cd /go/src/github.com/pingcap/tidb && \
    make godep && make server && \
    cp tidb-server/tidb-server /go/bin/ && \
    rm -rf /go/src/*

EXPOSE 4000

ENTRYPOINT ["tidb-server"]

