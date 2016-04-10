FROM golang:1.6

COPY . /go/src/github.com/pingcap/tidb

RUN cd /go/src/github.com/pingcap/tidb && \
    make godep && make server && \
    cp tidb-server/tidb-server /go/bin/ && \
    make clean

EXPOSE 4000

ENTRYPOINT ["tidb-server"]

