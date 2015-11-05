FROM jeanblanchard/alpine-glibc

VOLUME /opt

RUN apk add --update wget git make ; \
    cd /opt ; \
    wget --no-check-certificate -c https://storage.googleapis.com/golang/go1.5.1.linux-amd64.tar.gz ; \
    tar zxvf go1.5.1.linux-amd64.tar.gz ; \
    export GOROOT=/opt/go ; \
    mkdir /opt/goworkspace ; \
    export GOPATH=/opt/goworkspace ; \
    export PATH=$GOROOT/bin:$GOPATH/bin:$PATH ; \
    go get -v github.com/pingcap/tidb ; \
    cd $GOPATH/src/github.com/pingcap/tidb ; \
    make ; make server ; cp tidb-server/tidb-server /usr/bin/ ; \
    apk del run-parts openssl lua5.2-libs lua5.2 ncurses-terminfo-base ncurses-widec-libs lua 5.2-posix \
ca-certificates libssh2 curl expat pcre git wget make

EXPOSE 4000

CMD ["/usr/bin/tidb-server"]

