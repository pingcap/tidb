FROM pingcap/alpine-glibc:alpine-3.14
RUN apk add --no-cache \
    curl
COPY bin/tidb-server /tidb-server
EXPOSE 4000
ENTRYPOINT ["/tidb-server"]
