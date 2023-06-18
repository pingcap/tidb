FROM ubuntu:focal
ADD go1.20.1.patch /bootstrap/diff.patch
ADD build.sh /
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    clang-10 \
    cmake \
    curl \
    git \
    gnupg2 \
    make \
    python-is-python3 \
    python3 \
    python3.8-venv
ENV GOLANG_VERSION 1.20.5
ENV ARCH amd64
ENV GOLANG_DOWNLOAD_URL https://dl.google.com/go/go$GOLANG_VERSION.linux-$ARCH.tar.gz
ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH
RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz \
	&& tar -C /usr/local -xzf golang.tar.gz \
	&& rm golang.tar.gz
