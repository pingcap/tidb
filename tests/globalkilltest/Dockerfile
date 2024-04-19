# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM rockylinux:9

RUN dnf update -y && dnf groupinstall 'Development Tools' -y
RUN dnf install procps-ng mysql -y

ENV GOLANG_VERSION 1.21.6
ENV ARCH amd64
ENV GOLANG_DOWNLOAD_URL https://dl.google.com/go/go$GOLANG_VERSION.linux-$ARCH.tar.gz
ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH
RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz \
    && tar -C /usr/local -xzf golang.tar.gz \
    && rm golang.tar.gz

RUN mkdir -p /go/src/github.com/pingcap/tidb
WORKDIR /go/src/github.com/pingcap/tidb

COPY go.mod .
COPY go.sum .
COPY pkg/parser/go.mod pkg/parser/go.mod
COPY pkg/parser/go.sum pkg/parser/go.sum

ARG GOPROXY
RUN GO111MODULE=on GOPROXY=${GOPROXY} go mod download
