# Copyright 2022 PingCAP, Inc.
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

# The current dockerfile is only used for development purposes. If used in a 
# production environment, please refer to https://github.com/PingCAP-QE/artifacts/blob/main/dockerfiles/cd/builders/tidb/Dockerfile.

# Builder image
FROM golang:1.21 as builder
WORKDIR /tidb

COPY . .

ARG GOPROXY
ENV GOPROXY ${GOPROXY}

RUN make server


FROM rockylinux:9-minimal

COPY --from=builder /tidb/bin/tidb-server /tidb-server

WORKDIR /
EXPOSE 4000
ENTRYPOINT ["/tidb-server"]
