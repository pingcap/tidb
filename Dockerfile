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

# Builder image
FROM alpine:edge as builder

ADD . https://raw.githubusercontent.com/njhallett/apk-fastest-mirror/c4ca44caef3385d830fea34df2dbc2ba4a17e021/apk-fastest-mirror.sh ./proxy
RUN sh ./proxy/apk-fastest-mirror.sh -t 50 && apk add --no-cache git build-base go

COPY . /tidb
ARG GOPROXY
RUN export GOPROXY=${GOPROXY} && cd /tidb && make server

FROM alpine:latest

COPY --from=builder /tidb/bin/tidb-server /tidb-server

WORKDIR /
EXPOSE 4000
ENTRYPOINT ["/tidb-server"]
