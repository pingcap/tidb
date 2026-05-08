# Copyright 2026 PingCAP, Inc.
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

FROM golang:1.25.9 AS builder

WORKDIR /tidb

COPY . .

RUN go install github.com/bazelbuild/bazelisk@latest
RUN bazelisk build --norun_validations //cmd/mirror && \
    mirror_bin="$(bazelisk cquery --output=files //cmd/mirror | head -n 1)" && \
    mkdir -p /opt/tidb-mirror && \
    cp "${mirror_bin}" /opt/tidb-mirror/mirror

FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/go /usr/local/go
COPY --from=builder /opt/tidb-mirror /opt/tidb-mirror
COPY .github/docker/go-module-mirror/entrypoint.sh /usr/local/bin/tidb-mirror-upload

ENV PATH="/usr/local/go/bin:${PATH}"

ENTRYPOINT ["/usr/local/bin/tidb-mirror-upload"]
