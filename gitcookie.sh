# Copyright 2019 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

touch ~/.gitcookies
chmod 0600 ~/.gitcookies

git config --global http.cookiefile ~/.gitcookies

tr , \\t <<\__END__ >>~/.gitcookies
go.googlesource.com,FALSE,/,TRUE,2147483647,o,git-shenli.pingcap.com=1/rGvVlvFq_x9rxOmXqQe_rfcrjbOk6NSOHIQKhhsfidM
go-review.googlesource.com,FALSE,/,TRUE,2147483647,o,git-shenli.pingcap.com=1/rGvVlvFq_x9rxOmXqQe_rfcrjbOk6NSOHIQKhhsfidM
__END__

