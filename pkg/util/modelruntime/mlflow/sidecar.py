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

import argparse
from collections import OrderedDict
import json
import os
import socket
import struct

import mlflow
import numpy as np

model_cache = OrderedDict()
cache_entries = 64


def recv_exact(conn, size):
    data = b""
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def read_msg(conn):
    size_data = recv_exact(conn, 4)
    if not size_data:
        return None
    size = struct.unpack(">I", size_data)[0]
    payload = recv_exact(conn, size)
    if payload is None:
        return None
    return json.loads(payload.decode("utf-8"))


def write_msg(conn, obj):
    payload = json.dumps(obj).encode("utf-8")
    conn.sendall(struct.pack(">I", len(payload)))
    conn.sendall(payload)


def normalize_outputs(outputs):
    if isinstance(outputs, dict):
        if len(outputs) != 1:
            raise ValueError("mlflow model returned multiple outputs")
        outputs = next(iter(outputs.values()))
    if hasattr(outputs, "to_numpy"):
        outputs = outputs.to_numpy()
    outputs = np.asarray(outputs, dtype=np.float32)
    if outputs.ndim == 1:
        outputs = outputs.reshape(-1, 1)
    return outputs


def handle_conn(conn):
    with conn:
        while True:
            msg = read_msg(conn)
            if msg is None:
                return
            path = msg["model_path"]
            if path in model_cache:
                model_cache.move_to_end(path)
            else:
                model_cache[path] = mlflow.pyfunc.load_model(path)
                if cache_entries > 0 and len(model_cache) > cache_entries:
                    model_cache.popitem(last=False)
            model = model_cache[path]
            inputs = np.array(msg["inputs"], dtype=np.float32)
            outputs = model.predict(inputs)
            outputs = normalize_outputs(outputs)
            write_msg(conn, {"outputs": outputs.tolist()})


def main():
    global cache_entries
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", required=True)
    parser.add_argument("--cache-entries", type=int, default=64)
    args = parser.parse_args()
    cache_entries = max(args.cache_entries, 1)

    if os.path.exists(args.socket):
        os.remove(args.socket)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(args.socket)
    server.listen(16)

    while True:
        conn, _ = server.accept()
        handle_conn(conn)


if __name__ == "__main__":
    main()
