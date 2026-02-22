import argparse
import json
import os
import socket
import struct

import mlflow
import numpy as np

model_cache = {}


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


def handle_conn(conn):
    with conn:
        while True:
            msg = read_msg(conn)
            if msg is None:
                return
            path = msg["model_path"]
            if path not in model_cache:
                model_cache[path] = mlflow.pyfunc.load_model(path)
            model = model_cache[path]
            inputs = np.array(msg["inputs"], dtype=np.float32)
            outputs = model.predict(inputs)
            write_msg(conn, {"outputs": outputs.astype(np.float32).tolist()})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", required=True)
    parser.add_argument("--cache-entries", type=int, default=64)
    args = parser.parse_args()

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
