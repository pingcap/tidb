import json
import struct
import sys

import mlflow
import numpy as np

model_cache = {}


def read_msg():
    size_data = sys.stdin.buffer.read(4)
    if not size_data:
        return None
    size = struct.unpack(">I", size_data)[0]
    payload = sys.stdin.buffer.read(size)
    return json.loads(payload.decode("utf-8"))


def write_msg(obj):
    payload = json.dumps(obj).encode("utf-8")
    sys.stdout.buffer.write(struct.pack(">I", len(payload)))
    sys.stdout.buffer.write(payload)
    sys.stdout.buffer.flush()


while True:
    msg = read_msg()
    if msg is None:
        break
    path = msg["model_path"]
    if path not in model_cache:
        model_cache[path] = mlflow.pyfunc.load_model(path)
    model = model_cache[path]
    inputs = np.array(msg["inputs"], dtype=np.float32)
    outputs = model.predict(inputs)
    write_msg({"outputs": outputs.astype(np.float32).tolist()})
