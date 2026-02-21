# ONNX Test Model

This directory contains `identity_scalar.onnx`, a minimal ONNX Identity model used for end-to-end
`MODEL_PREDICT` testing.

Model details:
- Input name: `x`
- Output name: `y`
- Element type: float32
- Shape: `[1]`

The model is generated from the public ONNX `ModelProto` schema (onnx/onnx v1.14.0) in text format
and encoded with `protoc`. It is licensed under Apache 2.0, consistent with ONNX.
