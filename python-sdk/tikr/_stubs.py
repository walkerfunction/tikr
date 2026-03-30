"""Auto-generate and import protobuf stubs for Tikr gRPC."""

from __future__ import annotations

import importlib
import os
import sys

_STUB_DIR: str | None = None


def _find_proto_dir() -> str:
    """Locate tikr.proto relative to the SDK package."""
    # Check common locations
    candidates = [
        os.path.join(os.path.dirname(__file__), "..", "..", "proto"),
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "proto"),
        os.environ.get("TIKR_PROTO_DIR", ""),
    ]
    for path in candidates:
        if path and os.path.isfile(os.path.join(path, "tikr.proto")):
            return os.path.abspath(path)
    raise FileNotFoundError(
        "Cannot find tikr.proto. Set TIKR_PROTO_DIR or install from the repo root."
    )


def _ensure_stubs() -> str:
    """Generate Python gRPC stubs if not already present. Returns stub directory."""
    global _STUB_DIR
    if _STUB_DIR is not None:
        return _STUB_DIR

    stub_dir = os.path.join(os.path.dirname(__file__), "_generated")
    pb2_path = os.path.join(stub_dir, "tikr_pb2.py")

    if not os.path.exists(pb2_path):
        proto_dir = _find_proto_dir()
        os.makedirs(stub_dir, exist_ok=True)

        from grpc_tools import protoc

        result = protoc.main([
            "grpc_tools.protoc",
            f"-I{proto_dir}",
            f"--python_out={stub_dir}",
            f"--grpc_python_out={stub_dir}",
            os.path.join(proto_dir, "tikr.proto"),
        ])
        if result != 0:
            raise RuntimeError(f"protoc failed with code {result}")

        init_path = os.path.join(stub_dir, "__init__.py")
        if not os.path.exists(init_path):
            open(init_path, "w").close()

    if stub_dir not in sys.path:
        sys.path.insert(0, stub_dir)

    _STUB_DIR = stub_dir
    return stub_dir


def get_pb2():
    """Return the tikr_pb2 module."""
    _ensure_stubs()
    return importlib.import_module("tikr_pb2")


def get_pb2_grpc():
    """Return the tikr_pb2_grpc module."""
    _ensure_stubs()
    return importlib.import_module("tikr_pb2_grpc")
