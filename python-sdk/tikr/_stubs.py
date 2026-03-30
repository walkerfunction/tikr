"""Auto-generate and import protobuf stubs for Tikr gRPC."""

from __future__ import annotations

import importlib
import os
import sys
import tempfile

_STUB_DIR: str | None = None


def _find_proto_dir() -> str:
    """Locate tikr.proto relative to the SDK package."""
    candidates = [
        # Vendored proto inside the installed package
        os.path.join(os.path.dirname(__file__), "proto"),
        # Repo-relative layouts (editable install)
        os.path.join(os.path.dirname(__file__), "..", "..", "proto"),
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "proto"),
        # Explicit override
        os.environ.get("TIKR_PROTO_DIR", ""),
    ]
    for path in candidates:
        if path and os.path.isfile(os.path.join(path, "tikr.proto")):
            return os.path.abspath(path)
    raise FileNotFoundError(
        "Cannot find tikr.proto. Ensure tikr.proto is bundled in the package "
        "(e.g. under tikr/proto/), set TIKR_PROTO_DIR, or install from the "
        "repository root with the top-level proto/ directory available."
    )


def _ensure_stubs() -> str:
    """Generate Python gRPC stubs if not already present. Returns stub directory."""
    global _STUB_DIR
    if _STUB_DIR is not None:
        return _STUB_DIR

    # Use a user-writable cache dir to avoid issues with read-only site-packages
    cache_base = os.environ.get(
        "TIKR_STUB_CACHE", os.path.join(tempfile.gettempdir(), "tikr_stubs")
    )
    os.makedirs(cache_base, exist_ok=True)
    pb2_path = os.path.join(cache_base, "tikr_pb2.py")

    if not os.path.exists(pb2_path):
        proto_dir = _find_proto_dir()

        from grpc_tools import protoc

        result = protoc.main(
            [
                "grpc_tools.protoc",
                f"-I{proto_dir}",
                f"--python_out={cache_base}",
                f"--grpc_python_out={cache_base}",
                os.path.join(proto_dir, "tikr.proto"),
            ]
        )
        if result != 0:
            raise RuntimeError(f"protoc failed with code {result}")

        init_path = os.path.join(cache_base, "__init__.py")
        if not os.path.exists(init_path):
            open(init_path, "w").close()

    if cache_base not in sys.path:
        sys.path.insert(0, cache_base)

    _STUB_DIR = cache_base
    return cache_base


def get_pb2():
    """Return the tikr_pb2 module."""
    _ensure_stubs()
    return importlib.import_module("tikr_pb2")


def get_pb2_grpc():
    """Return the tikr_pb2_grpc module."""
    _ensure_stubs()
    return importlib.import_module("tikr_pb2_grpc")
