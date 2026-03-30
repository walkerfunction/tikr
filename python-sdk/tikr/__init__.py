"""Tikr Python SDK — schema-agnostic time series edge rollup engine."""

from tikr.client import TikrClient
from tikr.models import Tick, Bar, SeriesInfo

__version__ = "0.1.0"
__all__ = ["TikrClient", "Tick", "Bar", "SeriesInfo"]
