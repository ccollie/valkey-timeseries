from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Union

"""Helpers to parse TS.OUTLIERS test responses.

This module provides small helper functions and lightweight dataclasses
used by the test-suite to interpret raw Valkey/Redis-style reply shapes
for the TS.OUTLIERS command in its different FORMAT modes (full/cleaned).
"""

AnomalySignal = Literal[-1, 0, 1]


def to_int(value: Any) -> int:
    if isinstance(value, bytes):
        return int(value.decode("utf-8"))
    return int(value)


def to_float(value: Any) -> float:
    if isinstance(value, bytes):
        return float(value.decode('utf-8'))
    return float(value)


def to_str(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _to_float_if_numeric(v: Any) -> Any:
    """Converts bytes to float if they represent a numeric value."""
    if isinstance(v, bytes):
        try:
            return float(v)
        except (ValueError, TypeError):
            return v
    return v


def maybe_map_from_kv_array(value: Any) -> Any:
    """Convert a flat key/value array into a dict when needed.

    Some clients return MAP responses as a flat list: [k1, v1, k2, v2, ...].
    When that shape is detected we convert it into a dict {k1: v1, k2: v2,...}.
    Non-list inputs are returned unchanged.
    """

    if isinstance(value, list):
        out: dict[str, Any] = {}
        if len(value) % 2 != 0:
            raise ValueError("Invalid key-value array length")
        for i in range(0, len(value), 2):
            key = to_str(value[i])
            val = _to_float_if_numeric(value[i + 1])
            out[key] = val
        return out
    return value


class AnomalyMethod(Enum):
    EWMA = "EWMA"
    CUSUM = "CUSUM"
    ESD = "ESD"
    ZSCORE = "ZScore"
    MODIFIED_ZSCORE = "ModifiedZScore"
    SMOOTHED_ZSCORE = "SmoothedZScore"
    MAD = "Mad"
    DOUBLE_MAD = "DoubleMAD"
    IQR = "InterquartileRange"
    RANDOM_CUT_FOREST = "RandomCutForest"

    @staticmethod
    def parse(value: Any) -> "AnomalyMethod":
        """Parse a method name (string/bytes) into an AnomalyMethod enum.

        Accepts a variety of aliases and is case-insensitive for common
        method names.
        """

        raw = to_str(value)

        normalized = raw.strip()
        aliases: dict[str, AnomalyMethod] = {
            "ewma": AnomalyMethod.EWMA,
            "esd": AnomalyMethod.ESD,
            "cusum": AnomalyMethod.CUSUM,
            "zscore": AnomalyMethod.ZSCORE,
            "modified-zscore": AnomalyMethod.MODIFIED_ZSCORE,
            "modifiedzscore": AnomalyMethod.MODIFIED_ZSCORE,
            "smoothed-zscore": AnomalyMethod.SMOOTHED_ZSCORE,
            "smoothedzscore": AnomalyMethod.SMOOTHED_ZSCORE,
            "mad": AnomalyMethod.MAD,
            "doublemad": AnomalyMethod.DOUBLE_MAD,
            "interquartilerange": AnomalyMethod.IQR,
            "iqr": AnomalyMethod.IQR,
            "randomcutforest": AnomalyMethod.RANDOM_CUT_FOREST,
            "rcf": AnomalyMethod.RANDOM_CUT_FOREST,
        }

        if normalized in aliases:
            return aliases[normalized]

        lower = normalized.lower()
        if lower in aliases:
            return aliases[lower]

        for m in AnomalyMethod:
            if m.value == normalized:
                return m

        raise ValueError(f"Unknown anomaly method: {raw!r}")


@dataclass(frozen=True, slots=True)
class Sample:
    """A single time-series sample.

    Represented as a tuple/array in replies: [timestamp, value],
    [timestamp, value, score], or [timestamp, value, score, signal].
    """

    timestamp: int
    value: float
    score: Optional[float] = None
    signal: AnomalySignal = 0  # type: ignore[assignment]

    def is_positive_anomaly(self) -> bool:
        return self.signal == 1

    def is_negative_anomaly(self) -> bool:
        return self.signal == -1

    def is_anomalous(self) -> bool:
        return self.signal != 0

    @staticmethod
    def parse(value: Any) -> "Sample":
        if not isinstance(value, Sequence) or len(value) not in (2, 3, 4):
            raise TypeError("Sample must be [timestamp, value], [timestamp, value, score], or [timestamp, value, score, signal]")
        timestamp = to_int(value[0])
        val = to_float(value[1])
        score = None
        signal: AnomalySignal = 0  # type: ignore[assignment]
        if len(value) == 3:
            score = to_float(value[2])
        elif len(value) == 4:
            score = to_float(value[2])
            signal_int = to_int(value[3])
            if signal_int not in (-1, 0, 1):
                raise ValueError(f"Invalid sample signal: {signal_int}")
            signal = signal_int  # type: ignore[assignment]

        return Sample(timestamp=timestamp, value=val, score=score, signal=signal)


@dataclass(frozen=True, slots=True)
class AnomalyEntry:
    """A detected anomaly entry.

    Represented as a 4-item array in replies: [timestamp, value, signal, score].
    `signal` is -1/0/1 indicating negative/none/positive signal, and
    `score` is a numeric anomaly score.
    """

    timestamp: int
    value: float
    signal: AnomalySignal
    score: float

    def is_positive(self) -> bool:
        return self.signal == 1

    def is_negative(self) -> bool:
        return self.signal == -1

    def is_anomalous(self) -> bool:
        return self.signal != 0

    @staticmethod
    def parse(value: Any) -> "AnomalyEntry":
        if not isinstance(value, Sequence) or len(value) != 4:
            raise TypeError(
                "AnomalyEntry must be a 4-item array: [timestamp, value, signal, score]"
            )

        signal_int = to_int(value[2])
        if signal_int not in (-1, 0, 1):
            raise ValueError(f"Invalid anomaly signal: {signal_int}")

        return AnomalyEntry(
            timestamp=to_int(value[0]),
            value=to_float(value[1]),
            signal=signal_int,  # type: ignore[assignment]
            score=to_float(value[3]),
        )


@dataclass(frozen=True, slots=True)
class MethodInfoFenced:
    lower_fence: float
    upper_fence: float


@dataclass(frozen=True, slots=True)
class MethodInfoSpc:
    control_limits: Tuple[float, float]
    center_line: float


@dataclass(frozen=True, slots=True)
class MethodInfoIsolationForest:
    average_path_length: float


MethodInfo = Union[MethodInfoFenced, MethodInfoSpc, MethodInfoIsolationForest]


@dataclass(frozen=True, slots=True)
class TSOutliersFullResult:
    method: AnomalyMethod
    parameters: dict[str, Any]
    samples: List[Sample]
    method_info: Optional[MethodInfo] = None
    threshold: Optional[float] = None  # Legacy, for backward compatibility

    @staticmethod
    def parse(value: Any) -> TSOutliersFullResult:
        """
        Parse a Valkey client response for `TS.OUTLIERS ... FORMAT full`.

        Expected shape (as Python types):
        - dict with keys: method, parameters, samples, optional method_info
        - samples: list of [timestamp, value, score, signal]
          where `signal` is one of -1, 0, 1
        - optional legacy outliers/anomalies list: [timestamp, value, signal, score]
        - method_info: dict with one of:
            * {lower_fence, upper_fence}
            * {control_limits: [low, high], center_line}
            * {average_path_length}
        """

        value = maybe_map_from_kv_array(value)

        if not isinstance(value, dict):
            raise TypeError(f"Expected dict for FORMAT full result, got {type(value)!r}")

        method = AnomalyMethod.parse(value.get("method"))
        parameters = maybe_map_from_kv_array(value.get("parameters", {}))

        # For backward compatibility, some tests might still rely on a top-level threshold
        threshold = parameters.get("threshold")

        raw_samples = value.get("samples") or []
        samples: List[Sample] = [Sample.parse(pair) for pair in raw_samples]

        # Backfill signal metadata from legacy outliers for older shapes that omit sample signal.
        anomalies_raw = value.get("outliers") or value.get("anomalies") or []
        if anomalies_raw and all(sample.signal == 0 for sample in samples):
            parsed_anomalies = [AnomalyEntry.parse(item) for item in anomalies_raw]
            by_ts: dict[int, AnomalyEntry] = {item.timestamp: item for item in parsed_anomalies}
            samples = [
                Sample(
                    timestamp=sample.timestamp,
                    value=sample.value,
                    score=sample.score if sample.score is not None else by_ts[sample.timestamp].score if sample.timestamp in by_ts else None,
                    signal=by_ts[sample.timestamp].signal if sample.timestamp in by_ts else 0,
                )
                for sample in samples
            ]

        method_info_val = value.get("method_info")
        method_info: Optional[MethodInfo] = None
        if isinstance(method_info_val, dict):
            if "lower_fence" in method_info_val and "upper_fence" in method_info_val:
                method_info = MethodInfoFenced(
                    lower_fence=to_float(method_info_val["lower_fence"]),
                    upper_fence=to_float(method_info_val["upper_fence"]),
                )
            elif "control_limits" in method_info_val and "center_line" in method_info_val:
                cl = method_info_val["control_limits"]
                method_info = MethodInfoSpc(
                    control_limits=(to_float(cl[0]), to_float(cl[1])),
                    center_line=to_float(method_info_val["center_line"]),
                )
        return TSOutliersFullResult(
            method=method,
            parameters=parameters,
            samples=samples,
            method_info=method_info,
            threshold=threshold,
        )

    def anomalies(self) -> List[Sample]:
        return [sample for sample in self.samples if sample.signal != 0]

    def anomaly_count(self) -> int:
        return len(self.anomalies())


@dataclass(frozen=True, slots=True)
class TSOutliersCleanedResult:
    """Result from TS.OUTLIERS with FORMAT cleaned.

    Attributes:
        samples: List of samples with anomalies removed (cleaned data)
    """
    samples: List[Sample]

    @staticmethod
    def parse(value: Any) -> "TSOutliersCleanedResult":
        """Parse the raw Valkey response into a TSOutliersCleanedResult.

        FORMAT cleaned now returns only an array of cleaned samples.
        """
        if not isinstance(value, Sequence):
            raise TypeError("FORMAT cleaned result must be an array of samples")

        samples = [Sample.parse(item) for item in value]

        return TSOutliersCleanedResult(samples=samples)

    def sample_count(self) -> int:
        """Return the number of cleaned samples."""
        return len(self.samples)


    def sample_values(self) -> List[float]:
        """Return list of cleaned sample values."""
        return [sample.value for sample in self.samples]
