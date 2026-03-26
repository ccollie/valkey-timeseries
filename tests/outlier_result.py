from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Literal, Mapping, Optional, Sequence, Tuple, Union

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


def maybe_map_from_kv_array(value: Any) -> Any:
    # Some clients return MAP as a flat list: [k1, v1, k2, v2, ...]
    if isinstance(value, list):
        out: dict[str, Any] = {}
        for i in range(0, len(value), 2):
            out[to_str(value[i])] = value[i + 1]
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
    timestamp: int
    value: float
    score: Optional[float] = None

    @staticmethod
    def parse(value: Any) -> "Sample":
        if not isinstance(value, Sequence) or (len(value) != 2 and len(value) != 3):
            raise TypeError("Sample must be a 2- or 3-item array: [timestamp, value, score?]")
        timestamp = to_int(value[0])
        val = to_float(value[1])
        score = None
        if len(value) == 3:
            score = to_float(value[2])

        return Sample(timestamp=timestamp, value=val, score=score)


@dataclass(frozen=True, slots=True)
class AnomalyEntry:
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
    threshold: float
    samples: List[Sample]
    anomalies: List[AnomalyEntry]
    method_info: Optional[MethodInfo] = None

    @staticmethod
    def parse(value: Any) -> TSOutliersFullResult:
        value = maybe_map_from_kv_array(value)
        """
        Parse a Valkey client response for `TS.OUTLIERS ... FORMAT full`.

        Expected shape (as Python types):
        - dict with keys: method, threshold, samples, scores, anomalies, optional method_info
        - samples: list of [timestamp, value, Score?]
        - anomalies: list of ints (-1, 0, 1)
        - method_info: dict with one of:
            * {lower_fence, upper_fence}
            * {control_limits: [low, high], center_line}
            * {average_path_length}
        """
        if not isinstance(value, dict):
            raise TypeError(f"Expected dict for FORMAT full result, got {type(value)!r}")

        method = AnomalyMethod.parse(value.get("method"))
        threshold = to_float(value.get("threshold"))

        raw_samples = value.get("samples") or []
        samples: List[Sample] = [Sample.parse(pair) for pair in raw_samples]

        # Accept either 'outliers' or legacy 'anomalies' key for anomaly entries
        anomalies_raw = value.get("outliers") or value.get("anomalies") or []
        anomalies: List[AnomalyEntry] = []
        for raw_anomaly in anomalies_raw:
            anomaly = AnomalyEntry.parse(raw_anomaly)
            anomalies.append(anomaly)  # type: ignore[arg-type]

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
            elif "average_path_length" in method_info_val:
                method_info = MethodInfoIsolationForest(
                    average_path_length=to_float(method_info_val["average_path_length"])
                )

        return TSOutliersFullResult(
            method=method,
            threshold=threshold,
            samples=samples,
            anomalies=anomalies,
            method_info=method_info,
        )

    def anomaly_count(self) -> int:
        return len(self.anomalies)


@dataclass(frozen=True, slots=True)
class TSOutliersCleanedResult:
    """Result from TS.OUTLIERS with FORMAT cleaned.

    Attributes:
        samples: List of samples with anomalies removed (cleaned data)
        anomalies: List of detected anomaly entries
    """
    samples: List[Sample]
    anomalies: List[AnomalyEntry]

    @staticmethod
    def parse(value: Any) -> "TSOutliersCleanedResult":
        """Parse the raw Valkey response into a TSOutliersCleanedResult.

        Accepts responses shaped either with an 'outliers' key (preferred) or
        a legacy 'anomalies' key for the list of anomaly entries.
        """
        value = maybe_map_from_kv_array(value)

        if not isinstance(value, Mapping):
            raise TypeError("FORMAT cleaned result must be a map with keys: samples and outliers/anomalies")

        raw_samples = value.get("samples") or []
        raw_anomalies = value.get("outliers") or value.get("anomalies") or []

        samples = [Sample.parse(item) for item in raw_samples]
        anomalies = [AnomalyEntry.parse(item) for item in raw_anomalies]

        return TSOutliersCleanedResult(samples=samples, anomalies=anomalies)

    def anomaly_count(self) -> int:
        """Return the number of detected anomalies."""
        return len(self.anomalies)

    def sample_count(self) -> int:
        """Return the number of cleaned samples."""
        return len(self.samples)

    def anomaly_values(self) -> List[float]:
        """Return the list of anomaly values."""
        return [entry.value for entry in self.anomalies]

    def sample_values(self) -> List[float]:
        """Return list of cleaned sample values."""
        return [sample.value for sample in self.samples]
