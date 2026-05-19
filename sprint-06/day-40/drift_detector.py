#!/usr/bin/env python3
"""
drift_detector.py — Day 40 | Model Drift Detection
====================================================
Compares current model accuracy against baseline.
Logs WARNING when drift exceeds threshold.

Usage (standalone):
    python drift_detector.py

Usage (in Airflow task):
    from drift_detector import detect_drift
    result = detect_drift(current_accuracy=0.85, baseline_accuracy=0.95)
"""

from __future__ import annotations

import sys
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
from logger import get_pipeline_logger

logger = get_pipeline_logger("drift_detector")

DRIFT_THRESHOLD  = 0.05   # 5% accuracy drop = drift detected
WARN_THRESHOLD   = 0.02   # 2% drop = warning (not yet drift)


@dataclass
class DriftResult:
    """Result of a drift detection check."""
    baseline_accuracy: float
    current_accuracy:  float
    delta:             float
    drift_detected:    bool
    warning_only:      bool
    status:            str        # "ok", "warning", "drift"
    message:           str
    checked_at:        str

    @property
    def as_dict(self) -> dict:
        return {
            "baseline_accuracy": self.baseline_accuracy,
            "current_accuracy":  self.current_accuracy,
            "delta":             round(self.delta, 4),
            "drift_detected":    self.drift_detected,
            "warning_only":      self.warning_only,
            "status":            self.status,
            "message":           self.message,
            "checked_at":        self.checked_at,
        }


def detect_drift(
    current_accuracy: float,
    baseline_accuracy: float,
    drift_threshold: float = DRIFT_THRESHOLD,
    warn_threshold:  float = WARN_THRESHOLD,
) -> DriftResult:
    """
    Compare current model accuracy against baseline.
    Returns DriftResult with status and message.
    """
    delta = baseline_accuracy - current_accuracy

    if delta >= drift_threshold:
        status   = "drift"
        detected = True
        warning  = False
        message  = (
            f"🚨 DRIFT DETECTED: accuracy dropped {delta:.1%} "
            f"(baseline={baseline_accuracy:.1%} → current={current_accuracy:.1%}). "
            f"Consider retraining with fresh data."
        )
        logger.warning(message)

    elif delta >= warn_threshold:
        status   = "warning"
        detected = False
        warning  = True
        message  = (
            f"⚠️ Accuracy declining: {delta:.1%} drop "
            f"(baseline={baseline_accuracy:.1%} → current={current_accuracy:.1%}). "
            f"Monitor closely."
        )
        logger.warning(message)

    else:
        status   = "ok"
        detected = False
        warning  = False
        message  = (
            f"✅ No drift: accuracy delta={delta:.1%} "
            f"(baseline={baseline_accuracy:.1%}, current={current_accuracy:.1%})"
        )
        logger.info(message)

    return DriftResult(
        baseline_accuracy=baseline_accuracy,
        current_accuracy=current_accuracy,
        delta=delta,
        drift_detected=detected,
        warning_only=warning,
        status=status,
        message=message,
        checked_at=datetime.now().isoformat(),
    )


def load_baseline_accuracy(metadata_path: Path) -> float:
    """
    Load baseline accuracy from model metadata JSON.
    Returns a default if not found (first run).
    """
    if not metadata_path.exists():
        logger.info("No baseline metadata found — using default 0.90")
        return 0.90

    with open(metadata_path) as f:
        meta = json.load(f)
    return float(meta.get("baseline_accuracy", 0.90))


def save_baseline_accuracy(metadata_path: Path, accuracy: float) -> None:
    """Update baseline accuracy in metadata JSON."""
    meta = {}
    if metadata_path.exists():
        with open(metadata_path) as f:
            meta = json.load(f)
    meta["baseline_accuracy"] = round(accuracy, 4)
    meta["baseline_set_at"]   = datetime.now().isoformat()
    with open(metadata_path, "w") as f:
        json.dump(meta, f, indent=2)
    logger.info(f"Baseline accuracy saved: {accuracy:.4f}")


if __name__ == "__main__":
    # Demo
    print("\nDrift Detection Demo")
    print("=" * 40)
    for current in [0.95, 0.92, 0.89, 0.84]:
        result = detect_drift(current, baseline_accuracy=0.95)
        print(f"Current={current:.2f} → {result.status.upper()}: {result.message[:60]}")