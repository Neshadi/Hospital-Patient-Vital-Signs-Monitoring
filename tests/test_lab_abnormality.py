"""
Unit tests for reference-range parsing / abnormality detection logic mirrored
from airflow/dags/daily_risk_report_dag.py (kept dependency-free here so it
can run without an Airflow environment installed).
"""


def is_abnormal(value: float, reference_range: str) -> bool:
    low, high = [float(x) for x in reference_range.split("-")]
    return not (low <= value <= high)


def test_value_within_range_is_normal():
    assert is_abnormal(7.5, "4.0-11.0") is False


def test_value_above_range_is_abnormal():
    assert is_abnormal(15.0, "4.0-11.0") is True


def test_value_below_range_is_abnormal():
    assert is_abnormal(2.0, "4.0-11.0") is True


def test_boundary_values_are_normal():
    assert is_abnormal(4.0, "4.0-11.0") is False
    assert is_abnormal(11.0, "4.0-11.0") is False


def test_risk_score_thresholds():
    def risk_level(score: float) -> str:
        if score >= 70:
            return "critical"
        if score >= 40:
            return "high"
        if score >= 15:
            return "medium"
        return "low"

    assert risk_level(10) == "low"
    assert risk_level(15) == "medium"
    assert risk_level(45) == "high"
    assert risk_level(80) == "critical"
