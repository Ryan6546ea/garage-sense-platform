"""Deterministic condensation-risk domain rules shared by producers and consumers."""

import math

MAGNUS_A = 17.62
MAGNUS_B_CELSIUS = 243.12
CONDENSATION_HUMIDITY_THRESHOLD = 85.0
CONDENSATION_MAXIMUM_DEW_POINT_GAP = 3.0
FLOAT_TOLERANCE = 1e-9


def calculate_dew_point(
    temperature: float,
    humidity: float,
) -> float | None:
    """Return Magnus dew point in Celsius, or none outside its valid RH domain."""

    if (
        not math.isfinite(temperature)
        or not math.isfinite(humidity)
        or humidity <= 0
        or humidity > 100
        or math.isclose(temperature, -MAGNUS_B_CELSIUS, abs_tol=FLOAT_TOLERANCE)
    ):
        return None
    gamma = math.log(humidity / 100) + (
        MAGNUS_A * temperature
    ) / (MAGNUS_B_CELSIUS + temperature)
    denominator = MAGNUS_A - gamma
    if math.isclose(denominator, 0, abs_tol=FLOAT_TOLERANCE):
        return None
    dew_point = MAGNUS_B_CELSIUS * gamma / denominator
    return dew_point if math.isfinite(dew_point) else None


def calculate_condensation_risk(
    temperature: float,
    humidity: float,
    *,
    maximum_dew_point_gap: float = CONDENSATION_MAXIMUM_DEW_POINT_GAP,
) -> bool:
    """Apply the M4 functional-test threshold to actual published measurements."""

    if (
        humidity < CONDENSATION_HUMIDITY_THRESHOLD
        or humidity > 100
        or maximum_dew_point_gap < 0
    ):
        return False
    dew_point = calculate_dew_point(temperature, humidity)
    return (
        dew_point is not None
        and temperature - dew_point <= maximum_dew_point_gap + FLOAT_TOLERANCE
    )
