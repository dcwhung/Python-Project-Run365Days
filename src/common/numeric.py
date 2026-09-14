"""Numeric coercions shared by the record layer, the dashboard and the collectors.

Four byte-identical copies of the float coercion and two spellings of the same
rounding guard had drifted apart (AU-024): ``export.records`` screened out
``±inf`` while ``dashboard.builder`` let it through. Keeping one copy is what
stops the two from diverging again.

Base layer, standard library only: ``run365days.export.records`` sits on the
API's import path, which ``tests/test_api_imports.py`` pins to the light
dependency set, so nothing here may reach for pandas, numpy or lxml.
"""

import math


def finite(value):
    """Return *value* when it is a finite number, otherwise ``None``.

    The parsers already reject nan and ±inf, so reaching here means a guard
    leaked. The two writers answer such a value differently -- ``json.dumps``
    refuses it while SQLite stores ``Infinity`` -- so this shared layer settles
    it once and both deployment targets stay in step (CUI-0001, CUI-0007).

    Args:
        value: Any object; non-numbers are treated as absent.

    Returns:
        The value unchanged, or ``None`` when it is not a finite number.
    """
    try:
        return value if math.isfinite(value) else None
    except TypeError:
        return None


def round_or_none(value, ndigits: int = 1) -> float | None:
    """Round *value* for storage, mapping anything not finite to ``None``.

    The float cast is deliberate: the matching SQLite columns are REAL, so a
    whole-number input left as an ``int`` would make the JSON writer emit
    ``330`` where the database holds ``330.0``.

    Args:
        value: Any object; non-numbers are treated as absent.
        ndigits: Decimal places to keep.

    Returns:
        The rounded value, or ``None`` when it is not a finite number.
    """
    if finite(value) is None:
        return None
    return round(float(value), ndigits)


def to_float(value) -> float | None:
    """Return *value* as a float, or ``None`` when it cannot be read as one.

    Args:
        value: A scraped string or any object convertible to ``float``.

    Returns:
        The parsed float, or ``None``.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def finite_float(value) -> float | None:
    """Parse *value* as a float, keeping it only when the result is finite.

    ``to_float`` is deliberately permissive because ``"inf"``, ``"-inf"`` and
    ``"nan"`` are legal Python float literals and the collectors want them
    preserved as parsed. A record on its way to the writers does not: pairing
    the two guards once here is what stops any of the nine weather call sites
    from remembering the cast and forgetting the bound (CUI-0009).

    Args:
        value: A scraped string or any object convertible to ``float``.

    Returns:
        The parsed float, or ``None`` when it is unreadable or not finite.
    """
    return finite(to_float(value))
