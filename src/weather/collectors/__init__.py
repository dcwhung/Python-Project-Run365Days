"""Scrapers for online weather sources.

``WeatherPageStructureError`` is re-exported because it is the one name in this
package a caller outside it has to be able to write down: :func:`warnings.fetch_day`
and :func:`warnings.fetch_range` both declare it in their public ``Raises:``, so
catching it is part of their documented contract. It is defined in ``_parsing``
beside the landmark lookup that raises it -- the two are read together, and its
docstring explains itself in terms of that function -- but a leading underscore
says "this may be renamed or split without notice", which is the opposite of
what a declared failure mode may promise. The helpers there stay private; only
the exception is contract (W-032).
"""

from run365days.weather.collectors._parsing import WeatherPageStructureError

__all__ = ["WeatherPageStructureError"]
