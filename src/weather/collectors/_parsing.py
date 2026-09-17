"""Guarded reads shared by the weather collectors.

The collectors scrape pages nobody here controls, so every read of a nested
element has to say what happens when the element is gone. These helpers force
that choice at the call site: an *optional* part degrades to a documented
sentinel and keeps the row, while a missing *structural landmark* raises,
because a page without the landmark is no longer the page the collector was
written against (CUI-0012).

The same split the activity parsers settled on in ``parsers.base`` (AU-002),
rebuilt on the BeautifulSoup API rather than copied from the ElementTree one.
"""

from bs4 import Tag


class WeatherPageStructureError(RuntimeError):
    """A scraped page no longer carries a landmark the collector navigates by.

    Deliberately a :class:`RuntimeError` and not a :class:`ValueError`:
    ``float()``, ``int()`` and ``strptime()`` all raise ``ValueError`` while
    reading cells from these same pages, so an ``except ValueError`` written for
    one unreadable cell must not also swallow the news that the whole page
    changed shape.
    """


def section_after(html: str, marker: str) -> str:
    """Return the part of *html* that follows the first occurrence of *marker*.

    Args:
        html: The full page source.
        marker: The landmark text the wanted section begins after.

    Returns:
        Everything after *marker*.

    Raises:
        WeatherPageStructureError: *marker* does not appear in *html*.
    """
    start = html.find(marker)
    # str.find() reports "absent" as -1, and -1 is a perfectly usable slice
    # index: feeding it onward parses from an arbitrary offset near the top of
    # the page instead of parsing nothing, which is how a renamed HKO heading
    # used to yield a full set of plausible records scraped from the wrong
    # table. Absent has to be answered here, not passed on (CUI-0012).
    if start < 0:
        raise WeatherPageStructureError(f"page landmark not found: {marker!r}")
    return html[start + len(marker) :]


def child_attr(cell: Tag | None, name: str, attr: str, default: str = "") -> str:
    """Return attribute *attr* of the first *name* element inside *cell*.

    Args:
        cell: The element to search inside, or ``None``.
        name: Tag name of the wanted child, e.g. ``"img"``.
        attr: Attribute to read off that child, e.g. ``"src"``.
        default: Returned when the child or the attribute is absent.

    Returns:
        The attribute value, or *default*.
    """
    child = None if cell is None else cell.find(name)
    if not isinstance(child, Tag):
        return default
    value = child.get(attr, default)
    # bs4 hands back a list for attributes HTML defines as multi-valued; none of
    # the attributes read here is one, so anything but a string means the page
    # is not shaped the way this call assumed.
    return value if isinstance(value, str) else default


def child_string(cell: Tag | None, name: str) -> str | None:
    """Return the text of the first *name* element inside *cell*.

    Args:
        cell: The element to search inside, or ``None``.
        name: Tag name of the wanted child, e.g. ``"script"``.

    Returns:
        The child's string content, or ``None`` when the child is absent or
        holds no single string.
    """
    child = None if cell is None else cell.find(name)
    if not isinstance(child, Tag) or child.string is None:
        return None
    return str(child.string)
