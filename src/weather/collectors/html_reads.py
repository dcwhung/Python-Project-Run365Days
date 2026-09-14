"""Guarded reads for the scraped freemeteo and HKO pages.

``src.activities.parsers.base`` does the same job for ``xml.etree``, but the two
libraries disagree on what "missing" looks like and the guards cannot be shared:
ElementTree's ``find()`` returns ``None`` and its text lives on ``.text``, while
BeautifulSoup's ``find()`` returns ``None`` *or* a ``Tag`` whose ``.string`` is
independently ``None`` -- an emitted-but-empty ``<script>`` reads as present and
still has nothing to give. Chaining off either one is what CUI-0012 was: a
single absent element took down a whole day of observations.

Every helper here answers ``None`` for "not there" so the caller has to decide
what that means, rather than inheriting an ``AttributeError`` it never chose.
``cells`` extends the same contract to a row that arrived too short to index.
"""

from bs4.element import Tag


def child_string(parent: Tag, name: str) -> str | None:
    """Return the text of *parent*'s first ``<name>`` descendant.

    Args:
        parent: Element to search from.
        name: Tag name to look for.

    Returns:
        The descendant's string content, or ``None`` when the descendant is
        absent or holds no single string.
    """
    child = parent.find(name)
    if child is None or child.string is None:
        return None
    return str(child.string)


def child_attr(parent: Tag, name: str, attr: str) -> str | None:
    """Return an attribute of *parent*'s first ``<name>`` descendant.

    Args:
        parent: Element to search from.
        name: Tag name to look for.
        attr: Attribute to read off it.

    Returns:
        The attribute value, or ``None`` when the descendant is absent or does
        not carry the attribute.
    """
    child = parent.find(name)
    if child is None:
        return None
    value = child.get(attr)
    return None if value is None else str(value)


def cells(parent: Tag, minimum: int) -> list[Tag] | None:
    """Return *parent*'s ``<td>`` cells when it carries at least *minimum* of them.

    Args:
        parent: Element whose cells to read.
        minimum: Smallest cell count the caller can work with.

    Returns:
        The cells, or ``None`` when there are fewer than *minimum* -- so a short
        row becomes a decision the caller makes, not an ``IndexError`` it
        inherits from a header, a spacer or a page that shed a column.
    """
    found = parent.find_all("td")
    if len(found) < minimum:
        return None
    return found
