"""Abstract base class shared by the TCX, GPX and KML parsers."""

import logging
import math
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from pathlib import Path

from run365days.activities.models import Activity

logger = logging.getLogger(__name__)


# N818: an "Error" suffix would say the opposite of what this means. A skip is
# a routine filter decision, and telling it apart from an error is the point.
class ActivitySkipped(Exception):  # noqa: N818
    """The file parsed fine but does not belong in this run.

    Raised only for a deliberate filter decision -- wrong year, wrong sport --
    and therefore logged below the warning threshold. It is deliberately *not*
    a :class:`ValueError` subclass: ``float()``, ``int()`` and ``strptime()``
    all raise ``ValueError`` on bad data, and conflating the two is what let
    eight real exports disappear at DEBUG level (W-004).
    """


class ActivityParseError(Exception):
    """A mandatory element is missing or unreadable in an export file.

    Distinct from :class:`ActivitySkipped`: this one always means the file was
    meant to parse and did not, so it is logged as a warning.
    """


def element_text(parent: ET.Element, path: str, namespaces: dict[str, str]) -> str | None:
    """Return the stripped text of *path* under *parent*.

    Args:
        parent: Element to search from.
        path: ElementTree path expression.
        namespaces: Namespace prefix map.

    Returns:
        The element's text, or ``None`` when the element or its text is absent.
    """
    element = parent.find(path, namespaces)
    if element is None or element.text is None:
        return None
    return element.text.strip()


def required_text(parent: ET.Element, path: str, namespaces: dict[str, str]) -> str:
    """Return the stripped text of a mandatory *path* under *parent*.

    Args:
        parent: Element to search from.
        path: ElementTree path expression.
        namespaces: Namespace prefix map.

    Returns:
        The element's text.

    Raises:
        ActivityParseError: If the element is missing or empty.
    """
    text = element_text(parent, path, namespaces)
    if text is None:
        raise ActivityParseError(f"missing required element {path}")
    return text


def optional_float(parent: ET.Element, path: str, namespaces: dict[str, str]) -> float | None:
    """Return *path*'s text as a float, or ``None`` when absent or unparsable."""
    text = element_text(parent, path, namespaces)
    if text is None:
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    # "nan" / "inf" / "-inf" are valid float literals but never valid readings:
    # nan silently poisons any sum and inf poisons any min/max (W-005).
    return value if math.isfinite(value) else None


def optional_int(parent: ET.Element, path: str, namespaces: dict[str, str]) -> int | None:
    """Return *path*'s text as an int, or ``None`` when absent or unparsable."""
    value = optional_float(parent, path, namespaces)
    return None if value is None else int(value)


class BaseActivityParser(ABC):
    """Parse a single Garmin activity file into an Activity dataclass."""

    FORMAT: str = ""

    def __init__(self, current_year: int):
        self.current_year = current_year

    @abstractmethod
    def parse(self, file_path: Path) -> Activity:
        """Parse one file.

        Args:
            file_path: The export file to read.

        Returns:
            The parsed activity.

        Raises:
            ActivitySkipped: If the file should be skipped (wrong year or sport).
            ActivityParseError: If a mandatory element is missing.
        """

    def parse_all(self, directory: Path) -> list[Activity]:
        """Parse every ``*.<FORMAT>`` file in a directory.

        Files the parser deliberately skips (wrong year, wrong sport) and files
        that cannot be read at all are dropped, but every drop is logged with
        the file name and the reason. Deliberate skips are the normal case for
        a multi-year export folder so they stay at DEBUG; everything else is a
        data problem and is logged at WARNING. Non-matching files such as
        ``.DS_Store`` are never opened.

        Args:
            directory: Folder containing the export files.

        Returns:
            Parsed activities in file-name order.
        """
        pattern = f"*.{self.FORMAT}" if self.FORMAT else "*"
        activities = []
        for fp in sorted(directory.glob(pattern)):
            if not fp.is_file():
                continue
            try:
                activities.append(self.parse(fp))
            except ActivitySkipped as exc:
                # Wrong year / wrong sport is the normal case for most of the
                # export folder, so it stays below the warning threshold.
                logger.debug("Skipping %s: %s", fp.name, exc)
            except ActivityParseError as exc:
                logger.warning("Skipping %s: %s", fp.name, exc)
            except ET.ParseError as exc:
                logger.warning("Skipping %s: malformed XML (%s)", fp.name, exc)
            except ValueError as exc:
                # A bad float() / int() / strptime() anywhere in a parser means
                # unreadable data, never a deliberate skip -- it must be heard.
                logger.warning("Skipping %s: unreadable value (%s)", fp.name, exc)
        return activities
