"""Abstract base class shared by the TCX, GPX and KML parsers."""

import logging
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from pathlib import Path

from run365days.activities.models import Activity

logger = logging.getLogger(__name__)


class ActivityParseError(Exception):
    """A mandatory element is missing or unreadable in an export file.

    Distinct from :class:`ValueError`, which the parsers raise to skip a file
    on purpose (wrong year, wrong sport). This one always means the file was
    meant to parse and did not.
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
        return float(text)
    except ValueError:
        return None


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
            ValueError: If the file should be skipped (wrong year or sport).
            ActivityParseError: If a mandatory element is missing.
        """

    def parse_all(self, directory: Path) -> list[Activity]:
        """Parse every ``*.<FORMAT>`` file in a directory.

        Files the parser deliberately skips (wrong year, wrong sport) and files
        that cannot be read at all are dropped, but every drop is logged with
        the file name and the reason. Non-matching files such as ``.DS_Store``
        are never opened.

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
            except ActivityParseError as exc:
                logger.warning("Skipping %s: %s", fp.name, exc)
            except ET.ParseError as exc:
                logger.warning("Skipping %s: malformed XML (%s)", fp.name, exc)
            except ValueError as exc:
                # Wrong year / wrong sport is the normal case for most of the
                # export folder, so it stays below the warning threshold.
                logger.debug("Skipping %s: %s", fp.name, exc)
        return activities
