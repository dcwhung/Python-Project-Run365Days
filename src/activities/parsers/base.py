"""Abstract base class shared by the TCX, GPX and KML parsers."""

import contextlib
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from pathlib import Path

from run365days.activities.models import Activity


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
        """

    def parse_all(self, directory: Path) -> list[Activity]:
        """Parse every ``*.<FORMAT>`` file in a directory.

        Files that are skipped by the parser (wrong year, wrong sport) or that
        are not well-formed XML are silently dropped. Non-matching files such
        as ``.DS_Store`` are never opened.

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
            with contextlib.suppress(ValueError, AttributeError, ET.ParseError):
                activities.append(self.parse(fp))
        return activities
