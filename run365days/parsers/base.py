from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from run365days.models.activity import Activity


class BaseActivityParser(ABC):
    """Parse a single Garmin activity file into an Activity dataclass."""

    FORMAT: str = ""

    def __init__(self, current_year: int):
        self.current_year = current_year

    @abstractmethod
    def parse(self, file_path: Path) -> Activity:
        """Parse one file and return an Activity, or raise ValueError if skipped."""

    def parse_all(self, directory: Path) -> List[Activity]:
        """Parse every file in *directory* and return a list of Activities."""
        activities = []
        for fp in sorted(directory.glob("*")):
            if fp.is_file():
                try:
                    activities.append(self.parse(fp))
                except (ValueError, AttributeError):
                    pass
        return activities
