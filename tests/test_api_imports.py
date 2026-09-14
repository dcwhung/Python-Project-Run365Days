"""The API must stay importable without the parsing stack (pandas, numpy, lxml)."""

import subprocess
import sys

HEAVY = ("pandas", "numpy", "lxml", "bs4", "requests")


def test_api_import_does_not_pull_in_parsing_dependencies():
    code = (
        "import sys; import run365days.api.app, run365days.api.schema, run365days.export.records; "
        f"heavy = [m for m in {HEAVY!r} if m in sys.modules]; "
        "print(','.join(heavy))"
    )
    # S603: argument list is a literal, the executable is sys.executable, and no
    # shell is involved -- there is no untrusted input to inject through.
    out = subprocess.run(  # noqa: S603
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "", f"API import loaded: {out.stdout.strip()}"
