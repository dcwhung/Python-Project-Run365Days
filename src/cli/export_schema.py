"""Print the GraphQL schema as SDL (for front-end code generation).

Usage
-----
    run365-schema > frontend/schema.graphql
    run365-schema --check frontend/schema.graphql   # exit 1 if the file is stale
"""

import argparse
import sys
from pathlib import Path

from run365days.api.schema import schema


def sdl() -> str:
    """Return the schema definition language text with a trailing newline."""
    return schema.as_str().rstrip("\n") + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--check", type=Path, default=None, help="compare against this file")
    args = ap.parse_args()
    if args.check is None:
        sys.stdout.write(sdl())
        return
    current = args.check.read_text() if args.check.exists() else ""
    if current != sdl():
        print(f"{args.check} is out of date; run `run365-schema > {args.check}`", file=sys.stderr)
        sys.exit(1)
    print(f"{args.check} is up to date")


if __name__ == "__main__":
    main()
