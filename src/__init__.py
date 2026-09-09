"""run365days: analytics for a one-run-every-day challenge.

Features are organised as sub-packages:

- ``activities`` - Garmin TCX / GPX / KML parsing, activity model, MET metrics
- ``weather``    - Hong Kong Observatory and freemeteo collectors and models
- ``weight``     - daily body-weight parsing and derived metrics
- ``dashboard``  - builds the payload for the static web dashboard
- ``common``     - configuration, geo and time helpers
- ``cli``        - command-line entry points
"""
