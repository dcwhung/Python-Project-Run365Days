"""Build the processed data set consumed by the API and the static dashboard.

The pipeline has one intermediate representation, :class:`~run365days.export.records.ExportRecords`,
built once from the raw sources, and two writers that serialise it:

- :mod:`run365days.export.sqlite` writes ``run365.db`` for the GraphQL API
- :mod:`run365days.export.static_json` writes split JSON files for the static build

Both outputs are generated at build time and are never committed.
"""
