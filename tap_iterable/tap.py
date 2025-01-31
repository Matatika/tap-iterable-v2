"""Iterable tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_iterable import streams


class TapIterable(Tap):
    """Iterable tap class."""

    name = "tap-iterable"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            title="API key",
            description="Iterable API key",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Timestamp in ISO 8601 format to get data from (inclusive)",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="Timestamp in ISO 8601 format to get data up to (inclusive)",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.IterableStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.GroupsStream(self),
            streams.UsersStream(self),
        ]


if __name__ == "__main__":
    TapIterable.cli()
