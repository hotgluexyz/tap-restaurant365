"""Restaurant365 tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_restaurant365 import streams


class TapRestaurant365(Tap):
    """Restaurant365 tap class."""

    name = "tap-restaurant365"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="Username of account",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="Password for account used to login",
        ),
        th.Property(
            "store_name",
            th.StringType,
            required=True,
            description="Your Company's domain ex: yourCompany in https://yourCompany.restaurant365.com",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.Restaurant365Stream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.AccountsStream(self),
            streams.JournalEntriesStream(self),
            streams.BillsStream(self),
            streams.VendorsStream(self),
            streams.ItemsStream(self),
            streams.EmployeesStream(self),
            streams.LaborDetailStream(self),
            streams.LocationsStream(self),
            streams.JobTitleStream(self),
            streams.POSEmployeeStream(self),
            streams.SalesDetailStream(self),
            streams.SalesEmployeeStream(self),
            streams.SalesPaymentStream(self),
            streams.EntityDeletedStream(self),
            streams.CreditMemosStream(self),
            streams.BankExpensesStream(self),
            streams.StockCountStream(self),
            streams.TransactionDetailsStream(self),
        ]


if __name__ == "__main__":
    TapRestaurant365.cli()
