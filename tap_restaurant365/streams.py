"""Stream type classes for tap-restaurant365."""

from __future__ import annotations

import typing as t
from pathlib import Path
from typing import Any
import requests
from datetime import timedelta, datetime
from dateutil import parser

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_restaurant365.client import Restaurant365Stream


class AccountsStream(Restaurant365Stream):
    """Define custom stream."""

    name = "accounts"
    path = "/GLAccount"
    primary_keys = ["glAccountId"]
    replication_key = "modifiedOn"

    schema = th.PropertiesList(
        th.Property("glAccountId", th.StringType),
        th.Property("locationNumber", th.StringType),
        th.Property("locationName", th.StringType),
        th.Property("locationId", th.StringType),
        th.Property("legalEntityNumber", th.StringType),
        th.Property("legalEntityName", th.StringType),
        th.Property("legalEntityId", th.StringType),
        th.Property("attribute1Number", th.StringType),
        th.Property("attribute1Name", th.StringType),
        th.Property("attribute1Id", th.StringType),
        th.Property("attribute2Number", th.StringType),
        th.Property("attribute2Name", th.StringType),
        th.Property("attribute2Id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("glAccountNumber", th.StringType),
        th.Property("glTypeClass", th.IntegerType),
        th.Property("glType", th.StringType),
        th.Property("operationalCategory", th.StringType),
        th.Property("createdBy", th.StringType),
        th.Property("createdOn", th.DateTimeType),
        th.Property("modifiedBy", th.StringType),
        th.Property("modifiedOn", th.DateTimeType)
    ).to_dict()


class TransactionsStream(Restaurant365Stream):
    """Define custom stream."""

    name = "transaction"
    path = "/Transaction"
    primary_keys = ["transactionId"]
    replication_key = "modifiedOn"
    paginate = True
    schema = th.PropertiesList(
        th.Property("transactionId", th.StringType),
        th.Property("locationId", th.StringType),
        th.Property("locationName", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("transactionNumber", th.StringType),
        th.Property("name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("companyId", th.StringType),
        th.Property("rowVersion", th.IntegerType),
        th.Property("isApproved", th.BooleanType),
        th.Property("createdOn", th.DateTimeType),
        th.Property("modifiedOn", th.DateTimeType),
        th.Property("createdBy", th.StringType),
        th.Property("modifiedBy", th.StringType)
    ).to_dict()

    def get_next_page_token(
        self, response: requests.Response, previous_token: t.Optional[t.Any]
    ) -> t.Optional[t.Any]:
        """Return a token for identifying next page or None if no more pages."""
        if self.paginate == True:
            # start_date = self.config.get("start_date")
            start_date = (parser.parse(self.tap_state["bookmarks"][self.name]['starting_replication_value']) + timedelta(seconds=1)) or parser.parse(self.config.get("start_date"))
            today = datetime.today()
            previous_token = previous_token or start_date
            next_token = (previous_token + timedelta(days=30)).replace(tzinfo=None)

            if (today - next_token).days < 30:
                self.paginate = False
            return next_token
        else:
            return None
        
    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        
        params: dict = {}
#x.strftime('%Y-%m-%dT%H:%M:%SZ')


        start_date = next_page_token or self.get_starting_time(context)
        end_date = start_date + timedelta(days = 30)
        if self.replication_key:
            params["$filter"] = f"{self.replication_key} ge {start_date.strftime('%Y-%m-%dT%H:%M:%SZ')} and {self.replication_key} lt {end_date.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        if self.name == "journal_entries":
            #set a date in the stream to check later to see if we need to keep calling to the stream
            params["$filter"] += f" and type eq 'Journal Entry'"
        if self.name == "bills":
            params["$filter"] += f" and type eq 'AP Invoice'"
        #   
        return params
        


class BillsStream(TransactionsStream):
    """Define custom stream."""

    name = "bills"
    path = "/Transaction"  #?$filter=type eq 'AP Invoices' 
    primary_keys = ["transactionId"]
    replication_key = "modifiedOn"
    paginate = True


class JournalEntriesStream(TransactionsStream):
    """Define custom stream."""

    name = "journal_entries"
    path = "/Transaction" #?$filter=type eq 'Journal Entry and modifiedOn ge '
    primary_keys = ["transactionId"]
    replication_key = "modifiedOn"
    paginate = True


class VendorsStream(Restaurant365Stream):
    """Define custom stream."""

    name = "vendors"
    path = "/Company"
    primary_keys = ["companyId"]
    replication_key = "modifiedOn"
    schema = th.PropertiesList(
        th.Property("companyId", th.StringType),
        th.Property("name", th.StringType),
        th.Property("companyNumber", th.StringType),
        th.Property("comment", th.StringType),
        th.Property("createdBy", th.StringType),
        th.Property("createdOn", th.DateTimeType),
        th.Property("modifiedBy", th.StringType),
        th.Property("modifiedOn", th.DateTimeType),
    ).to_dict()

class ItemsStream(Restaurant365Stream):
    """Define custom stream."""

    name = "items"
    path = "/Item"
    primary_keys = ["itemId"]
    replication_key = "modifiedOn"
    schema = th.PropertiesList(
        th.Property("itemId", th.StringType),
        th.Property("name", th.StringType),
        th.Property("itemNumber", th.StringType),
        th.Property("category1", th.StringType),
        th.Property("category2", th.StringType),
        th.Property("category3", th.StringType),
        th.Property("createdBy", th.StringType),
        th.Property("createdOn", th.DateTimeType),
        th.Property("modifiedBy", th.StringType),
        th.Property("modifiedOn", th.DateTimeType),

    ).to_dict()
