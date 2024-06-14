"""Stream type classes for tap-restaurant365."""

from __future__ import annotations

import typing as t
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any

import requests
from dateutil import parser
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_restaurant365.client import Restaurant365Stream


class LimitedTimeframeStream(Restaurant365Stream):
    """parent class stream for override/pagination"""

    name = "vendors"
    path = "/Company"

    def get_next_page_token(
        self, response: requests.Response, previous_token: t.Optional[t.Any]
    ) -> t.Optional[t.Any]:
        """Return a token for identifying next page or None if no more pages."""
        # Check if pagination is enabled
        if self.paginate:
            data = response.json()
            # Check for the presence of a next page link in the response data. nextLink is only present if there are more than 5000 records in filter response.
            if "@odata.nextLink" in data:
                # Increment the skip counter for pagination
                self.skip += 5000
                # Update the previous token if it exists
                if previous_token:
                    previous_token = previous_token["token"]
                # Return the next page token and the updated skip value
                return {"token": previous_token, "skip": self.skip}
            else:
                # Reset skip value for a new pagination sequence
                self.skip = 0
                # Determine the starting replication value for data extraction
                replication_key_value = self.tap_state["bookmarks"][self.name][
                    "starting_replication_value"
                ]
                # Update the replication key value if progress markers are present
                if "progress_markers" in self.tap_state["bookmarks"][self.name]:
                    replication_key_value = self.tap_state["bookmarks"][self.name][
                        "progress_markers"
                    ]["replication_key_value"]

                # Calculate the start date for data extraction
                start_date = (
                    parser.parse(replication_key_value) + timedelta(seconds=1)
                ) or parser.parse(self.config.get("start_date"))
                today = datetime.today()
                # Adjust the start date based on the previous token if applicable (will occur if progress marker is unable to find a value in empty data response)
                if (
                    previous_token
                    and "token" in previous_token
                    and previous_token["token"]
                    and start_date.replace(tzinfo=None)
                    <= previous_token["token"].replace(tzinfo=None)
                ):
                    start_date = previous_token["token"] + timedelta(
                        days=self.days_delta
                    )
                next_token = start_date.replace(tzinfo=None)

                # Disable pagination if the next token's date is in the future
                if (today - next_token).days < 0:
                    self.paginate = False
                # Return the next token and the current skip value
                return {"token": next_token, "skip": self.skip}
        else:
            # Return None if pagination is not enabled
            return None

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:

        params: dict = {}
        token_date = None
        skip = 0
        if next_page_token:
            token_date, skip = next_page_token["token"], next_page_token["skip"]
        start_date = token_date or self.get_starting_time(context)
        end_date = start_date + timedelta(days=self.days_delta)
        if self.replication_key:
            params[
                "$filter"
            ] = f"{self.replication_key} ge {start_date.strftime('%Y-%m-%dT%H:%M:%SZ')} and {self.replication_key} lt {end_date.strftime('%Y-%m-%dT23:59:59Z')}"
            # Order by replication key so the response is consistent
            params["$orderby"] = f"{self.replication_key}"
        if self.name == "journal_entries":
            # set a date in the stream to check later to see if we need to keep calling to the stream
            params["$filter"] += f" and type eq 'Journal Entry'"
        if self.name == "bills":
            params["$filter"] += f" and type eq 'AP Invoice'"
        if self.name == "credit_memos":
            params["$filter"] += f" and type eq 'AP Credit Memo'"
        if self.name == "stock_count":
            params["$filter"] += f" and type eq 'Stock Count'"
        if self.name == "bank_expenses":
            params["$filter"] += f" and type eq 'Bank Expense'"
        #
        if skip > 0:
            params["$skip"] = skip
        return params


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
        th.Property("modifiedOn", th.DateTimeType),
    ).to_dict()


class TransactionsParentStream(LimitedTimeframeStream):
    """Define custom stream."""

    name = "transaction_parent_stream"
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
        th.Property("modifiedBy", th.StringType),
    ).to_dict()


class BillsStream(TransactionsParentStream):
    """Define custom stream."""

    name = "bills"
    path = "/Transaction"  # ?$filter=type eq 'AP Invoices'
    primary_keys = ["transactionId"]
    replication_key = "modifiedOn"
    paginate = True


class JournalEntriesStream(TransactionsParentStream):
    """Define custom stream."""

    name = "journal_entries"
    path = "/Transaction"  # ?$filter=type eq 'Journal Entry and modifiedOn ge '
    primary_keys = ["transactionId"]
    replication_key = "modifiedOn"
    paginate = True


class CreditMemosStream(TransactionsParentStream):
    """Define custom stream."""

    name = "credit_memos"
    path = "/Transaction"  # ?$filter=type eq 'AP Credit memo and modifiedOn ge '
    primary_keys = ["transactionId"]
    replication_key = "modifiedOn"
    paginate = True


class StockCountStream(TransactionsParentStream):
    """Define custom stream."""

    name = "stock_count"
    path = "/Transaction"  # ?$filter=type eq 'Stock Count and modifiedOn ge '
    primary_keys = ["transactionId"]
    replication_key = "modifiedOn"
    paginate = True


class BankExpensesStream(TransactionsParentStream):
    """Define custom stream."""

    name = "bank_expenses"
    path = "/Transaction"  # ?$filter=type eq 'Bank Expense and modifiedOn ge '
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


class LocationsStream(Restaurant365Stream):
    """Define custom stream."""

    name = "locations"
    path = "/Location"
    primary_keys = ["locationId"]
    replication_key = "modifiedOn"
    schema = th.PropertiesList(
        th.Property("locationId", th.StringType),
        th.Property("name", th.StringType),
        th.Property("locationNumber", th.StringType),
        th.Property("legalEntityId", th.StringType),
        th.Property("legalEntityNumber", th.StringType),
        th.Property("legalEntityName", th.StringType),
        th.Property("createdBy", th.StringType),
        th.Property("createdOn", th.DateTimeType),
        th.Property("modifiedBy", th.StringType),
        th.Property("modifiedOn", th.DateTimeType),
    ).to_dict()


class EmployeesStream(Restaurant365Stream):
    """Define custom stream."""

    name = "employees"
    path = "/Employee"
    primary_keys = ["employeeId"]
    replication_key = "modifiedOn"
    schema = th.PropertiesList(
        th.Property("employeeId", th.StringType),
        th.Property("fullName", th.StringType),
        th.Property("address1", th.StringType),
        th.Property("address2", th.StringType),
        th.Property("allowTextMessaging", th.BooleanType),
        th.Property("birthdayDay", th.NumberType),
        th.Property("birthdayMonth", th.NumberType),
        th.Property("city", th.StringType),
        th.Property("firstName", th.StringType),
        th.Property("hireDate", th.DateTimeType),
        th.Property("lastName", th.StringType),
        th.Property("middleName", th.StringType),
        th.Property("mobilePhone", th.StringType),
        th.Property("multipleLocations", th.BooleanType),
        th.Property("payrollID", th.StringType),
        th.Property("phoneNumber", th.StringType),
        th.Property("posid", th.StringType),
        th.Property("state", th.StringType),
        th.Property("zipCode", th.StringType),
        th.Property("primaryLocation_id", th.StringType),
        th.Property("inactive", th.BooleanType),
        th.Property("email", th.StringType),
        th.Property("birthday", th.DateTimeType),
        th.Property("createdBy", th.StringType),
        th.Property("createdOn", th.DateTimeType),
        th.Property("modifiedBy", th.StringType),
        th.Property("modifiedOn", th.DateTimeType),
    ).to_dict()


class JobTitleStream(Restaurant365Stream):
    """Define custom stream."""

    name = "job_title"
    path = "/JobTitle"
    primary_keys = ["jobTitleId"]
    replication_key = "modifiedOn"
    schema = th.PropertiesList(
        th.Property("jobTitleId", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("jobCode", th.StringType),
        th.Property("payRate", th.NumberType),
        th.Property("posid", th.StringType),
        th.Property("glAccount_Id", th.StringType),
        th.Property("location_Id", th.StringType),
        th.Property("excludeFromSchedule", th.BooleanType),
        th.Property("excludeFromPOSImport", th.BooleanType),
        th.Property("createdBy", th.StringType),
        th.Property("createdOn", th.DateTimeType),
        th.Property("modifiedBy", th.StringType),
        th.Property("modifiedOn", th.DateTimeType),
    ).to_dict()


class LaborDetailStream(Restaurant365Stream):
    """Define custom stream."""

    name = "labor_detail"
    path = "/LaborDetail"
    primary_keys = ["laborId"]
    replication_key = "modifiedOn"
    schema = th.PropertiesList(
        th.Property("laborId", th.StringType),
        th.Property("labor", th.StringType),
        th.Property("dateWorked", th.DateTimeType),
        th.Property("dailysalessummaryid", th.StringType),
        th.Property("endTime", th.DateTimeType),
        th.Property("hours", th.NumberType),
        th.Property("payRate", th.NumberType),
        th.Property("payrollStatus", th.IntegerType),
        th.Property("startTime", th.DateTimeType),
        th.Property("total", th.NumberType),
        th.Property("employee_ID", th.StringType),
        th.Property("employeeJobTitle_ID", th.StringType),
        th.Property("jobTitle_ID", th.StringType),
        th.Property("location_ID", th.StringType),
        th.Property("cateringEvent", th.StringType),
        th.Property("employee", th.StringType),
        th.Property("payrollID", th.StringType),
        th.Property("jobTitle", th.StringType),
        th.Property("dateWorkedDateText", th.StringType),
        th.Property("location", th.StringType),
        th.Property("createdBy", th.StringType),
        th.Property("createdOn", th.DateTimeType),
        th.Property("modifiedBy", th.StringType),
        th.Property("modifiedOn", th.DateTimeType),
    ).to_dict()


class POSEmployeeStream(Restaurant365Stream):
    """Define custom stream."""

    name = "pos_employee"
    path = "/POSEmployee"
    primary_keys = ["posEmployeeId"]
    replication_key = "modifiedOn"
    schema = th.PropertiesList(
        th.Property("posEmployeeId", th.StringType),
        th.Property("fullName", th.StringType),
        th.Property("posid", th.StringType),
        th.Property("location_id", th.StringType),
        th.Property("employee_id", th.StringType),
        th.Property("createdBy", th.StringType),
        th.Property("createdOn", th.DateTimeType),
        th.Property("modifiedBy", th.StringType),
        th.Property("modifiedOn", th.DateTimeType),
    ).to_dict()


class SalesEmployeeStream(LimitedTimeframeStream):
    """Define custom stream."""

    name = "sales_employee"
    path = "/SalesEmployee"
    primary_keys = ["salesId"]
    replication_key = "modifiedOn"
    paginate = True
    schema = th.PropertiesList(
        th.Property("salesId", th.StringType),
        th.Property("receiptNumber", th.StringType),
        th.Property("checkNumber", th.StringType),
        th.Property("comment", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("dayOfWeek", th.StringType),
        th.Property("dayPart", th.StringType),
        th.Property("netSales", th.NumberType),
        th.Property("numberOfGuests", th.StringType),
        th.Property("orderHour", th.IntegerType),
        th.Property("salesAmount", th.NumberType),
        th.Property("taxAmount", th.NumberType),
        th.Property("tipAmount", th.NumberType),
        th.Property("totalAmount", th.NumberType),
        th.Property("totalPayment", th.NumberType),
        th.Property("void", th.BooleanType),
        th.Property("server", th.StringType),
        th.Property("location", th.StringType),
        th.Property("grossSales ", th.NumberType),
        th.Property("dailysalessummaryid", th.StringType),
        th.Property("createdBy", th.StringType),
        th.Property("createdOn", th.DateTimeType),
        th.Property("modifiedBy", th.StringType),
        th.Property("modifiedOn", th.DateTimeType),
        th.Property("serviceType", th.StringType),
    ).to_dict()


class SalesDetailStream(LimitedTimeframeStream):
    """Define custom stream."""

    name = "sales_detail"
    path = "/SalesDetail"
    primary_keys = ["salesdetailID"]
    replication_key = "modifiedOn"
    paginate = True
    schema = th.PropertiesList(
        th.Property("salesdetailID", th.StringType),
        th.Property("menuitem", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("customerPOSText", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("quantity", th.NumberType),
        th.Property("void", th.BooleanType),
        th.Property("company", th.StringType),
        th.Property("location", th.StringType),
        th.Property("salesID", th.StringType),
        th.Property("salesAccount", th.StringType),
        th.Property("category", th.StringType),
        th.Property("taxAmount", th.NumberType),
        th.Property("houseAccountTransaction", th.StringType),
        th.Property("dailysalessummaryid", th.StringType),
        th.Property("transactionDetailID", th.StringType),
        th.Property("createdBy", th.StringType),
        th.Property("createdOn", th.DateTimeType),
        th.Property("modifiedBy", th.StringType),
        th.Property("modifiedOn", th.DateTimeType),
    ).to_dict()


class SalesPaymentStream(LimitedTimeframeStream):
    """Define custom stream."""

    name = "sales_payment"
    path = "/SalesPayment"
    primary_keys = ["salespaymentId"]
    replication_key = "modifiedOn"
    paginate = True
    schema = th.PropertiesList(
        th.Property("salespaymentId", th.StringType),
        th.Property("name", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("comment", th.StringType),
        th.Property("customerPOSText", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("dailysalessummaryid", th.StringType),
        th.Property("isException", th.BooleanType),
        th.Property("missingreceipt", th.BooleanType),
        th.Property("company", th.StringType),
        th.Property("location", th.StringType),
        th.Property("paymenttype", th.StringType),
        th.Property("salesID", th.StringType),
        th.Property("houseAccountTransaction", th.StringType),
        th.Property("dailysalessummaryid", th.StringType),
        th.Property("transactionDetailID", th.StringType),
        th.Property("cateringEvent", th.StringType),
        th.Property("exclude", th.BooleanType),
        th.Property("createdBy", th.StringType),
        th.Property("createdOn", th.DateTimeType),
        th.Property("modifiedBy", th.StringType),
        th.Property("modifiedOn", th.DateTimeType),
    ).to_dict()


class EntityDeletedStream(Restaurant365Stream):
    """Define custom stream."""

    name = "entity_deleted"
    path = "/EntityDeleted"
    primary_keys = ["entityId"]
    replication_key = "deletedOn"
    schema = th.PropertiesList(
        th.Property("entityId", th.StringType),
        th.Property("entityName", th.StringType),
        th.Property("deletedOn", th.DateTimeType),
        th.Property("rowVersion", th.IntegerType),
    ).to_dict()


class TransactionsStream(TransactionsParentStream):
    """Define custom stream."""

    name = "transaction"
    # We get node limit exceeded for number larger than this one.
    batch_size = 10
    result_count = 0

    def get_child_context(self, record: dict, context: t.Optional[dict]) -> dict:
        return {"transaction_id": record["transactionId"]}

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: A raw `requests.Response`_ object.

        Yields:
            One item for every item found in the response.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response
        """
        data = response.json()
        if "value" in data:
            # We cant get this number later because it breaks the generator flow.
            self.result_count = len(data["value"])
        yield from extract_jsonpath(self.records_jsonpath, input=data)

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Override the get records to call child stream once batch size is reached we have processed all of the records. ."""  # noqa: E501
        batch_size = self.batch_size
        session = requests.Session()
        current_batch = []
        futures = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            for i, record in enumerate(self.request_records(context), 1):
                num_records = self.result_count
                transformed_record = self.post_process(record, context)
                if transformed_record is None:
                    # Record filtered out during post_process()
                    continue
                current_batch.append(record["transactionId"])
                if (
                    i % batch_size == 0 or i == num_records
                ):  # Check if the batch is full or it's the last record
                    futures.append(executor.submit(self._sync_children, {"transaction_ids": current_batch}))
                    current_batch = []
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error in syncing children: {e}")
            session.close()
            for record in self.request_records(context):
                yield self.post_process(record, context)

    def _process_record(
        self,
        record: dict,
        child_context: dict | None = None,
        partition_context: dict | None = None,
    ) -> None:
        """Need to override this function because we don't want to call child stream for each parent request.
        Above get_records will call child stream in batches.
        """


class TransactionDetailsStream(LimitedTimeframeStream):
    """Define custom stream."""

    name = "transaction_detail"
    path = "/TransactionDetail"
    primary_keys = ["transactionDetailId"]
    replication_key = None
    paginate = True
    parent_stream_type = TransactionsStream
    schema = th.PropertiesList(
        th.Property("transactionDetailId", th.StringType),
        th.Property("transactionId", th.StringType),
        th.Property("locationId", th.StringType),
        th.Property("glAccountId", th.StringType),
        th.Property("item", th.StringType),
        th.Property("credit", th.NumberType),
        th.Property("debit", th.NumberType),
        th.Property("amount", th.NumberType),
        th.Property("quantity", th.NumberType),
        th.Property("adjustment", th.NumberType),
        th.Property("unitOfMeasureName", th.StringType),
        th.Property("comment", th.StringType),
        th.Property("cateringEvent", th.StringType),
        th.Property("exclude", th.BooleanType),
        th.Property("createdBy", th.StringType),
        th.Property("createdOn", th.DateTimeType),
        th.Property("modifiedBy", th.StringType),
        th.Property("modifiedOn", th.DateTimeType),
    ).to_dict()

    def get_next_page_token(
        self, response: requests.Response, previous_token: t.Optional[t.Any]
    ) -> t.Optional[t.Any]:
        """Return a token for identifying next page or None if no more pages."""
        # Check if pagination is enabled
        data = response.json()
        # Check for the presence of a next page link in the response data. nextLink is only present if there are more than 5000 records in filter response.
        # This is unlikely that a single transaction will have 5k records but it is possible so leaving this code part here.
        if "@odata.nextLink" in data:
            # Increment the skip counter for pagination
            self.skip += 5000
            # Update the previous token if it exists
            if previous_token:
                previous_token = previous_token["token"]
            # Return the next page token and the updated skip value
            return {"token": previous_token, "skip": self.skip}
        self.skip = 0
        # Return the next token and the current skip value
        return None

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:

        params: dict = {}
        token_date = None
        skip = 0
        if next_page_token:
            token_date, skip = next_page_token["token"], next_page_token["skip"]
        if context.get("transaction_ids"):
            params["$filter"] = " or ".join(
                [
                    f"transactionId eq '{transaction_id}'"
                    for transaction_id in context.get("transaction_ids")
                ],
            )
            # Replace ' from the populated filter to avoid errors
            params["$filter"] = params["$filter"].replace("'", "")
        if skip > 0:
            params["$skip"] = skip
        return params

class PayrollSummaryStream(LimitedTimeframeStream):
    """Define custom stream."""

    name = "payroll_summary"
    path = "/PayrollSummary"
    primary_keys = None
    replication_key = None
    pagination_date = None
    schema = th.PropertiesList(
        th.Property("employeeID", th.StringType),
        th.Property("location", th.StringType),
        th.Property("locationNumber", th.StringType),
        th.Property("jobCode", th.StringType),
        th.Property("payRate", th.NumberType),
        th.Property("regularHours", th.NumberType),
        th.Property("overtimeHours", th.NumberType),
        th.Property("doubleOvertime", th.NumberType),
        th.Property("breakPenalty", th.NumberType),
        th.Property("grossReceipts", th.NumberType),
        th.Property("splitShiftPenalty", th.NumberType),
        th.Property("chargeTips", th.NumberType),
        th.Property("declaredTips", th.NumberType),
        th.Property("percentageOfSales", th.NumberType),
        th.Property("percent", th.IntegerType),
        th.Property("payrollStart", th.DateTimeType),
        th.Property("payrollEnd", th.DateTimeType),
    ).to_dict()
    def get_next_page_token(
        self, response: requests.Response, previous_token: t.Optional[t.Any]
    ) -> t.Optional[t.Any]:
        """
        Return a token for identifying next page or None if no more pages.

        The token is a datetime object that is used to filter the next page
        of results. The token is calculated by adding the days_delta
        parameter to the previous token. If the new token is in the future,
        the pagination is disabled.

        Args:
            response: The response object from the latest request.
            previous_token: The token from the previous page, or None if
                this is the first page.

        Returns:
            A token for the next page, or None if no more pages are available.
        """
        today = datetime.today()  # noqa: DTZ002
        if self.pagination_date:
            previous_token = self.pagination_date
        if isinstance(previous_token,str):
            previous_token = parser.parse(previous_token)
        # Add a day to previous token
        next_token = previous_token + timedelta(days=1)
        next_token = next_token.replace(tzinfo=None)

        # Disable pagination if the next token's date is in the future
        if (today - next_token).days < 0:
            return None
        # Return the next token and the current skip value
        return next_token
        
    
    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:

        params: dict = {}
        token_date = None
        if next_page_token:
            token_date = next_page_token
        start_date = token_date or self.get_starting_time(context)
        end_date = start_date + timedelta(days=self.days_delta)
        self.pagination_date = end_date
        params["$filter"] = f"payrollStart ge {start_date.strftime('%Y-%m-%dT%H:%M:%SZ')} and payrollEnd le {end_date.strftime('%Y-%m-%dT23:59:59Z')}"
        return params
