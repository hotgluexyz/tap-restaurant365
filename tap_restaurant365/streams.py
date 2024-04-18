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


class LimitedTimeframeStream(Restaurant365Stream):
    """parent class stream for override/pagination"""

    name = "vendors"
    path = "/Company"

    def get_next_page_token(
        self, response: requests.Response, previous_token: t.Optional[t.Any]
    ) -> t.Optional[t.Any]:
        """Return a token for identifying next page or None if no more pages."""
        if self.paginate == True:
            data = response.json()
            if "@odata.nextLink" in data:
                self.skip += 5000
                if previous_token:
                    previous_token = previous_token['token']
                return {"token":previous_token,"skip":self.skip}  
            else:
                self.skip = 0
                #Pick starting value just incase progress marker is not present.
                replication_key_value = self.tap_state["bookmarks"][self.name]['starting_replication_value']
                if "progress_markers" in self.tap_state["bookmarks"][self.name]:
                    replication_key_value = self.tap_state["bookmarks"][self.name]['progress_markers']["replication_key_value"]

                start_date = (parser.parse(replication_key_value) + timedelta(seconds=1)) or parser.parse(self.config.get("start_date"))
                today = datetime.today()
                if previous_token and "token" in previous_token and previous_token['token'] is None:
                    some = ""
                if (
                    previous_token
                    and "token" in previous_token
                    and previous_token['token']
                    and start_date.replace(tzinfo=None)
                    <= previous_token["token"].replace(tzinfo=None)
                ):
                    start_date = previous_token["token"] + timedelta(days=self.days_delta)
                next_token = start_date.replace(tzinfo=None)

                if (today - next_token).days < self.days_delta:
                    self.paginate = False
                return {"token":next_token,'skip':self.skip}
        else:
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
        end_date = start_date + timedelta(days = self.days_delta)
        if self.replication_key:
            params["$filter"] = (
                f"{self.replication_key} ge {start_date.strftime('%Y-%m-%dT%H:%M:%SZ')} and {self.replication_key} lt {end_date.strftime('%Y-%m-%dT23:59:59Z')}"
            )
            params['$orderby'] = f"{self.replication_key}"
        if self.name == "journal_entries":
            #set a date in the stream to check later to see if we need to keep calling to the stream
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
        if skip>0:
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
        th.Property("modifiedOn", th.DateTimeType)
    ).to_dict()


class TransactionsStream(LimitedTimeframeStream):
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

class CreditMemosStream(TransactionsStream):
    """Define custom stream."""

    name = "credit_memos"
    path = "/Transaction" #?$filter=type eq 'AP Credit memo and modifiedOn ge '
    primary_keys = ["transactionId"]
    replication_key = "modifiedOn"
    paginate = True

class StockCountStream(TransactionsStream):
    """Define custom stream."""

    name = "stock_count"
    path = "/Transaction" #?$filter=type eq 'Stock Count and modifiedOn ge '
    primary_keys = ["transactionId"]
    replication_key = "modifiedOn"
    paginate = True

class BankExpensesStream(TransactionsStream):
    """Define custom stream."""

    name = "bank_expenses"
    path = "/Transaction" #?$filter=type eq 'Bank Expense and modifiedOn ge '
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

class TransactionDetailsStream(LimitedTimeframeStream):
    """Define custom stream."""

    name = "transaction_detail"
    path = "/TransactionDetail"
    primary_keys = ["transactionDetailId"]
    replication_key = "modifiedOn"
    paginate = True
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
