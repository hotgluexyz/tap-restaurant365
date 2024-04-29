"""REST client handling, including Restaurant365Stream base class."""

from __future__ import annotations

from datetime import timedelta
from http import HTTPStatus
from typing import Any, Callable

import requests
from dateutil import parser
from singer_sdk.authenticators import BasicAuthenticator
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]


class Restaurant365Stream(RESTStream):
    """Restaurant365 stream class."""

    skip = 0
    days_delta = 10

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://odata.restaurant365.net/api/v2/views"

    records_jsonpath = "$.value[*]"

    @property
    def authenticator(self) -> BasicAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BasicAuthenticator.create_for_stream(
            self,
            username=f"{self.config.get('store_name')}\{self.config.get('username')}",
            password=self.config.get("password", ""),
        )

    @property
    def http_headers(self) -> dict:

        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")  # noqa: ERA001
        return headers

    def get_new_paginator(self) -> BaseAPIPaginator:

        # if self.name in ['bills', 'journal_entries']:
        #     if parser.parse(self.started_on) >= datetime.now():
        #         self.logger.info(f"Synced all data until {self.started_on}")
        #         pass
        #     else:
        #         params

        return super().get_new_paginator()

    def get_starting_time(self, context):
        start_date = self.config.get("start_date")
        if start_date:
            start_date = parser.parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context) + timedelta(seconds=1)
        return rep_key or start_date

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """

        params: dict = {}
        if self.replication_key:
            start_date = self.get_starting_time(context).strftime("%Y-%m-%dT%H:%M:%SZ")
            params["$filter"] = f"{self.replication_key} ge {start_date}"

        return params

    def validate_response(self, response: requests.Response) -> None:
        if (
            response.status_code in self.extra_retry_statuses
            or response.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)

        if (
            HTTPStatus.BAD_REQUEST
            <= response.status_code
            < HTTPStatus.INTERNAL_SERVER_ERROR
        ):
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)
