"""REST client handling, including Restaurant365Stream base class."""

from __future__ import annotations

from datetime import timedelta
from http import HTTPStatus
from typing import Any, Callable, Generator
from urllib.parse import parse_qs, urlparse

import backoff
import requests
from dateutil import parser
from singer_sdk.authenticators import BasicAuthenticator
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream

import singer_sdk._singerlib as singer

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

    def get_next_page_token(
        self, response: requests.Response, previous_token: Any | None
    ) -> Any | None:
        data = response.json()
        next_page_token = None
        if "@odata.nextLink" in data:
            url = data["@odata.nextLink"]
            parsed_url = urlparse(url)
            # Extract the query parameters
            params = parse_qs(parsed_url.query)
            if "$skip" in params and len(params["$skip"]) > 0 :
                return int(params["$skip"][0])
            
        return next_page_token

    def get_starting_time(self, context):
        start_date = self.config.get("start_date")
        rep_key = None
        if start_date:
            start_date = parser.parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context)
        if rep_key:
            rep_key = rep_key + timedelta(seconds=1)
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
        if next_page_token:
            params["$skip"] = next_page_token
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

    def backoff_wait_generator(self) -> Generator[float, None, None]:
        """The wait generator used by the backoff decorator on request failure.

        See for options:
        https://github.com/litl/backoff/blob/master/backoff/_wait_gen.py

        And see for examples: `Code Samples <../code_samples.html#custom-backoff>`_

        Returns:
            The wait generator
        """
        return backoff.expo(factor=5)

    def backoff_max_tries(self) -> int:
        """The number of attempts before giving up when retrying requests.

        Returns:
            Number of max retries.
        """
        return 8

    def _write_state_message(self) -> None:
        """Write out a STATE message with the latest state."""
        tap_state = self.tap_state

        if tap_state and tap_state.get("bookmarks"):
            for stream_name in tap_state.get("bookmarks").keys():
                if tap_state["bookmarks"][stream_name].get("partitions") and stream_name in ["transaction_detail"]:
                    tap_state["bookmarks"][stream_name] = {"partitions": []}

        singer.write_message(singer.StateMessage(value=tap_state))