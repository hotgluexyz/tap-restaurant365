"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import pytest

from hotglue_singer_sdk.testing import get_standard_tap_tests
from tap_restaurant365.tap import TapRestaurant365

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "username": "test",
    "password": "test",
    "store_name": "test",
    # TODO: Initialize minimal tap config
}


# Run standard built-in tap tests from the SDK:
@pytest.mark.parametrize("test_fn", get_standard_tap_tests(TapRestaurant365, SAMPLE_CONFIG))
def test_tap(test_fn):
    test_fn()

# TODO: Create additional tests as appropriate for your tap.
