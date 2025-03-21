import base64
import json

import pytest


@pytest.fixture(scope="session")
def api_token():
    return base64.b64encode(
        json.dumps({"api_address": "http://api-address", "api_url": "http://api-url"}).encode("utf-8")
    ).decode("utf-8")
