import base64
import json

import pytest


@pytest.fixture(scope="session")
def api_token():
    return base64.b64encode(json.dumps({"api_address": "aa", "api_url": "bb"}).encode("utf-8")).decode("utf-8")
