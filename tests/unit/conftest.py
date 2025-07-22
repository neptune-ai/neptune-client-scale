import base64
import json
import os
import tempfile
from pathlib import Path

import pytest

from neptune_scale.sync.operations_repository import OperationsRepository


@pytest.fixture(scope="session")
def api_token():
    return base64.b64encode(
        json.dumps({"api_address": "http://api-address", "api_url": "http://api-url"}).encode("utf-8")
    ).decode("utf-8")


@pytest.fixture
def temp_db_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "test_operations.db")
        yield db_path


@pytest.fixture
def operations_repo(temp_db_path):
    repo = OperationsRepository(db_path=Path(temp_db_path))
    repo.init_db()
    yield repo
    repo.close(cleanup_files=True)
