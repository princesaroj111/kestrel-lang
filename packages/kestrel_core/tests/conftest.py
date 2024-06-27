from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
    # Setup: remove any old DB
    Path("cache.db").unlink(missing_ok=True)
    yield # this is where the testing happens
