# 'conftest.py' will share the fixtures across all files
from .__init__ import engine
import pytest

@pytest.fixture
def my_engine():
    return engine