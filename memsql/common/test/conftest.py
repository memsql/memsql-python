import os
import pytest
from memsql.common import database

host = os.environ.get('MEMSQL_PYTHON_TEST_HOST', '127.0.0.1')

@pytest.fixture(scope="module")
def test_db_database():
    return "memsql_python_tests"

@pytest.fixture(scope="module")
def test_db_args():
    return {
        "host": host,
        "port": 3306,
        "user": 'root',
        "password": 'mysql'
    }

@pytest.fixture
def test_db_conn(test_db_args):
    return database.connect(**test_db_args)
