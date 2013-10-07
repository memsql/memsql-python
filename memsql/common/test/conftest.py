import pytest
from memsql.common import database
import os

@pytest.fixture(scope="module")
def test_db_database():
    return "memsql_python_tests"

@pytest.fixture(scope="module")
def test_db_args():
    if os.environ.get('TRAVIS') == 'true':
        return {
            "host": '127.0.0.1',
            "port": 3306,
            "user": 'travis',
            "password": ''
        }
    else:
        return {
            "host": 'leaf-1.cs.memcompute.com',
            "port": 3306,
            "user": 'root',
            "password": ''
        }

@pytest.fixture
def test_db_conn(test_db_args):
    return database.connect(**test_db_args)
