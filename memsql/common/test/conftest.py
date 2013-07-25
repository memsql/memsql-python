import pytest
from memsql.common import database_mysqldb
import os

@pytest.fixture(scope="module")
def test_db_args():
    user = 'travis' if os.environ.get('TRAVIS') == 'true' else 'root'
    return {
        "host": '127.0.0.1',
        "port": 3306,
        "user": user,
        "password": '',
    }

@pytest.fixture
def test_db_conn(test_db_args):
    return database_mysqldb.connect(**test_db_args)
