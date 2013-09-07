import pytest
from memsql.common import database

def test_connection_open(test_db_conn):
    assert test_db_conn.connected()

def test_connection_close(test_db_conn):
    assert test_db_conn.connected()
    test_db_conn.close()
    assert not test_db_conn.connected()

def test_reconnecct(test_db_conn):
    assert test_db_conn.connected()
    db_instance = test_db_conn._db
    test_db_conn.reconnect()
    assert test_db_conn.connected()
    assert db_instance != test_db_conn._db

def test_query(test_db_conn):
    # select result
    res = test_db_conn.query('select 1')
    assert len(res) == 1
    assert res[0]['1'] == 1

class TestQueries(object):
    @pytest.fixture(scope="class")
    def x_conn(self, request, test_db_args):
        conn = database.connect(**test_db_args)
        conn.execute('CREATE DATABASE IF NOT EXISTS memsql_python_test')
        conn.execute('USE memsql_python_test')

        def cleanup():
            conn.execute('DROP DATABASE memsql_python_test')
        request.addfinalizer(cleanup)

        return conn

    @pytest.fixture(scope="class", autouse=True)
    def ensure_schema(self, x_conn, request):
        x_conn.execute('DROP TABLE IF EXISTS x')
        x_conn.execute('CREATE TABLE x (id BIGINT AUTO_INCREMENT PRIMARY KEY, value INT)')

    @pytest.fixture(autouse=True)
    def ensure_empty(self, x_conn, request):
        cleanup = lambda: x_conn.execute('DELETE FROM x')
        cleanup()
        request.addfinalizer(cleanup)

    def test_insert(self, x_conn):
        res = x_conn.query('INSERT INTO x (value) VALUES(1)')
        assert isinstance(res, long)
        assert res == 1  # 1 affected row

        res = x_conn.execute('INSERT INTO x (value) VALUES(1)')
        assert isinstance(res, long)

        res = x_conn.execute_lastrowid('INSERT INTO x (value) VALUES(1)')
        assert isinstance(res, long)
        last_row = x_conn.get('SELECT * FROM x ORDER BY id DESC LIMIT 1')
        assert res == last_row.id

    def test_select(self, x_conn):
        x_conn.execute('INSERT INTO x (value) VALUES (1), (2), (3)')

        all_rows = x_conn.query('SELECT * FROM x')
        assert len(all_rows) == 3
        assert all_rows[1].value == 2

        first_row = x_conn.get('SELECT * FROM x ORDER BY id LIMIT 1')
        assert first_row.value == 1

    def test_queryparams(self, x_conn):
        x_conn.execute('INSERT INTO x (value) VALUES (1), (2), (3)')

        rows = x_conn.query('SELECT * FROM x WHERE value > %s AND value < %s', 1, 3)

        assert len(rows) == 1
        assert rows[0].value == 2
