# -*- coding: utf-8 -*-

import pytest
import time
from memsql.common import database
import uuid
import copy

def test_connection_open(test_db_conn):
    assert test_db_conn.connected()

def test_connection_close(test_db_conn):
    assert test_db_conn.connected()
    test_db_conn.close()
    assert not test_db_conn.connected()

def test_reconnect(test_db_conn):
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

def test_ping(test_db_conn):
    test_db_conn.ping()

def test_thread_id(test_db_conn):
    assert isinstance(test_db_conn.thread_id(), int)

def test_connection_options(test_db_args):
    args = copy.deepcopy(test_db_args)
    args["host"] = "example.com"
    args["options"] = { "connect_timeout": 1 }
    with pytest.raises(database.OperationalError):
        conn = database.connect(**args)
        conn.query("SHOW TABLES")

class TestQueries(object):
    @pytest.fixture(scope="class")
    def x_conn(self, request, test_db_args, test_db_database):
        return self._x_conn(request, test_db_args, test_db_database)

    def _x_conn(self, request, test_db_args, test_db_database):
        conn = database.connect(**test_db_args)
        conn.execute('CREATE DATABASE IF NOT EXISTS %s CHARACTER SET utf8 COLLATE utf8_general_ci' % test_db_database)
        conn.select_db(test_db_database)

        def cleanup():
            conn.execute('DROP DATABASE %s' % test_db_database)
        request.addfinalizer(cleanup)

        return conn

    @pytest.fixture(scope="class", autouse=True)
    def ensure_schema(self, x_conn, request):
        return self._ensure_schema(x_conn, request)

    def _ensure_schema(self, x_conn, request):
        x_conn.execute('DROP TABLE IF EXISTS x')
        x_conn.execute("""
            CREATE TABLE x (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                value INT,
                col1 VARCHAR(255),
                col2 VARCHAR(255),
                colb VARBINARY(32)
            ) DEFAULT CHARSET=utf8
        """)

    @pytest.fixture(autouse=True)
    def ensure_empty(self, x_conn, request):
        return self._ensure_empty(x_conn, request)

    def _ensure_empty(self, x_conn, request):
        cleanup = lambda: x_conn.execute('DELETE FROM x')
        cleanup()
        request.addfinalizer(cleanup)

    def test_insert(self, x_conn):
        res = x_conn.query('INSERT INTO x (value) VALUES(1)')
        assert isinstance(res, int)
        assert res == 1  # 1 affected row

        res = x_conn.execute('INSERT INTO x (value) VALUES(1)')
        assert isinstance(res, int)

        res = x_conn.execute_lastrowid('INSERT INTO x (value) VALUES(1)')
        assert isinstance(res, int)
        last_row = x_conn.get('SELECT * FROM x ORDER BY id DESC LIMIT 1')
        assert res == last_row.id

    def test_select(self, x_conn):
        x_conn.execute('INSERT INTO x (value) VALUES (1), (2), (3)')

        all_rows = x_conn.query('SELECT * FROM x ORDER BY value ASC')
        assert len(all_rows) == 3
        assert all_rows[1].value == 2

        first_row = x_conn.get('SELECT * FROM x ORDER BY id LIMIT 1')
        assert first_row.value == 1

    def test_unicode(self, x_conn):
        x_conn.execute("SET NAMES utf8")

        x_conn.execute('INSERT INTO x (col1) VALUES (%s)', '⚑☃❄')
        rows = x_conn.query('SELECT * FROM x WHERE col1=%s', '⚑☃❄')
        assert len(rows) == 1
        assert rows[0].col1 == '⚑☃❄'

        rows = x_conn.query('SELECT * FROM x WHERE col1 in (%s)', ['⚑☃❄', 'jones'])
        assert len(rows) == 1
        assert rows[0].col1 == '⚑☃❄'

        rows = x_conn.query('SELECT * FROM x WHERE col1=%(col1)s', col1='⚑☃❄')
        assert len(rows) == 1
        assert rows[0].col1 == '⚑☃❄'

    def test_queryparams(self, x_conn):
        x_conn.execute('INSERT INTO x (value) VALUES (1), (2), (3)')

        rows = x_conn.query('SELECT * FROM x WHERE value > %s AND value < %s', 1, 3)

        assert len(rows) == 1
        assert rows[0].value == 2

    def test_advanced_params(self, x_conn):
        # multi-column insert with array
        x_conn.debug_query('''
            INSERT INTO x (value, col1, col2) VALUES (%s, %s)
        ''', 1, ['bob', 'jones'])

        x_conn.debug_query('''
            INSERT INTO x (value, col1, col2) VALUES (%(value)s, %(other)s)
        ''', value=1, other=['bob', 'jones'])

        rows = x_conn.query('SELECT * FROM x WHERE value = %s and col1 = %s', 1, 'bob')

        assert len(rows) == 2
        for i in range(2):
            assert rows[i].value == 1
            assert rows[i].col1 == 'bob'
            assert rows[i].col2 == 'jones'

    def test_kwargs(self, x_conn):
        # multi-column insert with kwargs
        x_conn.execute('''
            INSERT INTO x (value, col1, col2) VALUES (%(value)s, %(col1)s, %(col2)s)
        ''', value=1, col1='bilbo', col2='jones')

        rows = x_conn.query('SELECT * FROM x WHERE value = %s and col1 = %s', 1, 'bilbo')

        assert len(rows) == 1
        assert rows[0].value == 1
        assert rows[0].col1 == 'bilbo'
        assert rows[0].col2 == 'jones'

    def test_kwargs_all(self, x_conn):
        # ensure they all support kwargs
        for method in ['debug_query', 'query', 'get', 'execute', 'execute_lastrowid']:
            getattr(x_conn, method)('''select * from x where col1=%(col1)s''', col1='bilbo')

    def test_kwparams_exclusive(self, x_conn):
        # ensure they are all exclusive
        for method in ['debug_query', 'query', 'get', 'execute', 'execute_lastrowid']:
            with pytest.raises(ValueError):
                getattr(x_conn, method)('''select * from x where col1=%(col1)s''', 1, col1='bilbo')

    def test_single_format(self, x_conn):
        x_conn.query("select * from x where col1 LIKE '%'")

    def test_ensure_connected(self, x_conn):
        old = x_conn.max_idle_time
        before_db = x_conn._db

        try:
            x_conn.max_idle_time = 0
            time.sleep(1)
            x_conn.query("select * from x")

            assert x_conn._db != before_db
        finally:
            x_conn.max_idle_time = old
