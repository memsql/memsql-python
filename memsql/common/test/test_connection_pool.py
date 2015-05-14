import pytest
import mock
import multiprocessing
import six

from memsql.common.test.test_database_adapters import TestQueries

@pytest.fixture
def test_key(test_db_args):
    return (test_db_args['host'],
           test_db_args['port'],
           test_db_args['user'],
           test_db_args['password'],
           "information_schema",
           None,
           multiprocessing.current_process().pid)

@pytest.fixture
def pool():
    from memsql.common.connection_pool import ConnectionPool
    return ConnectionPool()

@pytest.fixture
def db_args(test_key):
    return test_key[:-2]

@pytest.fixture
def fairy(pool, db_args):
    return pool.connect(*db_args)

def test_checkout(pool, test_key, db_args):
    fairy = pool.connect(*db_args)

    assert fairy._key == test_key

    assert len(pool._connections) == 1
    assert list(pool._connections.values())[0].qsize() == 0
    assert len(pool._fairies) == 1

def test_checkout_options(pool, db_args):
    from memsql.common.connection_pool import PoolConnectionException
    args = ("memsql.com",) + db_args[1:5] + ({ "connect_timeout": 1 },)
    with pytest.raises(PoolConnectionException):
        pool.connect(*args)

def test_checkin(pool, fairy):
    pool.checkin(fairy, fairy._key, fairy._conn)

    assert len(pool._fairies) == 0
    assert len(pool._connections) == 1
    assert list(pool._connections.values())[0].qsize() == 1

def test_connection_reuse(pool, test_key, db_args):
    fairy = pool.connect(*db_args)
    db_conn = fairy._conn
    fairy.close()
    fairy = pool.connect(*db_args)
    assert fairy._conn == db_conn
    fairy.close()

def test_connection_invalidation(pool, test_key, db_args):
    fairy = pool.connect(*db_args)
    db_conn = fairy._conn
    r = fairy.query('SELECT 1')
    assert r[0]['1'] == 1
    fairy.close()
    db_conn.close()
    fairy = pool.connect(*db_args)
    # We should not have used the same db connection because we should have
    # detected that it was closed.
    assert fairy._conn != db_conn
    r = fairy.query('SELECT 1')
    assert r[0]['1'] == 1
    fairy.close()

def test_connection_close(pool, db_args):
    fairy = pool.connect(*db_args)
    fairy2 = pool.connect(*db_args)
    assert fairy
    assert fairy2
    pool.close()

    assert len(pool._fairies) == 0
    for queue in pool._connections.values():
        assert queue.qsize() == 0

def test_connection_info(fairy, db_args):
    conn_info = fairy.connection_info()
    assert len(conn_info) == 2
    assert conn_info[0] == db_args[0]
    assert conn_info[1] == db_args[1]

def test_fairy_expire(pool, test_key, db_args):
    fairy = pool.connect(*db_args)

    fairy.expire()
    fairy.close()
    assert len(pool._fairies) == 0
    assert len(pool._connections) == 1
    assert list(pool._connections.values())[0].qsize() == 0

def test_fairy_reconnect(fairy):
    assert fairy.connected()
    fairy.reconnect()
    assert fairy.connected()

@pytest.fixture()
def _fairy_queries_fixture(request, fairy, test_db_args, test_db_database):
    test_queries = TestQueries()
    conn = test_queries.x_conn(request, test_db_args, test_db_database)
    test_queries.ensure_schema(conn, request)

def test_fairy_queries(fairy, _fairy_queries_fixture, test_db_database):
    test_queries = TestQueries()
    fairy.select_db(test_db_database)

    for attr in dir(test_queries):
        if attr.startswith('test_') and "bytes" not in attr:
            getattr(test_queries, attr)(fairy)
        fairy.execute('DELETE FROM x')

def test_fairy_query(fairy):
    r = fairy.query('SELECT 1')
    assert r[0]['1'] == 1

def test_fairy_get(fairy):
    r = fairy.get('SELECT 1')
    assert r['1'] == 1

def test_fairy_execute(fairy):
    fairy.execute('SELECT 1')

def test_fairy_execute_lastrowid(fairy):
    row_id = fairy.execute_lastrowid('SELECT 1')
    assert isinstance(row_id, six.integer_types)

@mock.patch('memsql.common.database.Connection')
def test_socket_issues(mock_class, pool, db_args, test_key):
    from memsql.common.connection_pool import PoolConnectionException
    import errno, socket
    instance = mock_class.return_value

    def raise_ioerror(*args, **kwargs):
        raise socket.error(errno.ECONNRESET, "connection reset")
    instance.query.side_effect = raise_ioerror

    fairy = pool.connect(*db_args)

    with pytest.raises(PoolConnectionException) as exc:
        fairy.query('SELECT 1')

    assert fairy._expired

    e = exc.value
    assert e.message == 'connection reset'
    assert (e.host, e.port, e.user, e.password, e.db_name, e.options, e.pid) == test_key

def test_sql_errors(fairy):
    from _mysql import ProgrammingError
    with pytest.raises(ProgrammingError):
        fairy.query('asdf bad query!!')

def test_exception_remapping(pool, db_args, test_db_database):
    from memsql.common.connection_pool import PoolConnectionException
    from memsql.common import errorcodes
    import _mysql

    # check that some operationalerrors get mapped to PoolConnectionException
    bad_db_args = db_args[:-1] + ("aasjdkfjdoes_not_exist",)
    fairy = None
    with pytest.raises(PoolConnectionException):
        fairy = pool.connect(*bad_db_args)
    assert fairy is None

    # other programmer errors should not be mapped
    fairy = pool.connect(*db_args)
    fairy.query('CREATE DATABASE IF NOT EXISTS %s' % test_db_database)
    fairy.select_db(test_db_database)
    fairy.query('CREATE TABLE IF NOT EXISTS x (id BIGINT PRIMARY KEY)')

    with pytest.raises(_mysql.DatabaseError) as exc:
        fairy.query('SELECT bad_key FROM x')

    assert not fairy._expired
    e = exc.value
    assert e.args == (errorcodes.ER_BAD_FIELD_ERROR, "Unknown column 'bad_key' in 'field list'")

def test_size(pool, test_key, db_args):
    assert pool.size() == 0

    fairy = pool.connect(*db_args)
    fairy.close()

    assert pool.size() == 1

    fairy = pool.connect(*db_args)
    fairy2 = pool.connect(*db_args)  # noqa

    assert pool.size() == 2
