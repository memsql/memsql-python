import pytest
import mock
import multiprocessing

@pytest.fixture
def test_key(test_db_args):
    return (test_db_args['host'],
           test_db_args['port'],
           test_db_args['user'],
           test_db_args['password'],
           "information_schema",
           multiprocessing.current_process().pid)

@pytest.fixture
def pool():
    from memsql.common.connection_pool import ConnectionPool
    return ConnectionPool()

@pytest.fixture
def db_args(test_key):
    return test_key[:-1]

@pytest.fixture
def fairy(pool, db_args):
    return pool.connect(*db_args)

def test_checkout(pool, test_key, db_args):
    fairy = pool.connect(*db_args)

    assert fairy._key == test_key

    assert len(pool._connections) == 1
    assert pool._connections.values()[0].qsize() == 0
    assert len(pool._fairies) == 1

def test_checkin(pool, fairy):
    pool.checkin(fairy, fairy._key, fairy._conn)

    assert len(pool._fairies) == 0
    assert len(pool._connections) == 1
    assert pool._connections.values()[0].qsize() == 1

def test_connection_reuse(pool, test_key, db_args):
    fairy = pool.connect(*db_args)
    db_conn = fairy._conn
    fairy.close()
    fairy = pool.connect(*db_args)
    assert fairy._conn == db_conn
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
    assert pool._connections.values()[0].qsize() == 0

def test_fairy_reconnect(fairy):
    assert fairy.connected()
    fairy.reconnect()
    assert fairy.connected()

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
    assert isinstance(row_id, long)

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
    assert (e.host, e.port, e.user, e.password, e.db_name, e.pid) == test_key

def test_sql_errors(fairy):
    from _mysql import ProgrammingError
    with pytest.raises(ProgrammingError):
        fairy.query('asdf bad query!!')
