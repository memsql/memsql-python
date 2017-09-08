import pytest
import time
import threading
from memsql.common import database
from memsql.common import sql_lock
from memsql.common import exceptions
from memsql.common.test.thread_monitor import ThreadMonitor

@pytest.fixture(scope="module")
def manager_setup(request, test_db_args, test_db_database):
    with database.connect(**test_db_args) as conn:
        conn.execute('CREATE DATABASE IF NOT EXISTS %s' % test_db_database)

    test_db_args['database'] = test_db_database
    q = sql_lock.SQLLockManager('test').connect(**test_db_args).setup()

    def cleanup():
        q.destroy()
    request.addfinalizer(cleanup)

@pytest.fixture
def manager(manager_setup, test_db_args, test_db_database):
    test_db_args['database'] = test_db_database
    m = sql_lock.SQLLockManager('test').connect(**test_db_args)

    with m._db_conn() as conn:
        conn.query('DELETE FROM %s' % m.table_name)

    return m

def test_ensure_connected():
    q = sql_lock.SQLLockManager('bad_manager')

    with pytest.raises(exceptions.NotConnected):
        q.acquire('asdf')

def test_basic(manager):
    assert manager.ready()

def test_basic_usage(manager):
    l = manager.acquire('asdf', owner="test")
    assert l.valid()
    assert l.owner == 'test'

    with manager._db_conn() as conn:
        rows = conn.query('SELECT * FROM %s' % manager.table_name)

    assert len(rows) == 1
    assert rows[0].owner == 'test'
    assert rows[0].id == 'asdf'
    assert rows[0].lock_hash == l._lock_hash

def test_threading(manager):
    arr = []
    monitor = ThreadMonitor()

    def _test():
        with manager.acquire('test', block=True) as l:
            assert l.valid()
            arr.append(1)
            time.sleep(0.1)
            assert len(arr) == 1
            arr.pop()

    threads = [threading.Thread(target=monitor.wrap(_test)) for i in range(10)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    monitor.check()

def test_ping(manager):
    l = manager.acquire('test')

    with manager._db_conn() as conn:
        first = conn.get('SELECT last_contact FROM %s WHERE id=%%s' % manager.table_name, l._lock_id).last_contact

    time.sleep(1)
    l.ping()

    with manager._db_conn() as conn:
        assert first != conn.get('SELECT last_contact FROM %s WHERE id=%%s' % manager.table_name, l._lock_id).last_contact

def test_timeout(manager):
    l = manager.acquire('test', expiry=1)

    time.sleep(2)
    assert not l.valid()

    assert manager.acquire('test') is not None

def test_non_block(manager):
    manager.acquire('test')
    assert manager.acquire('test') is None

def test_with(manager):
    with manager.acquire('test') as l:
        assert l.valid()

def test_block_timeout(manager):
    acquired = []

    def _test():
        acquired.append(manager.acquire('test', block=True, timeout=0.5))

    # acquire the lock first
    manager.acquire('test')

    # this will fail to acquire
    t = threading.Thread(target=_test)

    start = time.time()
    t.start()
    t.join()
    diff = time.time() - start

    assert diff >= 0.5
    assert acquired[0] is None

def test_release(manager):
    l = manager.acquire('test')
    assert l.valid()
    l.release()
    assert not l.valid()
    assert not l.ping()
    assert not l.release()
    assert manager.acquire('test') is not None
