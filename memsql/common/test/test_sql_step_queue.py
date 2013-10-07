import pytest
import os
import time
import threading
from memsql.common import sql_step_queue
from memsql.common import database

memsql_required = pytest.mark.skipif(
    os.environ.get('TRAVIS') == 'true',
    reason="requires MemSQL connection"
)

@pytest.fixture(scope="module")
def queue_setup(request, test_db_args, test_db_database):
    with database.connect(**test_db_args) as conn:
        conn.execute('CREATE DATABASE IF NOT EXISTS %s' % test_db_database)

    test_db_args['database'] = test_db_database
    q = sql_step_queue.SQLStepQueue('test').connect(**test_db_args).setup()

    def cleanup():
        q.destroy()
    request.addfinalizer(cleanup)

@pytest.fixture
def queue(queue_setup, test_db_args, test_db_database):
    test_db_args['database'] = test_db_database
    q = sql_step_queue.SQLStepQueue('test').connect(**test_db_args)

    with q._db_conn() as conn:
        conn.query('DELETE FROM %s' % q.tasks_table)
        conn.query('DELETE FROM %s' % q.executions_table)

    return q

@memsql_required
def test_basic(queue):
    assert queue.ready()
    assert queue.qsize() == 0

@memsql_required
def test_start_none(queue):
    assert queue.start() == None

@memsql_required
def test_start_timeout(queue):
    handler = queue.start(block=True, timeout=0.1)
    assert handler == None

@memsql_required
def test_start_block(queue):
    data = { 'hello': 'world' }

    def _test():
        handler = queue.start(block=True, block_timeout=0.01)
        assert handler.data == data

    _t = threading.Thread(target=_test)
    _t.start()

    time.sleep(0.2)
    queue.enqueue(data)

    _t.join()

@memsql_required
def test_multi_start(queue):
    queue.enqueue({})
    handlers = []

    def _test():
        handler = queue.start()
        if handler is not None:
            handlers.append(handler)

    threads = [threading.Thread(target=_test) for i in range(10)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    assert len(handlers) == 1
    assert handlers[0].valid()

@memsql_required
def test_handler_basic(queue):
    data = { 'hello': 'world' }
    queue.enqueue(data)
    assert queue.qsize() == 1
    handler = queue.start()

    with queue._db_conn() as conn:
        task_id = conn.get('SELECT id FROM %s' % queue.tasks_table).id

    assert handler.data == data
    assert handler.valid()
    assert handler.task_id == task_id

@memsql_required
def test_handler_basic_2(queue):
    data = { 'hello': 'world' }
    queue.enqueue(data)
    assert queue.qsize() == 1
    handler = queue.start()

    with queue._db_conn() as conn:
        task_id = conn.get('SELECT id FROM %s' % queue.tasks_table).id

    assert handler.data == data
    assert handler.valid()
    assert handler.task_id == task_id

    with handler.step('test'):
        step_data = handler._get_step('test')
        assert step_data['name'] == 'test'
        assert 'start' in step_data
        assert 'stop' not in step_data

    assert 'stop' in step_data
    assert step_data['duration'] > 0

@memsql_required
def test_no_steal(queue):
    queue.enqueue({})
    assert queue.qsize() == 1
    handler = queue.start()
    assert queue.start() == None

@memsql_required
def test_auto_requeue(queue):
    queue.execution_ttl = 1

    queue.enqueue({})
    handler = queue.start()

    time.sleep(1.2)
    h2 = queue.start()

    assert h2 is not None
    assert h2.valid()

    assert not handler.valid()
    assert handler.task_id == h2.task_id

@memsql_required
def test_start_stop_step(queue):
    data = { 'hello': 'world' }
    queue.enqueue(data)
    handler = queue.start()

    handler.start_step('test')

    step_data = handler._get_step('test')
    assert step_data['name'] == 'test'
    assert 'start' in step_data
    assert 'stop' not in step_data

    with pytest.raises(sql_step_queue.StepAlreadyStarted):
        handler.start_step('test')

    handler.stop_step('test')

    assert 'stop' in step_data

    with pytest.raises(sql_step_queue.StepAlreadyFinished):
        handler.start_step('test')

@memsql_required
def test_finished_basic(queue):
    queue.execution_ttl = 1

    queue.enqueue({})
    handler = queue.start()

    with handler.step('test'):
        with pytest.raises(sql_step_queue.StepRunning):
            handler.finish()

    handler.finish()

    assert not handler.valid()

    with pytest.raises(sql_step_queue.AlreadyFinished):
        handler.ping()
    with pytest.raises(sql_step_queue.AlreadyFinished):
        handler.finish()
    with pytest.raises(sql_step_queue.AlreadyFinished):
        handler.start_step('asdf')
    with pytest.raises(sql_step_queue.AlreadyFinished):
        handler.stop_step('asdf')
    with pytest.raises(sql_step_queue.AlreadyFinished):
        with handler.step('asdf'):
            pass

@memsql_required
def test_multi_queue(queue):
    queue.enqueue({})
    queue.enqueue({})

    handler = queue.start()
    handler2 = queue.start()
    assert handler.valid()
    assert handler2.valid()

    assert handler.task_id != handler2.task_id

    handler2.finish()
    assert handler.valid()
    assert not handler2.valid()

    queue.execution_ttl = 1

    time.sleep(1.2)
    handler.finish()
    assert not handler.valid()

@memsql_required
def test_complex_multi_queue(queue):
    queue.enqueue({})
    queue.enqueue({})
    assert queue.qsize() == 2

    handler = queue.start()
    handler2 = queue.start()

    assert handler.valid()
    assert handler2.valid()
    assert queue.qsize() == 0

    assert handler.task_id != handler2.task_id

    handler2.finish()
    assert queue.qsize() == 0

    assert handler.valid()
    assert not handler2.valid()

    queue.execution_ttl = 1
    time.sleep(1.2)
    # handler didn't finish in time
    assert queue.qsize() == 1
    assert not handler.valid()

    handler2 = queue.start()
    assert handler2.task_id == handler.task_id
    handler2.finish()

    assert queue.qsize() == 0
