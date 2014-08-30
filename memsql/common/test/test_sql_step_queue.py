import pytest
import time
import threading
from memsql.common import sql_step_queue, database, exceptions
from memsql.common.test.thread_monitor import ThreadMonitor
from datetime import datetime

memsql_required = pytest.mark.skipif(
    "os.environ.get('TRAVIS') == 'true'",
    reason="requires MemSQL connection"
)

@pytest.fixture(scope="module")
@memsql_required
def queue_setup(request, test_db_args, test_db_database):
    with database.connect(**test_db_args) as conn:
        conn.execute('CREATE DATABASE IF NOT EXISTS %s' % test_db_database)

    test_db_args['database'] = test_db_database
    q = sql_step_queue.SQLStepQueue('test').connect(**test_db_args).setup()

    def cleanup():
        q.destroy()
    request.addfinalizer(cleanup)

@pytest.fixture
@memsql_required
def queue(queue_setup, test_db_args, test_db_database):
    test_db_args['database'] = test_db_database
    q = sql_step_queue.SQLStepQueue('test').connect(**test_db_args)

    with q._db_conn() as conn:
        conn.query('DELETE FROM %s' % q.table_name)

    return q

@memsql_required
def test_ensure_connected():
    q = sql_step_queue.SQLStepQueue('bad_queue')

    with pytest.raises(exceptions.NotConnected):
        q.qsize()
    with pytest.raises(exceptions.NotConnected):
        q.enqueue({})
    with pytest.raises(exceptions.NotConnected):
        q.start()

@memsql_required
def test_basic(queue):
    assert queue.ready()
    assert queue.qsize() == 0

@memsql_required
def test_start_none(queue):
    assert queue.start() is None

@memsql_required
def test_start_timeout(queue):
    handler = queue.start(block=True, timeout=0.1)
    assert handler is None

@memsql_required
def test_start_block(queue):
    data = { 'hello': 'world' }

    def _test():
        handler = queue.start(block=True, timeout=5)
        assert handler is not None
        assert handler.data == data

    monitor = ThreadMonitor()
    _t = threading.Thread(target=monitor.wrap(_test))
    _t.start()

    time.sleep(0.2)
    queue.enqueue(data)

    _t.join()
    monitor.check()

@memsql_required
def test_multi_start(queue):
    queue.enqueue({})
    handlers = []
    start_evt = threading.Event()

    def _test():
        start_evt.wait(30)
        handler = queue.start()
        if handler is not None:
            handlers.append(handler)

    monitor = ThreadMonitor()

    for i in range(5):
        start_evt.clear()
        threads = [threading.Thread(target=monitor.wrap(_test)) for i in range(10)]
        [t.start() for t in threads]
        start_evt.set()
        [t.join() for t in threads]

    monitor.check()

    assert len(handlers) == 1
    assert handlers[0].valid()

@memsql_required
def test_handler_basic(queue):
    data = { 'hello': 'world' }
    queue.enqueue(data)
    assert queue.qsize() == 1
    handler = queue.start()

    with queue._db_conn() as conn:
        task_id = conn.get('SELECT id FROM %s' % queue.table_name).id

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
        task_id = conn.get('SELECT id FROM %s' % queue.table_name).id

    assert handler.data == data
    assert handler.valid()
    assert handler.task_id == task_id

    with handler.step('test'):
        step_data = handler._get_step('test')
        assert step_data['name'] == 'test'
        assert 'start' in step_data
        assert 'stop' not in step_data

    step_data = handler._get_step('test')
    assert 'stop' in step_data
    assert step_data['duration'] > 0

@memsql_required
def test_no_steal(queue):
    queue.enqueue({})
    assert queue.qsize() == 1
    queue.start()
    assert queue.start() is None

@memsql_required
def test_enqueue_returns_id(queue):
    task_id = queue.enqueue({})
    task = queue.start()

    assert task.task_id == task_id

@memsql_required
def test_steps_survive_refresh(queue):
    queue.enqueue({})
    task = queue.start()

    task.start_step('test')
    assert isinstance(task.steps[0]['start'], datetime)
    task.refresh()
    assert isinstance(task.steps[0]['start'], datetime)
    task.stop_step('test')
    assert isinstance(task.steps[0]['stop'], datetime)
    task.refresh()
    assert isinstance(task.steps[0]['stop'], datetime)

@memsql_required
def test_auto_requeue(queue):
    queue.execution_ttl = 1

    queue.enqueue({})
    handler = queue.start()

    time.sleep(2)
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

    step_data = handler._get_step('test')
    assert 'stop' in step_data

    with pytest.raises(sql_step_queue.StepAlreadyFinished):
        handler.start_step('test')

@memsql_required
def test_finished_basic(queue):
    queue.enqueue({})
    handler = queue.start()

    with handler.step('test'):
        with pytest.raises(sql_step_queue.StepRunning):
            handler.finish()

    assert handler.finished is None

    handler.finish()

    assert isinstance(handler.finished, datetime)
    assert handler.data['result'] == 'success'
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
def test_timeout_fails(queue):
    queue.enqueue({})
    handler = queue.start()

    queue.execution_ttl = 1
    time.sleep(1.5)

    assert not handler.valid()

    with pytest.raises(sql_step_queue.TaskDoesNotExist):
        handler.finish()
    with pytest.raises(sql_step_queue.TaskDoesNotExist):
        handler.start_step('asdf')
    with pytest.raises(sql_step_queue.StepNotStarted):
        handler.stop_step('asdf')
    with pytest.raises(sql_step_queue.TaskDoesNotExist):
        with handler.step('asdf'):
            pass

@memsql_required
def test_requeue_fails(queue):
    queue.enqueue({})
    handler = queue.start()

    handler.requeue()
    assert not handler.valid()

    with pytest.raises(sql_step_queue.TaskDoesNotExist):
        handler.finish()
    with pytest.raises(sql_step_queue.TaskDoesNotExist):
        handler.start_step('asdf')
    with pytest.raises(sql_step_queue.StepNotStarted):
        handler.stop_step('asdf')
    with pytest.raises(sql_step_queue.TaskDoesNotExist):
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

    assert queue.start() is None

    handler2.finish()
    assert handler.valid()
    assert not handler2.valid()

    queue.execution_ttl = 1
    time.sleep(1.2)
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

@memsql_required
def test_extra_predicate_basic(queue):
    queue.enqueue({ 'test': 1 })
    queue.enqueue({ 'test': 5 })

    handler = queue.start(extra_predicate=('data::$test = %s', (5, )))
    assert handler is not None
    assert handler.data == { 'test': 5 }

    handler.requeue()

    handler = queue.start(extra_predicate=('data::$test = %(v)s', { 'v': 5 }))
    assert handler is not None
    assert handler.data == { 'test': 5 }

    handler.requeue()

    handler = queue.start(extra_predicate=('data::$test = %s', 5))
    assert handler is not None
    assert handler.data == { 'test': 5 }

@memsql_required
def test_bulk_finish_basic(queue):
    queue.enqueue({ })
    queue.enqueue({ })
    queue.enqueue({ })
    queue.enqueue({ })

    handler = queue.start()
    assert handler is not None
    assert handler.valid()

    assert queue.qsize() == 3

    queue.bulk_finish()

    assert queue.qsize() == 0

    assert handler is not None
    assert handler.valid()

@memsql_required
def test_bulk_finish_more(queue):
    queue.enqueue({ })
    queue.enqueue({ 'test': 'foo' })

    assert queue.qsize() == 2

    queue.bulk_finish(extra_predicate=('data::$test = %s', 'foo'))

    assert queue.qsize() == 1
    assert queue.start().data == { }

@memsql_required
def test_data_manip(queue):
    queue.enqueue({ 'a': 1 })
    handler = queue.start()

    assert handler.data['a'] == 1

    handler.data['a'] = 2
    handler.save()
    handler.refresh()

    assert handler.data['a'] == 2

    handler._save(data={ 'b': 2 })
    assert handler.data['b'] == 2

    handler.refresh()
    assert handler.data['b'] == 2
