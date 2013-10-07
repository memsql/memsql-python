from memsql.common.connection_pool import ConnectionPool, MySQLError, PoolConnectionException
from memsql.common import json, errorcodes
import time
from datetime import datetime
from contextlib import contextmanager

PRIMARY_TABLE = """\
CREATE TABLE IF NOT EXISTS %(prefix)s_tasks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    created TIMESTAMP DEFAULT NOW(),
    data JSON,

    INDEX (created)
)"""

EXECUTION_TABLE = """\
CREATE TABLE IF NOT EXISTS %(prefix)s_executions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    task_id INT,
    steps JSON,

    started TIMESTAMP DEFAULT NOW(),
    last_contact TIMESTAMP DEFAULT 0,
    finished TIMESTAMP DEFAULT 0,

    UNIQUE INDEX (task_id),
    INDEX (started),
    INDEX (last_contact)
)"""

class StepAlreadyStarted(Exception):
    pass

class StepNotStarted(Exception):
    pass

class StepAlreadyFinished(Exception):
    pass

class AlreadyFinished(Exception):
    pass

class StepRunning(Exception):
    pass

class SQLStepQueue(object):
    def __init__(self, table_prefix="", execution_ttl=60 * 5):
        """
        Initialize the SQLStepQueue with the specified table prefix and
        execution TTL (in seconds).
        """
        self.execution_ttl = execution_ttl

        self.table_prefix = table_prefix.rstrip('_') + '_stepqueue'
        self.tasks_table = self.table_prefix + '_tasks'
        self.executions_table = self.table_prefix + '_executions'

        self._pool = ConnectionPool()

    ###############################
    # Public Interface

    def connect(self, host, port, user, password, database):
        """ Connect to the database specified """
        self._db_args = { 'host': host, 'port': port, 'user': user, 'password': password, 'database': database }
        with self._db_conn() as conn:
            conn.query('SELECT 1')
        return self

    def setup(self):
        """ Initialize the required tables in the database """
        with self._db_conn() as conn:
            conn.execute(PRIMARY_TABLE % { 'prefix': self.table_prefix })
            conn.execute(EXECUTION_TABLE % { 'prefix': self.table_prefix })
        return self

    def destroy(self):
        """ Destroy the SQLStepQueue tables in the database """
        with self._db_conn() as conn:
            for table_name in [self.tasks_table, self.executions_table]:
                conn.execute('DROP TABLE IF EXISTS %s' % table_name)
        return self

    def ready(self):
        """ Returns True if the tables have been setup, False otherwise """
        with self._db_conn() as conn:
            tables = [row.t for row in conn.query('''
                SELECT table_name AS t FROM information_schema.tables
                WHERE table_schema=%s
            ''', self._db_args['database'])]
        return self.tasks_table in tables and self.executions_table in tables

    def qsize(self):
        """ Return an approximate number of queued tasks in the queue. """
        count = self._query_queued('COUNT(*) AS count')
        return count[0].count

    def enqueue(self, data):
        """ Enqueue task with specified data. """
        jsonified_data = json.dumps(data)
        with self._db_conn() as conn:
            conn.execute(
                'INSERT INTO %s_tasks (data) VALUES (%%s)' % self.table_prefix,
                jsonified_data
            )

    def start(self, block=False, timeout=None, retry_interval=0.5):
        """
        Retrieve a task handler from the queue.

        If block is True, this function will block until it is able to retrieve a task.
        If block is True and timeout is a number it will block for at most <timeout> seconds.
        retry_interval is the time in seconds between successive retries.
        """
        start = time.time()
        while 1:
            task_handler = self._dequeue_task()
            if task_handler is None and block:
                if timeout is not None and (time.time() - start) > timeout:
                    break
                time.sleep(retry_interval)
            else:
                break
        return task_handler

    ###############################
    # Private Interface

    def _db_conn(self):
        return self._pool.connect(**self._db_args)

    def _query_queued(self, projection):
        with self._db_conn() as conn:
            result = conn.query('''
                SELECT
                    %(projection)s
                FROM
                    %(prefix)s_tasks AS tsk
                    LEFT JOIN %(prefix)s_executions AS exc ON tsk.id = exc.task_id
                WHERE
                    (
                        exc.task_id IS NULL
                        OR (
                            exc.finished = 0
                            AND exc.last_contact <= NOW() - INTERVAL %(ttl)s SECOND
                        )
                    )
                ORDER BY tsk.created ASC    -- oldest first
            ''' % {
                'projection': projection,
                'prefix': self.table_prefix,
                'ttl': self.execution_ttl
            })
        return result

    def _dequeue_task(self):
        try:
            with self._db_conn() as conn:
                conn.execute('''
                    DELETE FROM %(prefix)s_executions
                    WHERE
                        finished = 0
                        AND last_contact <= NOW() - INTERVAL %(ttl)s SECOND
                ''' % { 'prefix': self.table_prefix, 'ttl': self.execution_ttl })

                execution_id = conn.execute('''
                    INSERT INTO %(prefix)s_executions (task_id, last_contact, steps)
                    SELECT
                        tsk.id, NOW(), '[]'
                    FROM
                        %(prefix)s_tasks AS tsk
                        LEFT JOIN %(prefix)s_executions AS exc ON tsk.id = exc.task_id
                    WHERE
                        exc.task_id IS NULL
                    ORDER BY tsk.created ASC    -- oldest first
                    LIMIT 1
                ''' % {
                    'prefix': self.table_prefix,
                    'ttl': self.execution_ttl
                })

                if execution_id == 0:
                    # select returned no rows
                    return None

                return TaskHandler(execution_id=execution_id, queue=self)

        except MySQLError as (errno, msg):
            if errno == errorcodes.ER_DUP_ENTRY:
                return None
            else:
                raise

class TaskHandler(object):
    def __init__(self, execution_id, queue):
        self._execution_id = execution_id
        self._queue = queue

        self.finished = 0
        self.task_id = None
        self.data = None
        self.steps = None

        self._refresh()

    ###############################
    # Public Interface

    def valid(self):
        if self.finished != 0:
            return False

        with self._db_conn() as conn:
            row = conn.get('''
                SELECT
                    (last_contact > NOW() - INTERVAL %(ttl)s SECOND) AS valid
                FROM %(prefix)s_executions
                WHERE id = %%s
            ''' % {
                'prefix': self._queue.table_prefix,
                'ttl': self._queue.execution_ttl
            }, self._execution_id)

        return bool(row is not None and row.valid)

    def ping(self):
        """ Notify the queue that this task is still active. """
        if self.finished != 0:
            raise AlreadyFinished()

        with self._db_conn() as conn:
            conn.execute('''
                UPDATE %(prefix)s_executions
                SET last_contact=NOW()
                WHERE id = %%s
            ''' % { 'prefix': self._queue.table_prefix }, self._execution_id)

    def finish(self):
        if self._running_steps() != 0:
            raise StepRunning()
        if self.finished != 0:
            raise AlreadyFinished()

        self.finished = datetime.utcnow()
        self._save()

    def start_step(self, step_name):
        """ Start a step. """
        if self.finished != 0:
            raise AlreadyFinished()

        step_data = self._get_step(step_name)
        if step_data is not None:
            if 'stop' in step_data:
                raise StepAlreadyFinished()
            else:
                raise StepAlreadyStarted()

        self.steps.append({
            "start": datetime.utcnow(),
            "name": step_name
        })
        self._save()

    def stop_step(self, step_name):
        """ Stop a step. """
        if self.finished != 0:
            raise AlreadyFinished()

        step_data = self._get_step(step_name)
        if step_data is None:
            raise StepNotStarted()
        elif 'stop' in step_data:
            raise StepAlreadyFinished()

        step_data['stop'] = datetime.utcnow()
        step_data['duration'] = (step_data['stop'] - step_data['start']).total_seconds()
        self._save()

    @contextmanager
    def step(self, step_name):
        self.start_step(step_name)
        yield
        self.stop_step(step_name)

    ###############################
    # Private Interface

    def _get_step(self, step_name):
        for step in self.steps:
            if step['name'] == step_name:
                return step
        return None

    def _running_steps(self):
        return len([s for s in self.steps if 'stop' not in s])

    def _db_conn(self):
        return self._queue._db_conn()

    def _refresh(self):
        with self._db_conn() as conn:
            row = conn.get('''
                SELECT
                    exc.task_id, tsk.data, exc.steps
                FROM
                    %(prefix)s_tasks AS tsk
                    INNER JOIN %(prefix)s_executions AS exc ON tsk.id = exc.task_id
                WHERE exc.id = %%s
            ''' % { 'prefix': self._queue.table_prefix }, self._execution_id)

        self.task_id = row.task_id
        self.data = json.loads(row.data)
        self.steps = json.loads(row.steps)

    def _save(self):
        with self._db_conn() as conn:
            conn.execute('''
                UPDATE %(prefix)s_executions
                SET
                    last_contact=NOW(),
                    steps=%%s,
                    finished=%%s
                WHERE id = %%s
            ''' % {
                'prefix': self._queue.table_prefix
            }, json.dumps(self.steps), self.finished, self._execution_id)
