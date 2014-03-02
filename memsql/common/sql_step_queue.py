from memsql.common import json, sql_utility, database, util
import sys
import time
import random
import uuid
import copy
import types
from datetime import datetime
from contextlib import contextmanager

PRIMARY_TABLE = """\
CREATE TABLE IF NOT EXISTS %(table_name)s (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    created INT NOT NULL,

    data JSON,

    execution_id CHAR(32) DEFAULT NULL,
    steps JSON,

    started INT DEFAULT 0 NOT NULL,
    last_contact INT DEFAULT 0 NOT NULL,
    finished INT DEFAULT 0 NOT NULL,

    result AS data::$result PERSISTED VARCHAR(255),

    INDEX (created),
    INDEX (started),
    INDEX (last_contact)
)"""

class SQLStepQueueException(Exception):
    pass

class TaskDoesNotExist(SQLStepQueueException):
    pass

class StepAlreadyStarted(SQLStepQueueException):
    pass

class StepNotStarted(SQLStepQueueException):
    pass

class StepAlreadyFinished(SQLStepQueueException):
    pass

class AlreadyFinished(SQLStepQueueException):
    pass

class StepRunning(SQLStepQueueException):
    pass

class SQLStepQueue(sql_utility.SQLUtility):
    def __init__(self, table_name, execution_ttl=60):
        """
        table_name      the table name for the queue in the DB
        execution_ttl   the amount of time (in seconds) that can pass before a task is automatically requeued
        """
        super(SQLStepQueue, self).__init__()

        self.table_name = table_name
        self.execution_ttl = execution_ttl
        self._define_table(self.table_name, PRIMARY_TABLE % { 'table_name': self.table_name })

    ###############################
    # Public Interface

    def qsize(self, extra_predicate=None):
        """ Return an approximate number of queued tasks in the queue. """
        count = self._query_queued('COUNT(*) AS count', extra_predicate=extra_predicate)
        return count[0].count

    def enqueue(self, data):
        """ Enqueue task with specified data. """
        jsonified_data = json.dumps(data)
        with self._db_conn() as conn:
            conn.execute(
                'INSERT INTO %s (created, data) VALUES (UNIX_TIMESTAMP(), %%(data)s)' % self.table_name,
                data=jsonified_data
            )

    def start(self, block=False, timeout=None, retry_interval=0.5, extra_predicate=None):
        """
        Retrieve a task handler from the queue.

        If block is True, this function will block until it is able to retrieve a task.
        If block is True and timeout is a number it will block for at most <timeout> seconds.
        retry_interval is the maximum time in seconds between successive retries.

        extra_predicate
        If extra_predicate is defined, it should be a tuple of (raw_predicate, predicate_args)
        raw_predicate will be prefixed by AND, and inserted into the WHERE condition in the queries.
        predicate_args will be sql escaped and formatted into raw_predicate.
        """
        start = time.time()
        while 1:
            task_handler = self._dequeue_task(extra_predicate)
            if task_handler is None and block:
                if timeout is not None and (time.time() - start) > timeout:
                    break
                time.sleep(retry_interval * (random.random() + 0.1))
            else:
                break
        return task_handler

    def bulk_finish(self, result='cancelled', extra_predicate=None):
        extra_predicate_sql = self._build_extra_predicate(extra_predicate)

        with self._db_conn() as conn:
            num_affected = conn.query('''
                UPDATE %s
                SET
                    execution_id = 0,
                    last_contact = UNIX_TIMESTAMP(),
                    steps = '[]',
                    started = UNIX_TIMESTAMP(),
                    finished = UNIX_TIMESTAMP(),
                    data::$result = %%(result)s
                WHERE
                    finished = 0
                    AND (
                        execution_id IS NULL
                        OR last_contact <= UNIX_TIMESTAMP() - %%(ttl)s
                    )
                    %s
            ''' % (self.table_name, extra_predicate_sql),
                result=result,
                ttl=self.execution_ttl)

        return num_affected

    ###############################
    # Private Interface

    def _query_queued(self, projection, limit=None, extra_predicate=None):
        extra_predicate_sql = self._build_extra_predicate(extra_predicate)

        with self._db_conn() as conn:
            result = conn.query('''
                SELECT
                    %s
                FROM %s
                WHERE
                    finished = 0
                    AND (
                        execution_id IS NULL
                        OR last_contact <= UNIX_TIMESTAMP() - %%(ttl)s
                    )
                    %s
                ORDER BY created ASC
                LIMIT %%(limit)s
            ''' % (projection, self.table_name, extra_predicate_sql),
                ttl=self.execution_ttl,
                limit=sys.maxint if limit is None else limit)
        return result

    def _dequeue_task(self, extra_predicate=None):
        execution_id = uuid.uuid1().hex

        extra_predicate_sql = self._build_extra_predicate(extra_predicate)

        with self._db_conn() as conn:
            while True:
                possible_tasks = self._query_queued('id, created, data', limit=5, extra_predicate=extra_predicate)

                if not possible_tasks:
                    # nothing to dequeue
                    return None

                for possible_task in possible_tasks:
                    # attempt to claim the task
                    success = conn.query('''
                        UPDATE %s
                        SET
                            execution_id = %%(execution_id)s,
                            last_contact = UNIX_TIMESTAMP(),
                            started = UNIX_TIMESTAMP(),
                            steps = '[]'
                        WHERE
                            id = %%(task_id)s
                            AND finished = 0
                            AND (
                                execution_id IS NULL
                                OR last_contact <= UNIX_TIMESTAMP() - %%(ttl)s
                            )
                            %s
                    ''' % (self.table_name, extra_predicate_sql),
                        execution_id=execution_id,
                        task_id=possible_task.id,
                        ttl=self.execution_ttl)

                    if success == 1:
                        return TaskHandler(execution_id=execution_id, task_id=possible_task.id, queue=self)

    def _build_extra_predicate(self, extra_predicate):
        """ This method is a good one to extend if you want to create a queue which always applies an extra predicate. """
        if extra_predicate is None:
            return ''

        # if they don't have a supported format seq, wrap it for them
        if not isinstance(extra_predicate[1], (types.ListType, types.DictType, types.TupleType)):
            extra_predicate = [extra_predicate[0], (extra_predicate[1], )]

        extra_predicate = database.escape_query(*extra_predicate)

        return 'AND (' + extra_predicate + ')'

class TaskHandler(object):
    def __init__(self, execution_id, task_id, queue):
        self.execution_id = execution_id
        self.task_id = task_id
        self._queue = queue

        self.started = 0
        self.finished = 0
        self.data = None
        self.steps = None

        self._refresh()

    ###############################
    # Public Interface

    def valid(self):
        """ Check to see if we are still active. """
        if self.finished != 0:
            return False

        with self._db_conn() as conn:
            row = conn.get('''
                SELECT (last_contact > UNIX_TIMESTAMP() - %%(ttl)s) AS valid
                FROM %s
                WHERE
                    id = %%(task_id)s
                    AND execution_id = %%(execution_id)s
            ''' % self._queue.table_name,
                ttl=self._queue.execution_ttl,
                task_id=self.task_id,
                execution_id=self.execution_id)

        return bool(row is not None and row.valid)

    def ping(self):
        """ Notify the queue that this task is still active. """
        if self.finished != 0:
            raise AlreadyFinished()

        with self._db_conn() as conn:
            success = conn.query('''
                UPDATE %s
                SET last_contact=UNIX_TIMESTAMP()
                WHERE
                    id = %%(task_id)s
                    AND execution_id = %%(execution_id)s
                    AND last_contact > UNIX_TIMESTAMP() - %%(ttl)s
            ''' % self._queue.table_name,
                task_id=self.task_id,
                execution_id=self.execution_id,
                ttl=self._queue.execution_ttl)

        if success != 1:
            raise TaskDoesNotExist()

    def finish(self, result='success'):
        if self._running_steps() != 0:
            raise StepRunning()
        if self.finished != 0:
            raise AlreadyFinished()

        self._save(result=result, finished=datetime.utcnow())

    def requeue(self):
        if self._running_steps() != 0:
            raise StepRunning()
        if self.finished != 0:
            raise AlreadyFinished()

        with self._db_conn() as conn:
            success = conn.query('''
                UPDATE %s
                SET
                    last_contact=0,
                    started=0,
                    steps=NULL,
                    execution_id=NULL,
                    finished=0,
                    data=JSON_DELETE_KEY(data, 'result')
                WHERE
                    id = %%(task_id)s
                    AND execution_id = %%(execution_id)s
                    AND last_contact > UNIX_TIMESTAMP() - %%(ttl)s
            ''' % self._queue.table_name,
                task_id=self.task_id,
                execution_id=self.execution_id,
                ttl=self._queue.execution_ttl)

        if success != 1:
            raise TaskDoesNotExist()

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

        steps = copy.deepcopy(self.steps)
        steps.append({
            "start": datetime.utcnow(),
            "name": step_name
        })
        self._save(steps=steps)

    def stop_step(self, step_name):
        """ Stop a step. """
        if self.finished != 0:
            raise AlreadyFinished()

        steps = copy.deepcopy(self.steps)

        step_data = self._get_step(step_name, steps=steps)
        if step_data is None:
            raise StepNotStarted()
        elif 'stop' in step_data:
            raise StepAlreadyFinished()

        step_data['stop'] = datetime.utcnow()
        step_data['duration'] = util.timedelta_total_seconds(step_data['stop'] - step_data['start'])
        self._save(steps=steps)

    @contextmanager
    def step(self, step_name):
        self.start_step(step_name)
        yield
        self.stop_step(step_name)

    ###############################
    # Private Interface

    def _get_step(self, step_name, steps=None):
        for step in (steps if steps is not None else self.steps):
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
                SELECT * FROM %s
                WHERE
                    id = %%(task_id)s
                    AND execution_id = %%(execution_id)s
                    AND last_contact > UNIX_TIMESTAMP() - %%(ttl)s
            ''' % self._queue.table_name,
                task_id=self.task_id,
                execution_id=self.execution_id,
                ttl=self._queue.execution_ttl)

        if not row:
            raise TaskDoesNotExist()

        self.task_id = row.id
        self.data = json.loads(row.data)
        self.steps = json.loads(row.steps)
        self.started = row.started
        self.finished = row.finished

    def _save(self, result=None, finished=None, steps=None):
        with self._db_conn() as conn:
            success = conn.query('''
                UPDATE %s
                SET
                    last_contact=UNIX_TIMESTAMP(),
                    steps=%%(steps)s,
                    finished=%%(finished)s,
                    data::$result=%%(result)s
                WHERE
                    id = %%(task_id)s
                    AND execution_id = %%(execution_id)s
                    AND last_contact > UNIX_TIMESTAMP() - %%(ttl)s
            ''' % self._queue.table_name,
                task_id=self.task_id,
                execution_id=self.execution_id,
                ttl=self._queue.execution_ttl,
                steps=json.dumps(steps if steps is not None else self.steps),
                finished=finished if finished is not None else self.finished,
                result=result if result is not None else self.data.get('result', None))

        if success != 1:
            raise TaskDoesNotExist()
        else:
            if steps is not None:
                self.steps = steps
            if finished is not None:
                self.finished = finished
            if result is not None:
                self.data['result'] = result
