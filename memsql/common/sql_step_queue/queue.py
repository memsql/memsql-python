import sys
import time
import random
import uuid
from datetime import datetime

from memsql.common import json
from memsql.common import sql_utility
from memsql.common import database
from memsql.common.sql_step_queue.task_handler import TaskHandler

PRIMARY_TABLE = """\
CREATE TABLE IF NOT EXISTS %(table_name)s (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    created DATETIME NOT NULL,

    data JSON,

    execution_id CHAR(32) DEFAULT NULL,
    steps JSON,

    started DATETIME,
    last_contact DATETIME,
    update_count INT UNSIGNED DEFAULT 0 NOT NULL,
    finished DATETIME,

    result AS data::$result PERSISTED VARCHAR(255),

    INDEX (created),
    INDEX (started),
    INDEX (last_contact)
)"""

class SQLStepQueue(sql_utility.SQLUtility):
    def __init__(self, table_name, execution_ttl=60, task_handler_class=TaskHandler):
        """
        table_name      the table name for the queue in the DB
        execution_ttl   the amount of time (in seconds) that can pass before a task is automatically requeued
        """
        super(SQLStepQueue, self).__init__()

        self.table_name = table_name
        self.execution_ttl = execution_ttl
        self.TaskHandlerClass = task_handler_class
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
            return conn.execute(
                'INSERT INTO %s (created, data) VALUES (%%(created)s, %%(data)s)' % self.table_name,
                created=datetime.utcnow(),
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
                    last_contact = %%(now)s,
                    update_count = update_count + 1,
                    steps = '[]',
                    started = %%(now)s,
                    finished = %%(now)s,
                    data::$result = %%(result)s
                WHERE
                    finished IS NULL
                    AND (
                        execution_id IS NULL
                        OR last_contact <= %%(now)s - INTERVAL %%(ttl)s SECOND
                    )
                    %s
            ''' % (self.table_name, extra_predicate_sql),
                now=datetime.utcnow(),
                result=result,
                ttl=self.execution_ttl)

        return num_affected

    def checkout(self, task_id, execution_id):
        return self.TaskHandlerClass(execution_id=execution_id, task_id=task_id, queue=self)

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
                    finished IS NULL
                    AND (
                        execution_id IS NULL
                        OR last_contact <= %%(now)s - INTERVAL %%(ttl)s SECOND
                    )
                    %s
                ORDER BY created ASC
                LIMIT %%(limit)s
            ''' % (projection, self.table_name, extra_predicate_sql),
                now=datetime.utcnow(),
                ttl=self.execution_ttl,
                limit=sys.maxsize if limit is None else limit)
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
                            last_contact = %%(now)s,
                            update_count = update_count + 1,
                            started = %%(now)s,
                            steps = '[]'
                        WHERE
                            id = %%(task_id)s
                            AND finished IS NULL
                            AND (
                                execution_id IS NULL
                                OR last_contact <= %%(now)s - INTERVAL %%(ttl)s SECOND
                            )
                            %s
                    ''' % (self.table_name, extra_predicate_sql),
                        now=datetime.utcnow(),
                        execution_id=execution_id,
                        task_id=possible_task.id,
                        ttl=self.execution_ttl)

                    if success == 1:
                        return self.TaskHandlerClass(execution_id=execution_id, task_id=possible_task.id, queue=self)

    def _build_extra_predicate(self, extra_predicate):
        """ This method is a good one to extend if you want to create a queue which always applies an extra predicate. """
        if extra_predicate is None:
            return ''

        # if they don't have a supported format seq, wrap it for them
        if not isinstance(extra_predicate[1], (list, dict, tuple)):
            extra_predicate = [extra_predicate[0], (extra_predicate[1], )]

        extra_predicate = database.escape_query(*extra_predicate)

        return 'AND (' + extra_predicate + ')'
