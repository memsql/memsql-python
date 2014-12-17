import copy
from datetime import datetime
from dateutil import parser
from contextlib import contextmanager

from memsql.common import json
from memsql.common import util
from memsql.common.sql_step_queue.errors import TaskDoesNotExist, StepAlreadyStarted, StepNotStarted, StepAlreadyFinished, StepRunning, AlreadyFinished

class TaskHandler(object):
    def __init__(self, execution_id, task_id, queue):
        self.execution_id = execution_id
        self.task_id = task_id
        self._queue = queue

        self.started = 0
        self.finished = None
        self.data = None
        self.steps = None

        self._refresh()

    ###############################
    # Public Interface

    def valid(self):
        """ Check to see if we are still active. """
        if self.finished is not None:
            return False

        with self._db_conn() as conn:
            row = conn.get('''
                SELECT (last_contact > %%(now)s - INTERVAL %%(ttl)s SECOND) AS valid
                FROM %s
                WHERE
                    id = %%(task_id)s
                    AND execution_id = %%(execution_id)s
            ''' % self._queue.table_name,
                now=datetime.utcnow(),
                ttl=self._queue.execution_ttl,
                task_id=self.task_id,
                execution_id=self.execution_id)

        return bool(row is not None and row.valid)

    def ping(self):
        """ Notify the queue that this task is still active. """
        if self.finished is not None:
            raise AlreadyFinished()

        with self._db_conn() as conn:
            success = conn.query('''
                UPDATE %s
                SET
                    last_contact=%%(now)s,
                    update_count=update_count + 1
                WHERE
                    id = %%(task_id)s
                    AND execution_id = %%(execution_id)s
                    AND last_contact > %%(now)s - INTERVAL %%(ttl)s SECOND
            ''' % self._queue.table_name,
                now=datetime.utcnow(),
                task_id=self.task_id,
                execution_id=self.execution_id,
                ttl=self._queue.execution_ttl)

        if success != 1:
            raise TaskDoesNotExist()

    def finish(self, result='success'):
        if self._running_steps() != 0:
            raise StepRunning()
        if self.finished is not None:
            raise AlreadyFinished()

        data = copy.deepcopy(self.data)
        data['result'] = result
        self._save(data=data, finished=datetime.utcnow())

    def requeue(self):
        if self._running_steps() != 0:
            raise StepRunning()
        if self.finished is not None:
            raise AlreadyFinished()

        with self._db_conn() as conn:
            success = conn.query('''
                UPDATE %s
                SET
                    last_contact=NULL,
                    update_count=update_count + 1,
                    started=NULL,
                    steps=NULL,
                    execution_id=NULL,
                    finished=NULL,
                    data=JSON_DELETE_KEY(data, 'result')
                WHERE
                    id = %%(task_id)s
                    AND execution_id = %%(execution_id)s
                    AND last_contact > %%(now)s - INTERVAL %%(ttl)s SECOND
            ''' % self._queue.table_name,
                now=datetime.utcnow(),
                task_id=self.task_id,
                execution_id=self.execution_id,
                ttl=self._queue.execution_ttl)

        if success != 1:
            raise TaskDoesNotExist()

    def start_step(self, step_name):
        """ Start a step. """
        if self.finished is not None:
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
        if self.finished is not None:
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

    def refresh(self):
        self._refresh()

    def save(self):
        self._save()

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
                    AND last_contact > %%(now)s - INTERVAL %%(ttl)s SECOND
            ''' % self._queue.table_name,
                now=datetime.utcnow(),
                task_id=self.task_id,
                execution_id=self.execution_id,
                ttl=self._queue.execution_ttl)

        if not row:
            raise TaskDoesNotExist()

        self.task_id = row.id
        self.data = json.loads(row.data)
        self.steps = self._load_steps(json.loads(row.steps))
        self.started = row.started
        self.finished = row.finished

    def _load_steps(self, raw_steps):
        """ load steps -> basically load all the datetime isoformats into datetimes """
        for step in raw_steps:
            if 'start' in step:
                step['start'] = parser.parse(step['start'])
            if 'stop' in step:
                step['stop'] = parser.parse(step['stop'])
        return raw_steps

    def _save(self, finished=None, steps=None, data=None):
        with self._db_conn() as conn:
            success = conn.query('''
                UPDATE %s
                SET
                    last_contact=%%(now)s,
                    update_count=update_count + 1,
                    steps=%%(steps)s,
                    finished=%%(finished)s,
                    data=%%(data)s
                WHERE
                    id = %%(task_id)s
                    AND execution_id = %%(execution_id)s
                    AND last_contact > %%(now)s - INTERVAL %%(ttl)s SECOND
            ''' % self._queue.table_name,
                now=datetime.utcnow(),
                task_id=self.task_id,
                execution_id=self.execution_id,
                ttl=self._queue.execution_ttl,
                steps=json.dumps(steps if steps is not None else self.steps),
                finished=finished if finished is not None else self.finished,
                data=json.dumps(data if data is not None else self.data))

        if success != 1:
            raise TaskDoesNotExist()
        else:
            if steps is not None:
                self.steps = steps
            if finished is not None:
                self.finished = finished
            if data is not None:
                self.data = data
