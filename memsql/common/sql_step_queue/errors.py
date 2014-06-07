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
