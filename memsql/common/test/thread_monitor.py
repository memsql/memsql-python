import sys

try:
    import queue
except ImportError:
    import Queue as queue

class ThreadMonitor(object):
    """Helper class for catching exceptions generated in threads.
    http://blog.eugeneoden.com/2008/05/12/testing-threads-with-pytest/

    Usage:

        mon = ThreadMonitor()

        th = threading.Thread(target=mon.wrap(myFunction))
        th.start()

        th.join()

        mon.check() # raises any exception generated in the thread

    Any raised exception will include a traceback from the original
    thread, not the function calling mon.check()

    Works for multiple threads
    """
    def __init__(self):
        self.queue = queue.Queue()

    def wrap(self, function):
        def threadMonitorWrapper(*args, **kw):
            try:
                ret = function(*args, **kw)
            except Exception:
                self.queue.put(sys.exc_info())
                raise

            return ret

        return threadMonitorWrapper

    def check(self):
        try:
            item = self.queue.get(block=False)
        except queue.Empty:
            return

        klass, value, tb = item
        exc = klass(value)

        if hasattr(exc, "with_traceback"):
            raise exc.with_traceback(tb)
        else:
            raise exc
