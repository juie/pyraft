import threading
import inspect
import ctypes
import types


def _async_raise(tid, exc_type):
    """raises the exception, performs cleanup if needed"""
    tid = ctypes.c_long(tid)
    if not inspect.isclass(exc_type):
        exc_type = type(exc_type)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exc_type))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
        raise SystemError("PyThreadState_SetAsyncExc failed")


# def stop_thread(thread):
#     _async_raise(thread.ident, SystemExit)

def stop(self):
    _async_raise(self.ident, SystemExit)


def spawn(*args, **kwargs):
    t = threading.Thread(*args, **kwargs)
    t.setDaemon(True)
    t.stop = types.MethodType(stop, t)
    t.start()
    return t
