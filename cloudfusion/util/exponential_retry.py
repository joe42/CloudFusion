import time
from functools import wraps


def retry(ExceptionToCheck, tries=20, delay=0.1, backoff=2):
    """Retry calling the decorated function using an exponential backoff.
    The decorated method's object may define the _handle_error.
    The parameters are the exception object, the wrapped methods' name, and the original parameters.
    If _handle_error returns True, retrying is aborted.

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int 
    """
    def deco_retry(f):

        @wraps(f) #preserve original function name, docstring, arguments 
        def f_retry(self, *args, **kwargs):
            mdelay = delay; mbackoff = backoff
            for i in range(1, tries+1):
                remaining_tries = tries-i
                try:
                    return f(self, *args, **kwargs)
                except ExceptionToCheck, e:
                    if hasattr(self, '_handle_error'):
                        if self._handle_error(e, f.__name__, *args, **kwargs): #if error is handled, break
                            return f(self, *args, **kwargs)
                    time.sleep(mdelay)
                    mdelay *= mbackoff
                    if remaining_tries == 0:
                        raise e
            #end for
            
        return f_retry  # true decorator

    return deco_retry
