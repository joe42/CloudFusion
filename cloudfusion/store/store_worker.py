from copy import deepcopy
from threading import Thread
import time

class GetFreeSpaceWorker(object):
    """Worker to cyclically poll for free space on store."""
    def __init__(self, store, logger, poll_wait_time_in_s=60*10):
        self.store = deepcopy(store)
        self.logger = logger
        self.poll_wait_time_in_s = poll_wait_time_in_s
        self._thread = Thread(target=self._run)
        self._stop = False
        self.free_bytes = 30000000
    
    def get_free_bytes_in_remote_store(self):
        return self.free_bytes  
    
    def start(self):
        if self._thread:
            self._stop = False
            self._thread.start()
    
    def is_alive(self):
        if self._thread:        
            return self._thread.is_alive()
        else:
            return False
        
    def stop(self):
        self._stop = True
    
    def _run(self):
        while not self._stop:
            try:
                self.free_bytes = self.store.get_free_space()
            except Exception, e:
                self.logger.error("Error on getting number of free bytes of store: "+ str(e))
            time.sleep(self.poll_wait_time_in_s)
    
    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k == '_thread':
                setattr(result, k, None)
            elif k == 'logger':
                setattr(result, k, self.logger)
            else:
                setattr(result, k, deepcopy(v, memo))
        return result