from copy import deepcopy
from threading import Thread
import time
import multiprocessing 
import copy
import pickle
import os

class GetFreeSpaceWorker(object):
    """Worker to cyclically poll for free space on store."""
    def __init__(self, store, logger, poll_wait_time_in_s=60*10):
        self.store = deepcopy(store)
        self.logger = logger
        self.poll_wait_time_in_s = poll_wait_time_in_s
        self._thread = Thread(target=self._run)
        self._thread.setDaemon(True)
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

class WriteWorker(object):
    def __init__(self, store, path, file, logger):
        self.store = copy.deepcopy(store)
        self.path = path
        self._filename = file.name
        self._filesize = os.path.getsize(file.name)
        file.close()
        self.logger = logger
        self.logger.debug("writing %s", path)
        self.interrupt_event = multiprocessing.Event()
        self._result_queue = multiprocessing.Queue()
        self.start_time = 0
        self.end_time = multiprocessing.Value('d', 0.0)
        self.process = multiprocessing.Process(target=self._run, args=(self._result_queue, self.interrupt_event, self.end_time))
        self._is_successful = False
        self._error = None 
        
    def get_duration(self):
        """Get duration of upload in seconds"""
        return self.end_time.value-self.start_time
    
    def get_endtime(self):
        """Get the end time of the upload in seconds from the epoche"""
        return self.end_time.value
    
    def get_starttime(self):
        """Get the start time of the upload in seconds from the epoche"""
        return self.start_time
    
    def get_filesize(self):
        """Get size of the file to write in bytes"""
        return self._filesize
    
    def is_finished(self):
        return not self.process.is_alive() 
    
    def get_error(self):
        return self._error
    
    def is_successful(self):
        if not self._result_queue.empty():
            result = self._result_queue.get()
            if result == True:
                self._is_successful = True
            else:
                self._is_successful = False
                self._error = result
        return self._is_successful
    
    def stop(self):
        self.interrupt_event.set()
        self.process.join()
        os.remove(self._filename)
        self.logger.debug("Stopped WriteWorker process to write %s", self.path)
    
    def start(self):
        self.start_time = time.time()
        self.process.start()
    
    def _run(self, result_queue, interrupt_event, end_time):
        self.logger.debug("Start WriteWorker process %s to write %s", os.getpid(), self.path)
        try:
            self.store.store_file(self._filename, os.path.dirname(self.path), os.path.basename(self.path), interrupt_event)
            end_time.value = time.time()
            result_queue.put(True)
        except Exception, e:
            self.logger.error("Error on storing %s in WriteWorker: %s", self.path, e)
            try:
                pickle.loads(pickle.dumps(e)) #check if exception can be de/serialized
                result_queue.put(e)
            except Exception, e:
                self.logger.error("Error on serializing exception in WriteWorker: %s", repr(e))
                result_queue.put(Exception(repr(e)))
        self.logger.debug("Finish WriteWorker process %s to write %s", os.getpid(), self.path)
            
class RemoveWorker(object):
    def __init__(self, store, path, logger):
        store.delete(path)
        self.store = copy.deepcopy(store)
        self.path = path
        self.logger = logger
        self.thread = Thread(target=self._run)
        self.successful = False
    
    def is_finished(self):
        return True#return not self.thread.is_alive()
    
    def is_successful(self):
        return True#return self.successful 
    
    def stop(self):
        pass#self.thread.join()
    
    def start(self):
        #self.thread.start()
        pass
    
    def _run(self):
        try:
            self.store.delete(self.path)
            self.successful = True
        except Exception, e:
            self.logger.debug("Error on removing %s in RemoveWorker: %s", self.path, e)

class ReadWorker(object):
    def __init__(self, store, path, logger):
        self.store = copy.deepcopy(store)
        self.path = path
        self._filesize = None
        self.logger = logger
        self._result_queue = multiprocessing.Queue()
        self.start_time = 0
        self.end_time = multiprocessing.Value('d', 0.0)
        self.process = multiprocessing.Process(target=self._run, args=(self._result_queue, self.end_time))

    def get_duration(self):
        """Get duration of download in seconds"""
        return self.end_time.value-self.start_time
    
    def get_starttime(self):
        """Get the start time of the download in seconds from the epoche"""
        return self.start_time
    
    def get_endtime(self):
        """Get the end time of the download in seconds from the epoche"""
        return self.end_time.value
    
    def get_filesize(self):
        """Get size of the file to write in bytes"""
        if not self._filesize:
            raise RuntimeError, 'Cannot obtain file size: the download did not yet end'
        return self._filesize
    
    def is_finished(self):
        return not self.process.is_alive() 
    
    def is_successful(self):
        return not self._result_queue.empty()
    
    def get_result(self): 
        """Get the data of the read file.
        This only works once after a successful read and is a blocking call.
        Use is_successful to check if the read has been successful without blocking.
        """ 
        return self._result_queue.get()
    
    def stop(self):
        self.process.terminate()
    
    def start(self):
        self.start_time = time.time()
        self.process.start()
    
    def _run(self, result_queue, end_time):
        try:
            start = time.time()
            content = self.store.get_file(self.path)
            end_time.value = time.time() - start
            result_queue.put(content)
        except Exception, e:
            self.logger.debug("Error on reading %s in ReadWorker: %s", self.path, e)
        