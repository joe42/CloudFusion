from copy import deepcopy
from threading import Thread
import time
import multiprocessing 
import copy
import pickle
import os
from cloudfusion.store.store import NoSuchFilesytemObjectError

class GetFreeSpaceWorker(object):
    """Worker to cyclically poll for free space on store."""
    def __init__(self, store, logger, poll_wait_time_in_s=60*10):
        self.store = deepcopy(store)
        self.logger = logger
        self.poll_wait_time_in_s = poll_wait_time_in_s
        self._thread = None
        self._stop = False
        self.free_bytes = 30000000
    
    def get_free_bytes_in_remote_store(self):
        return self.free_bytes  
    
    def start(self):
        self._thread = Thread(target=self._run)
        self._thread.setDaemon(True)
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
        self.interrupt_event = multiprocessing.Event()
        self._result_queue = multiprocessing.Queue()
        self.start_time = 0
        self.end_time = multiprocessing.Value('d', 0.0)
        self._update_time = None
        self.process = multiprocessing.Process(target=self._run, args=(self._result_queue, self.interrupt_event, self.end_time))
        self._is_successful = False
        self._error = None 
        
    def get_duration(self):
        """Get duration of upload in seconds"""
        if self.start_time == 0 or self.end_time.value == 0:
            raise RuntimeError, 'Cannot obtain duration: the upload did not yet end'
        return self.end_time.value-self.start_time
    
    def get_endtime(self):
        """Get the end time of the upload in seconds from the epoche"""
        if self.end_time.value == 0:
            raise RuntimeError, 'Cannot obtain end time: the upload did not yet end'
        return self.end_time.value
    
    def get_starttime(self):
        """Get the start time of the upload in seconds from the epoche"""
        if self.start_time == 0:
            raise RuntimeError, 'Cannot obtain start time: the upload did not yet start'
        return self.start_time
    
    def get_updatetime(self):
        """Get the point of time the file has been updated in the store in seconds from the epoche"""
        if not self._update_time:
            raise RuntimeError, 'Cannot obtain last point of time the file has been updated: the upload did not yet end'
        return self._update_time
    
    def get_filesize(self):
        """Get size of the file to write in bytes"""
        return self._filesize
    
    def is_finished(self):
        return not self.process.is_alive() 
    
    def get_error(self):
        self._check_result()
        return self._error
    
    def _check_result(self):
        if not self._result_queue.empty():
            self._remove_tmpfile()
            result = self._result_queue.get()
            if isinstance( result, ( int, long, float ) ):
                self._update_time = result
                self._is_successful = True
            else:
                self._is_successful = False
                self._error = result
    
    def is_successful(self):
        self._check_result()
        return self._is_successful
    
    def stop(self):
        self.interrupt_event.set()
        self.process.join(60)
        if not self.process.is_alive():
            print "stop joined"
            self._error = Exception("Stopped WriteWorker process %s to write %s", self.process.pid, self.path)
        else:
            import os
            print "forceful terminateion1"
            self.process.terminate()
            print "forceful terminateion2 %s" % self.process.pid
            os.system('kill -9 {0}'.format(self.process.pid)) 

            self._error = Exception("Forcefully terminated WriteWorker process %s to write %s", self.process.pid, self.path) 
        self.process.terminate()
        print "forceful terminateion3"
        self.end_time.value = time.time()
        self._remove_tmpfile()
        self.logger.debug("Stopped WriteWorker process %s to write %s", self.process.pid, self.path)
        
    def _remove_tmpfile(self):
        if os.path.exists(self._filename):
            os.remove(self._filename)
    
    def start(self):
        self.start_time = time.time()
        self.logger.debug("Create WriteWorker process to write %s", self.path)
        self.process.start()
        
    def _run(self, result_queue, interrupt_event, end_time):
        self.logger.debug("Start WriteWorker process %s to write %s", os.getpid(), self.path)
        try:
            update_time = self.store.store_file(self._filename, os.path.dirname(self.path), os.path.basename(self.path), interrupt_event)
            end_time.value = time.time()
            if not update_time:
                update_time = end_time.value
            result_queue.put(update_time)
        except Exception, e:
            self.logger.exception("Error on storing %s in WriteWorker", self.path)
            try:
                pickle.loads(pickle.dumps(e)) #check if exception can be de/serialized
                result_queue.put(e)
            except Exception:
                self.logger.error("Error on serializing exception in WriteWorker: %s", repr(e))
                result_queue.put(Exception(repr(e)))
        self.logger.debug("Finish WriteWorker process %s to write %s", os.getpid(), self.path)
            
class RemoveWorker(object):
    def __init__(self, store, path, logger):
        store.delete(path)
        self.store = copy.deepcopy(store)
        self.path = path
        self.thread = None
        self.logger = logger
        self.successful = False
    
    def is_finished(self):
        return not self.thread.is_alive()
    
    def is_successful(self):
        return self.successful 
    
    def stop(self):
        self.thread.join()
    
    def start(self):
        self.thread = Thread(target=self._run)
        self.thread.setDaemon(True)
        self.thread.start()
        self.thread.join()
    
    def _run(self):
        try:
            self.store.delete(self.path)
            self.successful = True
        except NoSuchFilesytemObjectError, e: #file does not exist anyway
            self.successful = True
        except Exception, e:
            self.logger.debug("Error on removing %s in RemoveWorker: %s", self.path, e)

class ReadWorker(object):
    def __init__(self, store, path, logger):
        self.store = copy.deepcopy(store)
        self.path = path
        self._filesize = -1
        self.logger = logger
        self._result_queue = multiprocessing.Queue()
        self._temp_result = None 
        self._is_successful = False
        self._error = None
        self.start_time = 0
        self.end_time = multiprocessing.Value('d', 0.0)
        self.process = multiprocessing.Process(target=self._run, args=(self._result_queue, self.end_time))

    def get_duration(self):
        """Get duration of download in seconds"""
        if self.start_time == 0 or self.end_time.value == 0:
            raise RuntimeError, 'Cannot obtain end time: the download did not yet end'
        return self.end_time.value-self.start_time
    
    def get_starttime(self):
        """Get the start time of the download in seconds from the epoche"""
        if self.start_time == 0:
            raise RuntimeError, 'Cannot obtain end time: the download did not yet start'
        return self.start_time
    
    def get_endtime(self):
        """Get the end time of the download in seconds from the epoche"""
        if self.end_time.value == 0:
            raise RuntimeError, 'Cannot obtain end time: the download did not yet end'
        return self.end_time.value
    
    def get_filesize(self):
        """Get size of the file to write in bytes"""
        if self._filesize == -1: # might be zero
            raise RuntimeError, 'Cannot obtain file size: the download did not yet end'
        return self._filesize
    
    def is_finished(self):
        return not self.process.is_alive() 
    
    def is_successful(self):
        self._check_result()
        return self._is_successful
    
    def get_error(self):
        self._check_result()
        return self._error
    
    def get_result(self): 
        """Get the data of the read file.
        This only works once after a successful read and is a blocking call.
        Use is_successful to check if the read has been successful without blocking.
        """ 
        if not self._temp_result:
            self._get_result()
        ret = self._temp_result
        self._temp_result = None
        return ret
    
    def stop(self):
        pass #Remove terminate to allow for logging with multiprocessing
    
    def start(self):
        self.start_time = time.time()
        self.process.start()

    def _check_result(self):
        if not self._result_queue.empty():
            self._get_result()
    
    def _get_result(self):
        result = self._result_queue.get()
        if isinstance(result, str) or isinstance(result, unicode):
            self._is_successful = True
            self._temp_result = result
            self._filesize = len(result)
        else:
            self._is_successful = False
            self._error = result

    def _run(self, result_queue, end_time):
        self.logger.debug("Starting ReadWorker process %s to read %s", os.getpid(), self.path)
        try:
            content = self.store.get_file(self.path)
            end_time.value = time.time() 
            result_queue.put(content)
        except Exception, e:
            self.logger.exception("Error on reading %s in ReadWorker", self.path)
            try:
                pickle.loads(pickle.dumps(e)) #check if exception can be de/serialized
                result_queue.put(e)
            except Exception:
                self.logger.error("Error on serializing exception in ReadWorker: %s", repr(e))
                result_queue.put(Exception(repr(e)))
        self.logger.debug("Finish ReadWorker process %s to read %s", os.getpid(), self.path)
        
