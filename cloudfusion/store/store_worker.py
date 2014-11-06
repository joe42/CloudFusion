from copy import deepcopy
from threading import Thread, Lock
import time
import multiprocessing 
import copy
import pickle
import os
from cloudfusion.store.store import NoSuchFilesytemObjectError
from cloudfusion.store.transparent_store import ExceptionStats
from cloudfusion.mylogging import db_logging_thread
import psutil
from psutil import NoSuchProcess


class LeightWeightValue(object):
    '''To replace multiprocessing.Value.'''
    def __init__(self, val):
        self.value = val
        
class WorkerStats(object):
    '''Statistics about workers. Resets statistics automatically after 100*1000 workers.'''
    def __init__(self):
        self.uploaded = 0
        self.downloaded = 0
        self.exceptions_log = {}
        self.write_workers = []
        self.read_workers = []
        
    def _log_exception(self, exception):
        self.exceptions_log = ExceptionStats.add_exception(exception, self.exceptions_log)
    
    def reset(self):
        '''Resets all statistics.'''
        self.uploaded = 0
        self.downloaded = 0
        self.exceptions_log = {}
        self.write_workers = []
        self.read_workers = []
    
    def add_finished_worker(self, worker):
        if len(self.write_workers) > 100*1000 or len(self.read_workers) > 100*1000:
            return self.reset()
        if isinstance(worker, WriteWorker):
            if worker.get_error():
                self._log_exception(worker.get_error())
                return
            if worker.is_successful():
                self.write_workers.append(worker)
                self.uploaded += worker.get_filesize()
        if isinstance(worker, ReadWorker):
            if worker.get_error():
                self._log_exception(worker.get_error())
                return
            if worker.is_successful():
                self.read_workers.append(worker)
                self.downloaded += worker.get_filesize()
            
    def get_download_time(self):
        '''Get download time considering parallel downloads.'''
        START=0; END=1
        if len(self.read_workers) == 0:
            return 0
        ret = 0
        self.read_workers = sorted(self.read_workers, key=lambda w: w.get_starttime())
        worker1 = self.read_workers[0]
        longest_connection = [worker1.get_starttime(), worker1.get_endtime()]
        for worker2 in self.read_workers[1:]:
            if longest_connection[START] <= worker2.get_starttime() <= longest_connection[END]:
                if longest_connection[END] <= worker2.get_endtime():
                    longest_connection[END] = worker2.get_endtime()
            else:
                ret += longest_connection[END] - longest_connection[START]
                longest_connection = [worker2.get_starttime(), worker2.get_endtime()]
        ret += longest_connection[END] - longest_connection[START]
        return ret
            
    def get_upload_time(self):
        '''Get download time considering parallel uploads.'''
        START=0; END=1
        if len(self.write_workers) == 0:
            return 0
        ret = 0
        self.write_workers = sorted(self.write_workers, key=lambda w: w.get_starttime())
        worker1 = self.write_workers[0]
        longest_connection = [worker1.get_starttime(), worker1.get_endtime()]
        for worker2 in self.write_workers[1:]:
            if longest_connection[START] <= worker2.get_starttime() <= longest_connection[END]:
                if longest_connection[END] <= worker2.get_endtime():
                    longest_connection[END] = worker2.get_endtime()
            else:
                ret += longest_connection[END] - longest_connection[START]
                longest_connection = [worker2.get_starttime(), worker2.get_endtime()]
        ret += longest_connection[END] - longest_connection[START]
        return ret
            
    def get_download_rate(self):
        if self.get_download_time() == 0:
            return 0
        return float(self.downloaded) / self.get_download_time()
    
    def get_upload_rate(self):
        if self.get_upload_time() == 0:
            return 0
        return float(self.uploaded) / self.get_upload_time()

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

class _WriteWorkerProcesses(object):
    '''Process pool for WriteWorker.'''
    END_TIME = 'end_time'
    LOCAL_FILEPATH = 'local_filepath'
    REMOTE_FILEPATH = 'remote_filepath'
    def __init__(self, store, logger):
        ''':param store: An implementation of the Store interface.'''
        self.store = store
        self.logger = logger
        self.processes = []
        self.manager = multiprocessing.Manager()
    
    def get_worker(self, local_filepath, remote_filepath):
        ''':param local_filepath: The path of the local file.
        :param remote_filepath: The remote path to store the local file.
        :returns: a worker process from the pool.'''
        if not self.processes:
            interrupt_event = multiprocessing.Event()
            result_queue = multiprocessing.Queue()
            parameters = self.manager.dict()
            parameters[self.END_TIME] = 0
            parameters[self.LOCAL_FILEPATH] = local_filepath
            parameters[self.REMOTE_FILEPATH] = remote_filepath
            store = copy.deepcopy(self.store)
            p = multiprocessing.Process(target=self._run, args=(result_queue, interrupt_event, parameters, store))
            p.daemon = True
            p.start()
            p.start_time = time.time()
            p.parameters = parameters
            p.result_queue = result_queue
            p.interrupt_event = interrupt_event
            p.parameters = parameters
        else:           
            p = self.processes.pop(0)
            p.parameters[self.END_TIME] = 0
            p.parameters[self.LOCAL_FILEPATH] = local_filepath
            p.parameters[self.REMOTE_FILEPATH] = remote_filepath
            p.start_time = time.time()
        return p
    
    def recycle_worker(self, worker):
        '''Put the *worker* process back into the pool for reuse.'''
        if worker.is_alive():
            if not worker.result_queue.empty(): 
                worker.result_queue.get()
            self.processes.append(worker)            

    def _run(self, result_queue, interrupt_event, parameters, store):
        self.logger = db_logging_thread.make_logger_multiprocessingsave(self.logger)
        while True:
            while None in [parameters[self.LOCAL_FILEPATH], parameters[self.REMOTE_FILEPATH]]:
                # wait for new upload task
                time.sleep(0.1)
            local_filepath = parameters[self.LOCAL_FILEPATH]
            remote_filepath = parameters[self.REMOTE_FILEPATH]
            self.logger.debug("Start WriteWorker process %s to write %s", os.getpid(), remote_filepath)
            try:
                update_time = store.store_file(local_filepath, os.path.dirname(remote_filepath), os.path.basename(remote_filepath), interrupt_event)
                parameters[self.END_TIME] = time.time()
                if not update_time:
                    update_time = parameters[self.END_TIME]
                result_queue.put(update_time)
            except Exception, e:
                self.logger.exception("Error on storing %s in WriteWorker", remote_filepath)
                try:
                    pickle.loads(pickle.dumps(e))  # check if exception can be de/serialized
                    result_queue.put(e)
                except Exception:
                    self.logger.error("Error on serializing exception in WriteWorker: %s", repr(e))
                    result_queue.put(Exception(repr(e)))
            self.logger.debug("Finish WriteWorker process %s to write %s", os.getpid(), remote_filepath)
            
            parameters[self.LOCAL_FILEPATH], parameters[self.REMOTE_FILEPATH] = None, None
            
            
        

class WriteWorker(object):
    '''Uploads a single file in a separate process. 
    The start method is used to begin the upload. The method is_finished can be used to check
    if the worker is done. is_successful shows if it has been successful after it is done. If not successful, an error message can be retrieved with get_error.
    Other methods can be used to get upload statistics.'''
    pool = None
    def __init__(self, store, path, file, logger):
        ''':param store: Store instance that will be deepcopied and used in a newly created process to upload file
        :param path: path that is used in the store as a reference to the uploaded file
        :param file: fileobject with a name attribute; file.name needs to be a file on the local harddrive; the file is removed after the worker is finished
        :param logger: a multiprocessing logger 
        '''
        if not WriteWorker.pool:
            WriteWorker.pool = _WriteWorkerProcesses(store, logger)
        self.store = store
        self.path = path
        self._filename = file.name
        file.close()
        self._filesize = os.path.getsize(file.name)
        self.logger = logger
        self.start_time = 0
        self.end_time = 0
        self._update_time = None
        self.process = None
        self._finished = False
        self._pid = 0
        self._is_successful = False
        self._error = None 
        
    def is_sleeping(self):
        ''':returns: True iff the worker does nothing.'''
        # Give the process some time after starting
        if time.time() - self.start_time < 180:
            return False 
        try:
            p = psutil.Process(self._pid)
            return not self.is_finished() and p.status() == 'sleeping' and not p.connections()
        except NoSuchProcess, e:
            return False 

    def kill(self):
        '''Forcefully stop the upload process.'''
        os.system('kill -9 {0}'.format(self.process.pid)) 
        self._error = Exception("Forcefully terminated WriteWorker process %s to write %s", self.process.pid, self.path) 
        self.end_time = time.time()
        self._clean_up()

    def get_duration(self):
        """Get duration of upload in seconds"""
        if self.start_time == 0 or self.end_time == 0:
            raise RuntimeError, 'Cannot obtain duration: the upload did not yet end'
        return self.end_time - self.start_time
    
    def get_endtime(self):
        """Get the end time of the upload in seconds from the epoche"""
        if self.end_time == 0:
            raise RuntimeError, 'Cannot obtain end time: the upload did not yet end'
        return self.end_time
    
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
        self._check_result()
        return self._finished
    
    def get_error(self):
        self._check_result()
        return self._error
    
    def _check_result(self):
        if self._finished:
            return
        if not self.process.result_queue.empty(): 
            result = self.process.result_queue.get()
            self.end_time = self.process.parameters[_WriteWorkerProcesses.END_TIME]
            self._clean_up()
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
        if self._finished:
            return
        while not self.process.is_alive():
            time.sleep(1)
            self.logger.error("Trying to stop process that has not yet started.")
        self.interrupt_event.set()
        self.process.join(60)
        if not self.process.is_alive():
            self._error = Exception("Stopped WriteWorker process %s to write %s", self._pid, self.path)
        else:
            import os
            self.process.terminate()
            os.system('kill -9 {0}'.format(self.process.pid)) 

            self._error = Exception("Forcefully terminated WriteWorker process %s to write %s", self.process.pid, self.path) 
        self.process.terminate()
        self.end_time = time.time()
        self._clean_up()
        self.logger.debug("Stopped WriteWorker process %s to write %s", self._pid, self.path)
        
    def _clean_up(self):
        if os.path.exists(self._filename):
            os.remove(self._filename)
        self.store = None
        self.interrupt_event = None
        self._result_queue = None
        WriteWorker.pool.recycle_worker(self.process)
        self.process = None
        self._finished = True
    
    def start(self):
        if self._finished:
            return
        self.start_time = time.time()
        self.logger.debug("Create WriteWorker process to write %s", self.path)
        self.process = WriteWorker.pool.get_worker(self._filename, self.path)
        self._pid = self.process.pid

                    
class RemoveWorker(object):
    def __init__(self, store, path, is_dir, logger):
        store.delete(path, is_dir)
        self.store = copy.deepcopy(store)
        self._is_dir = is_dir
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
            self.store.delete(self.path, self._is_dir)
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
        self.process.daemon = True
        self._finished = False

    def _clean_up(self):
        self.store = None
        self._result_queue = None
        self.end_time = LeightWeightValue(self.end_time.value) 
        self.process = None
        self._finished = True
        
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
        return (self._finished or not self.process.is_alive()) #after successful operation, process is deleted
    
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
        self.process.join() #Remove terminate to allow for logging with multiprocessing
    
    def start(self):
        if self._finished:
            return
        self.start_time = time.time()
        self.process.start()

    def _check_result(self):
        if self._finished:
            return
        if not self._result_queue.empty(): # self._result_queue is empty after cleanup
            self._get_result()
    
    def _get_result(self):
        if self._finished:
            return
        result = self._result_queue.get()
        self._clean_up()
        if isinstance(result, str) or isinstance(result, unicode):
            self._is_successful = True
            self._temp_result = result
            self._filesize = len(result)
        else:
            self._is_successful = False
            self._error = result

    def _run(self, result_queue, end_time):
        self.logger = db_logging_thread.make_logger_multiprocessingsave(self.logger)
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
        
