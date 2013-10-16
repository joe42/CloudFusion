from __future__ import division
from cloudfusion.store.store_worker import WriteWorker, ReadWorker, RemoveWorker
from threading import Thread, RLock
import time
from cloudfusion.store.store import StoreSpaceLimitError
from cloudfusion.store.transparent_store import ExceptionStats

class StoreStats(object):
    '''Statistics about workers. Resets statistics automatically after 100*1000 workers.'''
    def __init__(self):
        self.uploaded = 0
        self.downloaded = 0
        self.exceptions_log = {}
        self.write_workers = []
        self.read_workers = []
        
    def _log_exception(self, exception):
        name = repr(exception)
        if self.exceptions_log.has_key(name):
            e_stat = self.exceptions_log[name]
            e_stat.exception_list.append(exception)
            e_stat.lasttime = time.time()
            e_stat.count += 1
        else:
            e_stat = ExceptionStats(name, [exception], str(exception))
            self.exceptions_log[name] = e_stat
    
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
            self.write_workers.append(worker)
            self.uploaded += worker.get_filesize()
        if isinstance(worker, ReadWorker):
            if worker.get_error():
                self._log_exception(worker.get_error())
                return
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

class StoreSyncThread(object):
    """Synchronizes between cache and store"""
    def __init__(self, cache, store, logger, max_writer_threads=3):
        self.logger = logger
        self.stats = StoreStats()
        self.cache = cache
        self.store = store
        self.max_writer_threads = max_writer_threads
        self.lock = RLock()
        self.removers = []
        self.writers = []
        self.readers = []
        self._stop = False
        self.thread = None
        self.last_reconnect = time.time()
        #used for waiting when quota errors occur
        self.skip_starting_new_writers_for_next_x_cycles = 0
        self.logger.debug("initialized StoreSyncThread")
        
    
    def get_downloaded(self):
        """Get amount of data downloaded from a store in MB"""
        return self.stats.downloaded / 1000.0 / 1000
    
    def get_uploaded(self):
        """Get amount of data uploaded to a store in MB"""
        return self.stats.uploaded / 1000.0 / 1000
    
    def get_download_rate(self):
        """Get download rate in MB/s"""
        return self.stats.get_download_rate() / 1000.0 / 1000
    
    def get_upload_rate(self):
        """Get upload rate in MB/s"""
        return self.stats.get_upload_rate() / 1000.0 / 1000
    
    def get_exception_stats(self):
        return self.stats.exceptions_log
    
    def start(self):
        self._stop = False
        self.thread = Thread(target=self.run)
        self.thread.setDaemon(True)
        self.thread.start()
    
    def stop(self):
        self._stop = True
        
    def _remove_finished_writers(self):
        for writer in self.writers:
            if writer.is_finished():
                self.writers.remove(writer)
                self.stats.add_finished_worker(writer)
            if writer.is_successful() and self.cache.exists(writer.path): #stop() call in delete method might not have prevented successful write 
                self.cache.set_dirty(writer.path, False)
                if self.cache.get_modified(writer.path) < writer.get_updatetime():
                    self.cache.set_modified(writer.path, writer.get_updatetime())
                
    def _check_for_failed_writers(self):
        for writer in self.writers:
            if writer.is_finished():
                if writer.get_error():
                    if isinstance(writer.get_error(), StoreSpaceLimitError): #quota error? -> stop writers 
                        self.skip_starting_new_writers_for_next_x_cycles = 10
                    
    def _remove_finished_readers(self):
        for reader in self.readers:
            if reader.is_finished():
                self.readers.remove(reader)
            if reader.is_successful():
                self.cache.set_dirty(reader.path, False)
                content = reader.get_result() # block until read is done
                self.cache.refresh(reader.path, content, self.store._get_metadata(reader.path)['modified'])
                self.stats.add_finished_worker(reader)
                
    def _remove_successful_removers(self):
        for remover in self.removers:
            if remover.is_finished() and remover.is_successful():
                self.removers.remove(remover)
                    
    def _restart_unsuccessful_removers(self):
        for remover in self.removers:
            if remover.is_finished() and not remover.is_successful():
                remover.start()
                
    def _is_in_progress(self, path):
        for writer in self.writers:
            if path == writer.path:
                return True
        for remover in self.removers:
            if path == remover.path:
                return True
        return False
    
    def _get_writer(self, path):
        for writer in self.writers:
            if path == writer.path:
                return writer
        return None
    
    def _get_reader(self, path):
        for reader in self.readers:
            if path == reader.path:
                return reader
        return None

    def run(self): 
        #TODO: check if the cached entries have changed remotely (delta request) and update asynchronously
        #TODO: check if entries being transferred have changed and stop transfer
        while not self._stop:
            self.logger.debug("StoreSyncThread run")
            time.sleep( 60 )
            self._reconnect()
            self.tidy_up()
            if self.skip_starting_new_writers_for_next_x_cycles > 0:
                self.skip_starting_new_writers_for_next_x_cycles -= 1
                continue
            self.enqueue_lru_entries()
    
    def _reconnect(self):
        if time.time() > self.last_reconnect + 60*60: #reconnect after 1h
            self.store.reconnect()
            self.last_reconnect = time.time()
    
    def tidy_up(self):
        """Remove finished workers and restart unsuccessful delete jobs."""
        with self.lock:
            self._check_for_failed_writers()
            self._remove_finished_writers()
            self._remove_finished_readers()
            self._remove_successful_removers()
            self._restart_unsuccessful_removers()
    
    def enqueue_lru_entries(self): 
        """Start new writer jobs with expired least recently used cache entries."""
        #TODO: check for user quota error and pause or do exponential backoff
        #TODO: check for internet connection availability and pause or do exponential backoff
        with self.lock:
            dirty_entry_keys = self.cache.get_dirty_lru_entries(self.max_writer_threads)
            for path in dirty_entry_keys:
                if len(self.writers) >= self.max_writer_threads:
                    break
                if not self.cache.is_expired(path): 
                    break
                if self._is_in_progress(path):
                    continue
                file = self.cache.peek_file(path)
                new_worker = WriteWorker(self.store, path, file, self.logger)
                new_worker.start()
                self.writers.append(new_worker)
    
    
    def enqueue_dirty_entries(self): 
        """Start new writer jobs with dirty cache entries."""
        with self.lock:
            dirty_entry_keys = self.cache.get_dirty_lru_entries(self.max_writer_threads)
            for path in dirty_entry_keys:
                if len(self.writers) >= self.max_writer_threads:
                    break
                if self._is_in_progress(path):
                    continue
                file = self.cache.peek_file(path)
                new_worker = WriteWorker(self.store, path, file, self.logger)
                new_worker.start()
                self.writers.append(new_worker)
    
    def sync(self):
        with self.lock:
            self.logger.debug("StoreSyncThread sync")
            while True:
                time.sleep(3)
                self.tidy_up()
                self.enqueue_dirty_entries()
                if not self.cache.get_dirty_lru_entries(1):
                    return
            self.logger.debug("StoreSyncThread endsync")
        
    
    def delete(self, path):
        with self.lock:
            if self._get_writer(path):
                writer = self._get_writer(path)
                writer.stop()
            if self._get_reader(path):
                reader = self._get_reader(path)
                reader.stop()
            remover = RemoveWorker(self.store, path, self.logger)
            remover.start()
            self.removers.append(remover)
            
    def read(self, path):
        with self.lock:
            if not self._get_reader(path): #ongoing read operation 
                reader = ReadWorker(self.store, path, self.logger)
                reader.start()
                self.readers.append(reader)
            
    def blocking_read(self, path):
        with self.lock:
            self.read(path)
            reader = self._get_reader(path)
            #assert modified < self.cache.get_modified(path), "file in cache should never be more recent, since before files are written to cache, they are read from the store" 
            #disk_entry_is_newer = modified > self.entries[key].modified
            content = reader.get_result() # block until read is done
            self.stats.add_finished_worker(reader)
            if reader.get_error():
                raise reader.get_error()
            self.cache.refresh(path, content, self.store._get_metadata(path)['modified'])
            self.readers.remove(reader)
            