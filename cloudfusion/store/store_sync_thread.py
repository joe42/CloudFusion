from __future__ import division
from cloudfusion.store.store_worker import WriteWorker, ReadWorker, RemoveWorker, WorkerStats
from threading import Thread, RLock
import time
from cloudfusion.store.store import StoreSpaceLimitError, StoreAccessError, NoSuchFilesytemObjectError,\
    StoreAutorizationError
import os
from profilehooks import profile

class StoreSyncThread(object):
    """Synchronizes between cache and store"""
    def __init__(self, cache, store, logger, max_writer_threads=30):
        '''Max. throughput for files < 1MB is max_writer_threads * 100KB per second.
        :param max_writer_threads: Max. number of writer threads to use.
        '''
        self.logger = logger
        self.stats = WorkerStats()
        self.cache = cache
        self.store = store
        self.max_writer_threads = max_writer_threads
        self.WRITE_TIMELIMIT = 60*60*2 #2h
        self.lock = RLock()
        self.protect_cache_from_write_access = RLock() #could also be normal lock
        self.oldest_modified_date = {} #keep track of modified date of a cache entry when it is first enqueued for upload. Their contents might change during upload.
        self.removers = []
        self.writers = []
        self.readers = []
        self._stop = False
        self.thread = None
        self.last_reconnect = time.time()
        self._heartbeat = time.time()
        #used for waiting when quota errors occur
        self.skip_starting_new_writers_for_next_x_cycles = 0
        self._finished_writers_of_last_round = 0
        self.do_profiling = False
        self.logger.info("initialized StoreSyncThread")
    
    def _get_max_threads(self, size_in_mb):
        '''Tries to increase the number of threads so that only 25% of the threads finish
        in every round.'''
        if size_in_mb <= 0.1:
            return self.max_writer_threads
        if size_in_mb > 5: # 25 MBps
            return 5 
        ret = self._finished_writers_of_last_round * 4
        if ret < 5:
            ret = 5
        if ret > self.max_writer_threads:
            ret = self.max_writer_threads
        return ret
    
    def restart(self):
        self.stop()
        self.thread.join(60*5)
        #stop write- and readworkers
        for reader in self.readers:
            reader.stop()
        for remover in self.removers:
            remover.stop()
        self.start()
        
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
        writers_to_be_removed = []
        for writer in self.writers:
            if writer.is_finished():
                writers_to_be_removed.append(writer)
                self.stats.add_finished_worker(writer)
                with self.protect_cache_from_write_access: #otherwise, fuse thread could delete current cache entry
                    if writer.is_successful() and self.cache.exists(writer.path): #stop() call in delete method might not have prevented successful write
                        modified_during_upload =  self.cache.get_modified(writer.path) > self.oldest_modified_date[writer.path] #two writers with the same path?
                        if not modified_during_upload: #actual modified date is >= oldest modified date
                            self.set_dirty_cache_entry(writer.path, False) # set_dirty might delete item, if cache limit is reached #[shares_resource: write self.entries]
                            if self.cache.exists(writer.path) and self.cache.get_modified(writer.path) < writer.get_updatetime(): 
                                self.set_modified_cache_entry(writer.path, writer.get_updatetime()) #[shares_resource: write self.entries] #FIXME: stops here
                del self.oldest_modified_date[writer.path]
        self._finished_writers_of_last_round = len(writers_to_be_removed)
        for writer in writers_to_be_removed:
            self.writers.remove(writer)
    
    def _remove_slow_writers(self):
        for writer in self.writers:
            if not writer.is_finished(): 
                try:
                    writer_run_too_long = writer.get_starttime() < time.time() - self.WRITE_TIMELIMIT
                    if writer_run_too_long:
                        writer.stop()
                        self._heartbeat = time.time()
                        self.logger.exception('Terminated slow writer after 2h.')
                except RuntimeError, writer_has_not_yet_started:
                    self.logger.exception('Trying to remove slow writer trying to write %s failed.'%writer.path)
                
    def _check_for_failed_writers(self):
        for writer in self.writers:
            if writer.is_finished():
                if writer.get_error():
                    if isinstance(writer.get_error(), StoreSpaceLimitError): #quota error? -> stop writers 
                        self.skip_starting_new_writers_for_next_x_cycles = 4*30 #4 means one minute
                    
    def _remove_finished_readers(self):
        readers_to_be_removed = []
        for reader in self.readers:
            if reader.is_finished():
                readers_to_be_removed.append(reader)
            if reader.is_successful():
                content = reader.get_result() # block until read is done
                self.refresh_cache_entry(reader.path, content, self.store.get_metadata(reader.path)['modified']) #[shares_resource: write self.entries]
                self.stats.add_finished_worker(reader)
        for reader in readers_to_be_removed:
            self.readers.remove(reader)
                
    def _remove_successful_removers(self):
        removers_to_be_removed = []
        for remover in self.removers:
            if remover.is_finished() and remover.is_successful():
                removers_to_be_removed.append(remover)
        for remover in removers_to_be_removed:
            self.removers.remove(remover)
                    
    def _restart_unsuccessful_removers(self):
        for remover in self.removers:
            if remover.is_finished() and not remover.is_successful():
                remover.start()
                
    def is_in_progress(self, path):
        ''':returns: True iff *path* is currently uploaded or being removed'''
        with self.lock:
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
    
    def last_heartbeat(self):
        ''''Get time since last heartbeat in seconds.'''
        last_heartbeat = self._heartbeat
        return time.time()-last_heartbeat

    def __sleep(self, seconds):
        '''Sleep until *seconds* have passed since last call'''
        if not hasattr(self.__sleep.im_func, 'last_call'):
            self.__sleep.im_func.last_call = time.time()
        last_call = self.__sleep.im_func.last_call
        time_since_last_call = time.time() - last_call
        time_to_sleep_in_s = seconds - time_since_last_call
        if time_to_sleep_in_s > 0:
            time.sleep( time_to_sleep_in_s )
        self.__sleep.im_func.last_call = time.time()

    def _get_time_to_sleep(self):
        if self._finished_writers_of_last_round > 1:
            return 0.5
        if self._finished_writers_of_last_round > 0:
            return 1
        if len(self.writers) > 3:
            return 1
        if len(self.writers) > 0:
            return 15
        return 60
        
    def run(self): 
        #TODO: check if the cached entries have changed remotely (delta request) and update asynchronously
        #TODO: check if entries being transferred have changed and stop transfer
        
        while not self._stop:
            if self.do_profiling:
                self._profiled_run()
            else:
                self._run()
            
    @profile(filename='/tmp/cloudfusion_profile_store_sync_thread')
    def _profiled_run(self):
        while not self._stop and self.do_profiling:
            self.logger.debug("StoreSyncThread profiling run")
            self._heartbeat = time.time()
            self.__sleep( self._get_time_to_sleep() )
            self._reconnect()
            self.tidy_up()
            if self.skip_starting_new_writers_for_next_x_cycles > 0:
                self.skip_starting_new_writers_for_next_x_cycles -= 1
                continue
            self.enqueue_lru_entries()
            
    def _run(self):
        self.logger.debug("StoreSyncThread run")
        self._heartbeat = time.time()
        self.__sleep( self._get_time_to_sleep() )
        self._reconnect()
        self.tidy_up()
        if self.skip_starting_new_writers_for_next_x_cycles > 0:
            self.skip_starting_new_writers_for_next_x_cycles -= 1
            return
        self.enqueue_lru_entries()
    
    def _reconnect(self):
        if time.time() > self.last_reconnect + 60*60: #reconnect after 1h
            with self.lock:
                self.store.reconnect()
            self.last_reconnect = time.time()
    
    def tidy_up(self):
        """Remove finished workers and restart unsuccessful delete jobs."""
        with self.lock:
            self._check_for_failed_writers()
            self._remove_finished_writers()
            self._remove_slow_writers()
            self._remove_finished_readers()
            self._remove_successful_removers()
            self._restart_unsuccessful_removers()
    
    def enqueue_lru_entries(self): 
        """Start new writer jobs with expired least recently used cache entries."""
        #TODO: check for user quota error and pause or do exponential backoff
        #TODO: check for internet connection availability and pause or do exponential backoff
        #Entries can be deleted during this method!!!
        #TODO: only access entries through store_sync_thread methods synchronized with self.lock
        self._acquire_two_locks() #otherwise, fuse thread could delete current cache entry
        dirty_entry_keys = self.cache.get_dirty_lru_entries(self.max_writer_threads)##KeyError: '########################## ######## list_tail ###### #############################' lru_cache.py return self.entries[self.entries[LISTTAIL]] if self.entries[LISTTAIL] else None
        for path in dirty_entry_keys:
            if not self.cache.is_expired(path): ##KeyError: '/fstest.7548/d010/66334873' cache.py return time.time() > self.entries[key].updated + self.expire
                break
            if self.is_in_progress(path):
                continue
            file = self.cache.peek_file(path)
            size_in_mb = self.__get_file_size_in_mb(file)
            if len(self.writers) >= self._get_max_threads(size_in_mb):
                break
            self.oldest_modified_date[path] = self.cache.get_modified(path) #might change during upload, if new file contents is written to the cache entry
            new_worker = WriteWorker(self.store, path, file, self.logger)
            new_worker.start()
            self.writers.append(new_worker)
        self._release_two_locks()
    
    
    def enqueue_dirty_entries(self): 
        """Start new writer jobs with dirty cache entries."""
        self._acquire_two_locks() #otherwise, fuse thread could delete current cache entry
        dirty_entry_keys = self.cache.get_dirty_lru_entries(self.max_writer_threads)
        for path in dirty_entry_keys:
            if self.is_in_progress(path):
                continue
            file = self.cache.peek_file(path)
            size_in_mb = self.__get_file_size_in_mb(file)
            if len(self.writers) >= self._get_max_threads(size_in_mb):
                break
            self.oldest_modified_date[path] = self.cache.get_modified(path) #might change during upload, if new file contents is written to the cache entry
            new_worker = WriteWorker(self.store, path, file, self.logger)
            new_worker.start()
            self.writers.append(new_worker)
        self._release_two_locks()
    
    def __get_file_size_in_mb(self, fileobject):
        fileobject.seek(0, os.SEEK_END)
        size_in_mb = fileobject.tell() / 1000.0 / 1000  
        fileobject.seek(0)
        return size_in_mb
    
    def sync(self):
        with self.lock: # does not block writing..
            self.logger.info("StoreSyncThread sync")
            while True:
                time.sleep(1)
                self.tidy_up()
                self.enqueue_dirty_entries()
                if not self.cache.get_dirty_lru_entries(1):  
                    return
            self.logger.info("StoreSyncThread endsync")
        
    
    def delete(self, path, is_dir):
        with self.lock:
            if self._get_writer(path):
                writer = self._get_writer(path)
                writer.stop()
            if self._get_reader(path):
                reader = self._get_reader(path)
                reader.stop()
            remover = RemoveWorker(self.store, path, is_dir, self.logger)
            remover.start()
            
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
                err = reader.get_error()
                if not err in [StoreAccessError, NoSuchFilesytemObjectError, StoreAutorizationError]:
                    self.readers.remove(reader)
                    err = StoreAccessError(str(err),0)
                raise err
            self.refresh_cache_entry(path, content, self.store.get_metadata(path)['modified']) #[shares_resource: write self.entries]
            self.readers.remove(reader)
            
    def delete_cache_entry(self, path):
        with self.protect_cache_from_write_access:
            self.cache.delete(path)
            
    def _acquire_two_locks(self):
        while self.protect_cache_from_write_access.acquire(True) and not self.lock.acquire(False): #acquire(False) returns False if it cannot acquire the lock
            self.protect_cache_from_write_access.release()
            time.sleep(0.0001) #give other threads a chance to get both locks

    def _release_two_locks(self):
        self.lock.release()
        self.protect_cache_from_write_access.release()
    
    #these are called from this thread while multiprocessingstore instance operates.., but should only be called in between its methods
    #how can we accomplish this? -> second lock
    
    def write_cache_entry(self, path, contents):
        with self.protect_cache_from_write_access:
            self.cache.write(path, contents)
        
    def refresh_cache_entry(self, path, contents, modified):
        with self.protect_cache_from_write_access:
            self.cache.refresh(path, contents, modified)
            
    def set_dirty_cache_entry(self, path, is_dirty): #may be called by this class
        with self.protect_cache_from_write_access:
            #give it some time until deletion:
            self.cache.set_dirty(path, is_dirty)
    
    def set_modified_cache_entry(self, path, updatetime): #may be called by this class 
        with self.protect_cache_from_write_access:
            self.cache.set_modified(path, updatetime)
            
