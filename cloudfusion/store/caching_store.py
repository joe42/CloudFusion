'''
Created on Jun 10, 2013

@author: joe
'''
#TODO: implement reading slowly from cache, to reduce memory
from cloudfusion.util.persistent_lru_cache import PersistentLRUCache
from cloudfusion.store.dropbox.file_decorator import *
from cloudfusion.store.store import *
from cloudfusion.util.synchronize_proxy import SynchronizeProxy
import os
import time
import random
import logging
from threading import Thread, RLock
from cloudfusion.mylogging.nullhandler import NullHandler
import multiprocessing 
import copy

logging.getLogger().addHandler(NullHandler())

class WriteWorker(object):
    def __init__(self, store, path, file, logger):
        self.store = copy.deepcopy(store)
        self.path = path
        self._file = file
        self.logger = logger
        self.logger.debug("writing "+path)
        self._result_queue = multiprocessing.Queue()
        self.process = multiprocessing.Process(target=self._run, args=(self._result_queue,))
        self.process.daemon = True
        self._is_successful = False
        self._error = None 
    
    def is_finished(self):
        return not self.process.is_alive() 
    
    def get_error(self):
        return self._error
    
    def is_successful(self):
        try:
            if not self._result_queue.empty():
                result = self._result_queue.get()
                if result == True:
                    self._is_successful = True
                else:
                    self._is_successful = False
                    self._error = result
        except: #Error thrown by broken queue
            pass
        return self._is_successful
    
    def stop(self):
        self.process.terminate()
    
    def start(self):
        self.process.start()
    
    def _run(self,result_queue):
        self.logger.debug("Start WriteWorker process %s to write %s" % (os.getpid(), self.path))
        try:
            self.store.store_fileobject(self._file, self.path)
            result_queue.put(True)
        except Exception, e:
            result_queue.put(e)
            self.logger.debug("Error on storing %s in WriteWorker: %s" % (self.path, e))
        self.logger.debug("Finish WriteWorker process %s to write %s" % (os.getpid(), self.path))
            
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
            self.logger.debug("Error on removing %s in RemoveWorker: %s" % (self.path, e))

class ReadWorker(object):
    def __init__(self, store, path, logger):
        self.store = copy.deepcopy(store)
        self.path = path
        self.logger = logger
        self._result_queue = multiprocessing.Queue()
        self.process = multiprocessing.Process(target=self._run, args=(self._result_queue,))
    
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
        self.process.start()
    
    def _run(self, result_queue):
        try:
            content = self.store.get_file(self.path)
            result_queue.put(content)
        except Exception, e:
            self.logger.debug("Error on reading %s in ReadWorker: %s" % (self.path, e))
        

class _StoreSyncThread(object):
    """Synchronizes between cache and store"""
    def __init__(self, cache, store, logger, max_writer_threads=3):
        self.logger = logger
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
        self.logger.debug("initialized StoreSyncThread")
    
    def start(self):
        self._stop = False
        self.thread = Thread(target=self.run)
        self.thread.start()
    
    def stop(self):
        self._stop = True
        
    def _remove_finished_writers(self):
        for writer in self.writers:
            if writer.is_finished():
                self.writers.remove(writer)
            if writer.is_successful() and self.cache.exists(writer.path): #stop() call in delete method might not have prevented successful write 
                self.cache.set_dirty(writer.path, False)
                
    def _check_for_failed_writers(self):
        for writer in self.writers:
            if writer.is_finished():
                if writer.get_error():
                    #TODO:is quota error -< stop writers
                    pass
                    
    def _remove_finished_readers(self):
        for reader in self.readers:
            if reader.is_finished():
                self.readers.remove(reader)
            if reader.is_successful():
                self.cache.set_dirty(reader.path, False)
                content = reader.get_result() # block until read is done
                self.cache.refresh(reader.path, content, self.store._get_metadata(reader.path)['modified'])
                
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
            self.enqueue_lru_entries()
    
    def _reconnect(self):
        if time.time() > self.last_reconnect + 60*60: #reconnect after 1h
            self.store.reconnect()
            self.last_reconnect = time.time()
    
    def tidy_up(self):
        """Remove finished workers and restart unsuccessful delete jobs."""
        with self.lock:
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
                content = self.cache.peek_file(path)
                new_worker = WriteWorker(self.store, path, content, self.logger)
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
                content = self.cache.peek_file(path)
                new_worker = WriteWorker(self.store, path, content, self.logger)
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
            self.cache.refresh(path, content, self.store._get_metadata(path)['modified'])
            self.readers.remove(reader)
            


class MultiprocessingCachingStore(Store):
    """Like CachingStore, but does not make guarantees as to the consistency of the wrapped store.
    Use of best effort strategy to synchronize the store.
    Employs multiple threads for increased throughput. Therefore, it can only use stores with a thread-safe put_file method.
    Unlike CachingStore, guarantees that write operations do not block for transfer, until the cache size limit is reached.
    Unlike CachingStore, guarantees that write operations on the wrapped store are invoked until a cached item expires.
    """
    def __init__(self, store, cache_expiration_time=60, cache_size_in_mb=2000, cache_id=str(random.random())):
        """
        :param store: the store whose access should be cached 
        :param cache_expiration_time: the time in seconds until any cache entry is expired
        :param cache_size_in_mb: Approximate limit of the cache in MB.
        :param cache_id: Serves as identifier for a persistent cache instance. """ 
        #prevent simultaneous access to store (synchronous use of __deepcopy__ by _store SyncThread and a different method): 
        self.store = SynchronizeProxy(store, private_methods_to_synchronize=['_get_metadata', '__deepcopy__']) 
        self.logger = logging.getLogger(self.get_logging_handler())
        self.logger.debug("creating CachingStore object")
#        self.temp_file = tempfile.SpooledTemporaryFile()
        self.cache_expiration_time = cache_expiration_time
        self.time_of_last_flush = time.time()
        self.entries = SynchronizeProxy( PersistentLRUCache("/tmp/cloudfusion/cachingstore_"+cache_id, cache_expiration_time, cache_size_in_mb) )
        self.sync_thread = _StoreSyncThread(self.entries, self.store, self.logger)
        self.sync_thread.start()
    
    def get_cache_expiration_time(self):
        """:returns: the time in seconds until any cache entry is expired"""
        return self.cache_expiration_time
    
    def _is_valid_path(self, path):
        return self.store._is_valid_path(path)
    
    def _raise_error_if_invalid_path(self, path):
        self.store._raise_error_if_invalid_path(path)
        
    def get_name(self):
        return self.store.get_name()
    
    def _refresh_cache(self, path_to_file):
        """ Reloads the locally cached file *path_to_file* from the wrapped store, if the wrapped store version is newer.
        The cached file's last updated time is set to the current point of time.
        Makes a new cache entry if it does not exist yet.
        If the file was changed in the wrapped store after the cached file's modified time, the cached file...
        
        * is updated with the contents of the file in the wrapped store
        * is set to not dirty
        * gets the modified time stamp of the file of the wrapped stores
        :raises: NoSuchFilesytemObjectError if file does not exist in wrapped store.
        TODO: exception raising does not work and needs to be implemented 
        """
        cached_version_is_invalid = self.is_cached_version_invalid(path_to_file)
        if cached_version_is_invalid:
            self.sync_thread.blocking_read(path_to_file)
            self.logger.debug("refreshed cached fileobject from store")
        
    def is_cached_version_invalid(self, path):
        """:returns: True if the stores version is newer than the cached entry or does not exist and False otherwise."""
        if self.entries.exists(path):
            actual_modified_date = self._get_actual_modified_date(path)
            cached_modified_date = self.entries.get_modified(path)
            if actual_modified_date > cached_modified_date:
                return True
        else:
            return True
        return False
    
    def get_file(self, path_to_file):
        """ :returns: string -- the data of the file with the path *path_to_file*
        If the file was updated in the wrapped store, then its content in the cache will be updated if its entry is expired but not dirty. 
        """
        self.logger.debug("cached get_file %s" % path_to_file)
        if not self.entries.exists(path_to_file):
            self._refresh_cache(path_to_file)
            self.logger.debug("cached get_file from new entry")
            return self.entries.get_value(path_to_file)
        if self.entries.is_expired(path_to_file) and not self.entries.is_dirty(path_to_file):
            self.logger.debug("cached get_file update from store if newer")
            self._refresh_cache(path_to_file)
        return self.entries.get_value(path_to_file) #not expired->from entries
    
    def store_file(self, path_to_file, dest_dir="/", remote_file_name = None):
        if dest_dir == "/":
            dest_dir = ""
        fileobject = open(path_to_file)
        if not remote_file_name:
            remote_file_name = os.path.basename(path_to_file)
        self.store_fileobject(fileobject, dest_dir + "/" + remote_file_name)
    
    def _get_actual_modified_date(self, path):
        ret = 0
        if self.store.exists(path):
            ret = self.store._get_metadata(path)['modified']
        return ret
                
    def store_fileobject(self, fileobject, path):
        """ Stores a fileobject to the :class:`cloudfusion.util.cache.Cache` and if the existing fileobject has expired it is also written to the wrapped store.
        The cached file's updated and modified attributes will be reset to the current point of time.
        The cached file's dirty flag is set to False if the entry has expired and was hence written to the store. Otherwise it is set to True.
        
        :param fileobject: The file object with the method read() returning data as a string 
        :param path: The path where the file object's data should be stored, including the filename
        """
        self.logger.debug("cached storing %s" % path)
        self.entries.write(path, fileobject.read())
        self.logger.debug("cached storing value %s..." %self.entries.get_value(path)[:10]) 

    def delete(self, path):#should be atomic
        self.logger.debug("delete %s" % path)
        if self.store.exists(path):  
            self.sync_thread.delete(path)
        self.entries.delete(path)
          
    def account_info(self):
        return self.store.account_info()
    
    def get_free_space(self):
        free_bytes = self.store.get_free_space() - self.entries.get_size_of_dirty_data()
        if free_bytes < 0:
            free_bytes = 0
        return free_bytes
    
    def get_overall_space(self):
        return self.store.get_overall_space()
    
    def get_used_space(self):
        return self.store.get_used_space() + self.entries.get_size_of_dirty_data()

    def create_directory(self, directory):
        return self.store.create_directory(directory)
    
    def duplicate(self, path_to_src, path_to_dest): # TODO only for files? # handle similarly to move
        self.entries.delete(path_to_dest) # delete possible locally cached entry at destination 
        local_dirty_entry_to_src_exists = self.entries.exists(path_to_src) and self.entries.is_dirty(path_to_src)
        source_is_in_store = not local_dirty_entry_to_src_exists
        if self.entries.exists(path_to_src):  
            self.logger.debug("cached storing duplicate %s to %s" % (path_to_src, path_to_dest))
            self.entries.write(path_to_dest, self.entries.get_value(path_to_src))
        if source_is_in_store: 
            self.store.duplicate(path_to_src, path_to_dest)
        
    def move(self, path_to_src, path_to_dest):
        self.entries.delete(path_to_dest) # delete possible locally cached entry at destination 
        local_dirty_entry_to_src_exists = self.entries.exists(path_to_src) and self.entries.is_dirty(path_to_src)
        source_is_in_store = not local_dirty_entry_to_src_exists 
        if self.entries.exists(path_to_src):  
            self.entries.write(path_to_dest, self.entries.get_value(path_to_src))
            self.entries.delete(path_to_src)
        if source_is_in_store: 
            self.store.move(path_to_src, path_to_dest)
 
    def get_modified(self, path):
        if self.entries.exists(path):
            return self.entries.get_modified(path)
        return self._get_metadata(path)["modified"]
    
    def get_directory_listing(self, directory):
        #merge cached files and entries from store into set with unique entries
        store_listing = self.store.get_directory_listing(directory)
        cache_listing = []
        for path in self.entries.get_dirty_lru_entries(9999):
            if os.path.dirname(path) == directory:
                cache_listing.append( path )
        ret = list(set( cache_listing + store_listing ))
        return ret
    
    def get_bytes(self, path):
        if self.entries.exists(path):
            return len(self.entries.peek(path))
        return self._get_metadata(path)['bytes']
    
    def exists(self, path):
        if self.entries.exists(path):
            return True
        try:
            self._get_metadata(path)
            return True
        except NoSuchFilesytemObjectError:
            return False
    
    def _get_metadata(self, path):
        metadata = {}
        if self.entries.exists(path):
            metadata['modified'] = self.entries.get_modified(path)
            metadata['bytes'] = len(self.entries.peek(path))
            metadata['is_dir'] = False
        else:
            metadata = self.store._get_metadata(path)
        return metadata

    def is_dir(self, path):
        if self.entries.exists(path):
            return False
        return self._get_metadata(path)["is_dir"]
    
    def flush(self):
        self.logger.debug("flushing")
        self.store.flush()
            
    def get_logging_handler(self):
        return self.store.get_logging_handler()
    
    def reconnect(self):
        self.store.reconnect()
        
    def get_max_filesize(self):
        """Return maximum number of bytes per file"""
        return self.store.get_max_filesize()
