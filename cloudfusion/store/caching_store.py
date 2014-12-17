'''
Created on Jun 10, 2013

@author: joe
'''
#TODO: implement reading slowly from cache, to reduce memory
from cloudfusion.util.persistent_lru_cache import PersistentLRUCache
from cloudfusion.util.file_decorator import *
from cloudfusion.store.store import *
from cloudfusion.store.store_sync_thread import StoreSyncThread
from cloudfusion.util.synchronize_proxy import SynchronizeProxy
import time
import random
import logging

ENABLE_PROFILING = 'enable_profiling'

class MultiprocessingCachingStore(Store):
    """Like CachingStore, but does not make guarantees as to the consistency of the wrapped store.
    Use of best effort strategy to synchronize the store.
    Employs multiple threads for increased throughput. Therefore, it can only use stores with a thread-safe put_file method.
    Unlike CachingStore, guarantees that write operations do not block for transfer, until the cache size limit is reached.
    Unlike CachingStore, guarantees that no write operations on the wrapped store are invoked until a cached item expires.
    """
    def __init__(self, store, cache_expiration_time=60, cache_size_in_mb=2000, cache_id=None, cache_dir='/tmp/cloudfusion/'):
        """
        :param store: the store whose access should be cached 
        :param cache_expiration_time: the time in seconds until any cache entry is expired
        :param cache_size_in_mb: Approximate limit of the cache in MB.
        :param cache_id: Serves as identifier for a persistent cache instance.
        :param cache_dir: Cache directory on local hard drive disk, default value is */tmp/cloudfusion*. """ 
        #prevent simultaneous access to store (synchronous use of __deepcopy__ by _store SyncThread and a different method): 
        self.store = SynchronizeProxy(store, private_methods_to_synchronize=['__deepcopy__'])
        if cache_id == None:
            cache_id = str(random.random()) 
        self.logger = logging.getLogger(self.get_logging_handler())
        self.logger.debug("creating CachingStore object")
        if cache_expiration_time < 240:
            self.logger.warning("Be aware of the synchronization issue https://github.com/joe42/CloudFusion/issues/16 \
                    or to avoid the issue set cache_expiration_time to more than 240 seconds.")
#        self.temp_file = tempfile.SpooledTemporaryFile()
        self.cache_expiration_time = cache_expiration_time
        self.time_of_last_flush = time.time()
        self.cache_dir = cache_dir[:-1] if cache_dir[-1:] == '/' else cache_dir # remove slash at the end
        cache = PersistentLRUCache(self.cache_dir+"/cachingstore_"+cache_id, cache_expiration_time, cache_size_in_mb)
        cache.set_resize_intervall(10)
        self.entries = SynchronizeProxy( cache ) #[shares_resource: write self.entries]
        self.sync_thread = StoreSyncThread(self.entries, self.store, self.logger)
        self.sync_thread.start()
    
    def set_configuration(self, config):
        self.store.set_configuration(config)
        if ENABLE_PROFILING in config:
            self.sync_thread.do_profiling = config[ENABLE_PROFILING]
    
    def get_configuration(self, config):
        return self.store.get_configuration()
    
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
                self.logger.debug("invalid cache entry: actual_modified_date > cached_modified_date of %s: %s > %s" % (path, actual_modified_date, cached_modified_date))
                return True
        else:
            return True
        return False
    
    def get_file(self, path_to_file):
        """ If the file was updated in the wrapped store, then its content in the cache will be updated if its entry is expired but not dirty.
        :returns: string -- the data of the file with the path *path_to_file*
        """
        #wait a little, so that storesyncthread gets lock as well
        #i could sleep here for one second for instance
        #or i calculate, how often read has run, and run tidy accordingly
        time.sleep(0.0001)
        is_in_progress = self.sync_thread.is_in_progress(path_to_file) #cannot obtain self.lock in is_in_progress if tidy runs, while tidy cannot proceed because of protect_cache lock
        with self.sync_thread.protect_cache_from_write_access: #do we really need this for storesync.get_file? yes, it could delete a cache entry while writing one while synchronizing with async read
            self.logger.debug("cached get_file %s", path_to_file)
            if not self.entries.exists(path_to_file):
                self.logger.debug("cached get_file from new entry")
                self._refresh_cache(path_to_file)
                return self.entries.get_value(path_to_file)
            if self.entries.is_expired(path_to_file) and not self.entries.is_dirty(path_to_file):
                if not is_in_progress: 
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
            ret = self.store.get_metadata(path)['modified']
        return ret
                
    def store_fileobject(self, fileobject, path):
        """ Stores a fileobject to the :class:`cloudfusion.util.cache.Cache` and if the existing fileobject has expired it is also written to the wrapped store.
        The cached file's updated and modified attributes will be reset to the current point of time.
        The cached file's dirty flag is set to False if the entry has expired and was hence written to the store. Otherwise it is set to True.
        
        :param fileobject: The file object with the method read() returning data as a string 
        :param path: The path where the file object's data should be stored, including the filename
        """
        self.logger.debug("cached storing %s", path)
        self.sync_thread.write_cache_entry(path, fileobject.read()) #[shares_resource: write self.entries]
        self.logger.debug("cached storing value %s...", self.entries.get_value(path)[:10]) 

    def delete(self, path, is_dir):#should be atomic
        self.logger.debug("delete %s", path)
        if self.store.exists(path):  
            self.sync_thread.delete(path, is_dir)
        self.sync_thread.delete_cache_entry(path) #[shares_resource: write self.entries]
          
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
    
    def duplicate(self, path_to_src, path_to_dest):  #TODO: move all cached entries
        with self.sync_thread.protect_cache_from_write_access:
            self.sync_thread.delete_cache_entry(path_to_dest) # delete possible locally cached entry at destination  #[shares_resource: write self.entries]
            local_dirty_entry_to_src_exists = self.entries.exists(path_to_src) and self.entries.is_dirty(path_to_src)
            source_is_in_store = not local_dirty_entry_to_src_exists
            if self.entries.exists(path_to_src):  
                self.logger.debug("cached storing duplicate %s to %s", path_to_src, path_to_dest)
                self.sync_thread.write_cache_entry(path_to_dest, self.entries.get_value(path_to_src)) #[shares_resource: write self.entries]
            else: #might be a directory
                self.sync_thread.sync()
            if source_is_in_store: 
                self.store.duplicate(path_to_src, path_to_dest)
        
    def move(self, path_to_src, path_to_dest): #TODO: move all cached entries
        with self.sync_thread.protect_cache_from_write_access:
            self.sync_thread.delete_cache_entry(path_to_dest) # delete possible locally cached entry at destination  #[shares_resource: write self.entries]
            local_dirty_entry_to_src_exists = self.entries.exists(path_to_src) and self.entries.is_dirty(path_to_src)
            source_is_in_store = not local_dirty_entry_to_src_exists 
            if self.entries.exists(path_to_src):  
                self.sync_thread.write_cache_entry(path_to_dest, self.entries.get_value(path_to_src)) #[shares_resource: write self.entries]
                self.sync_thread.delete_cache_entry(path_to_src) #[shares_resource: write self.entries]
            else: #might be a directory
                self.sync_thread.sync()                
            if source_is_in_store: 
                self.store.move(path_to_src, path_to_dest)
 
    def get_modified(self, path):
        if self.entries.exists(path):
            return self.entries.get_modified(path)
        return self.get_metadata(path)["modified"]
    
    def get_directory_listing(self, directory):
        #merge cached files and entries from store into set with unique entries
        store_listing = self.store.get_directory_listing(directory)
        cache_listing = []
        for path in self.entries.get_dirty_lru_entries(float("inf")):
            if os.path.dirname(path) == directory:
                cache_listing.append( path )
        ret = list(set( cache_listing + store_listing ))
        return ret
    
    def get_bytes(self, path):
        if self.entries.exists(path):
            return len(self.entries.peek(path))
        return self.get_metadata(path)['bytes']
    
    def exists(self, path):
        if self.entries.exists(path):
            return True
        try:
            self.get_metadata(path)
            return True
        except NoSuchFilesytemObjectError:
            return False
    
    def get_metadata(self, path):
        with self.sync_thread.protect_cache_from_write_access: # think about performance issues here, since this is called all the time
            metadata = {}
            if self.entries.exists(path):
                metadata['modified'] = self.entries.get_modified(path)
                metadata['bytes'] = len(self.entries.peek(path))
                metadata['is_dir'] = False
            else:
                metadata = self.store.get_metadata(path)
            return metadata

    def is_dir(self, path):
        if self.entries.exists(path):
            return False
        return self.get_metadata(path)["is_dir"]
            
    def get_logging_handler(self):
        return self.store.get_logging_handler()
    
    def reconnect(self):
        self.store.reconnect()
        
    def get_max_filesize(self):
        """Return maximum number of bytes per file"""
        return self.store.get_max_filesize()
