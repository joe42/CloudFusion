from cloudfusion.store.store_sync_thread import StoreSyncThread
from cloudfusion.util.persistent_lru_cache import PersistentLRUCache
from cloudfusion.util.file_decorator import *
from cloudfusion.store.store import *
from cloudfusion.store.chunk_store_sync_thread import ChunkStoreSyncThread, get_parent_dir
from cloudfusion.util.synchronize_proxy import SynchronizeProxy
import time
import random
import logging

class ChunkMultiprocessingCachingStore(Store):
    '''
    Puts small files into an archive, to upload and download them together. Garbage collects archives with stale contents.
    Employs multiple threads for increased throughput. Therefore, it can only use stores with a thread-safe put_file method.
    Write operations do not block for transfer, until the cache size limit is reached.
    No write operations on the wrapped store are invoked until a cached item expires.
    '''
    def __init__(self, store, cache_expiration_time=60, cache_size_in_mb=2000, cache_id=None, max_archive_size_in_mb = 4, cache_dir='/tmp/cloudfusion'):
        """
        :param store: the store whose access should be cached 
        :param max_archive_size_in_mb: the maximum size of an archive 
        :param cache_expiration_time: the time in seconds until any cache entry is expired
        :param cache_size_in_mb: Approximate (soft) limit of the cache in MB.
        :param hard_cache_size_limit_in_mb: Hard limit of the cache in MB, exceeding this limit should slow down write operations.
        :param cache_id: Serves as identifier for a persistent cache instance. 
        :param cache_dir: Cache directory on local hard drive disk, default value is */tmp/cloudfusion*.""" 
        #prevent simultaneous access to store (synchronous use of __deepcopy__ by _store SyncThread and a different method): 
        self.store = SynchronizeProxy(store, private_methods_to_synchronize=['__deepcopy__'])
        self.max_archive_size_in_mb = max_archive_size_in_mb
        if cache_id == None:
            cache_id = str(random.random()) 
        self.logger = logging.getLogger(self.get_logging_handler())
        self.logger.debug("creating ChunkTransparentMultiprocessingCachingStore object")
        if cache_expiration_time < 240:
            self.logger.warning("Be aware of the synchronization issue https://github.com/joe42/CloudFusion/issues/16 \
                    or to avoid the issue set cache_expiration_time to more than 240 seconds.")
        self.cache_expiration_time = cache_expiration_time
        self.time_of_last_flush = time.time()
        self.cache_dir = cache_dir[:-1] if cache_dir[-1:] == '/' else cache_dir # remove slash at the end
        temp_dir = self.cache_dir+"/cachingstore_"+cache_id
        cache = PersistentLRUCache(temp_dir, cache_expiration_time, cache_size_in_mb)
        cache.set_resize_intervall(10)
        self.entries = SynchronizeProxy( cache ) #[shares_resource: write self.entries]
        self.sync_thread = ChunkStoreSyncThread(self.entries, self.store, temp_dir, self.logger)
        self.sync_thread.start()
        
    
    def set_configuration(self, config):
        self.store.set_configuration(config)
    
    def get_configuration(self, config):
        return self.store.get_configuration()
    
    def get_max_archive_size(self):
        """:returns: the maximum size of an archive in MB"""
        return self.max_archive_size_in_mb
    
    def set_max_archive_size(self, max_archive_size_in_mb):
        """Set the maximum size of an archive in MB"""
        self.max_archive_size_in_mb = max_archive_size_in_mb
    
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
        """ Reloads the locally cached file *path_to_file* from the wrapped store.
        Currently only retrieves files from store if they are not cached.
        #TODO: implement file version resolution over different sessions.  
        #:raises: NoSuchFilesytemObjectError if file does not exist in wrapped store.
        """
        cached_version_is_invalid = self.sync_thread.is_cached_version_invalid(path_to_file)
        if cached_version_is_invalid:
            self.sync_thread.blocking_read(path_to_file)
            self.logger.debug("refreshed cached fileobject from store")
        
    
    
    def get_file(self, path_to_file):
        """ If the file was updated in the wrapped store, then its content in the cache will be updated if its entry is expired but not dirty.
        :returns: string -- the data of the file with the path *path_to_file*
        """
        #wait a little, so that storesyncthread gets lock as well
        #i could sleep here for one second for instance
        #or i calculate, how often read has run, and run tidy accordingly
        time.sleep(0.0001)
        with self.sync_thread.protect_cache_from_write_access: #do we really need this for storesync.get_file? yes, it could delete a cache entry while writing one while synchronizing with async read
            self.logger.debug("cached get_file %s", path_to_file)
            if not self.entries.exists(path_to_file):
                self.logger.debug("cached get_file from new entry")
                self._refresh_cache(path_to_file)
            return self.entries.get_value(path_to_file) #not expired->from entries
    
    def store_file(self, path_to_file, dest_dir="/", remote_file_name = None):
        if dest_dir == "/":
            dest_dir = ""
        fileobject = open(path_to_file)
        if not remote_file_name:
            remote_file_name = os.path.basename(path_to_file)
        self.store_fileobject(fileobject, dest_dir + "/" + remote_file_name)
                
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
        if is_dir:
            self.store.delete(path, is_dir)
        else:
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
    
    
    def duplicate(self, path_to_src, path_to_dest): 
        with self.sync_thread.protect_cache_from_write_access:
            self.sync_thread.duplicate(path_to_src, path_to_dest)
      
    def get_modified(self, path):
        if self.entries.exists(path):
            return self.entries.get_modified(path)
        #print "get chunk id"
        chunk_id = self.sync_thread.chunk_mapper.get_chunk_uuid(path)
        #print "get chunk id2"
        if not chunk_id:
            return self.get_metadata(path)["modified"] #may be a directory
        return self.get_metadata(get_parent_dir(path)+chunk_id)["modified"]
    
    def get_directory_listing(self, directory):
        #merge cached files and entries from store into set with unique entries
        store_item_list = []
        store_listing = self.store.get_directory_listing(directory)
        for item in store_listing:
            #print "listing item: "+item 
            files = self.sync_thread.chunk_mapper.get_files_in_chunk(str(item))
            if not files:
                store_item_list += [item] # add directories
            else:
                store_item_list += files
        cache_listing = []
        for path in self.entries.get_dirty_lru_entries(float("inf")):
            if os.path.dirname(path) == directory:
                #print "listing dirty item: "+path 
                cache_listing.append( path )
        ret = list(set( cache_listing + store_item_list ))
        return ret
    
    def get_bytes(self, path): # might be directory
        if not self.entries.exists(path):
            if self.sync_thread.chunk_mapper.get_chunk_uuid(path):
                self._refresh_cache(path)
            else:
                return self.get_metadata(path)['bytes'] # assume that it is a directory, since it is not a file in the mapping or cache
        return len(self.entries.peek(path))
        
    def exists(self, path):##
        if self.entries.exists(path) or self.sync_thread.chunk_mapper.get_chunk_uuid(path):
            return True
        try:
            self.get_metadata(path)
            return True
        except NoSuchFilesytemObjectError,e:
            #print "error:"+str(e)
            return False
    
    def get_metadata(self, path): 
        with self.sync_thread.protect_cache_from_write_access: # think about performance issues here, since this is called all the time
            metadata = {}
            if not self.entries.exists(path):
                if not self.sync_thread.chunk_mapper.get_chunk_uuid(path):
                    return self.store.get_metadata(path) # cannot assume that it is a directory, since the filesystem calls this on non existing files that are to be created
                self._refresh_cache(path)    
            metadata['modified'] = self.entries.get_modified(path)
            metadata['bytes'] = len(self.entries.peek(path))
            metadata['is_dir'] = False
            return metadata

    def is_dir(self, path):
        if self.entries.exists(path) or self.sync_thread.chunk_mapper.get_chunk_uuid(path):
            return False
        return self.get_metadata(path)['is_dir']# we could also assume that it is a directory, since it is not a file in the mapping or cache
            
    def get_logging_handler(self):
        return self.store.get_logging_handler()
    
    def reconnect(self):
        self.store.reconnect()
        
    def get_max_filesize(self):
        """Return maximum number of bytes per file"""
        return self.store.get_max_filesize()
