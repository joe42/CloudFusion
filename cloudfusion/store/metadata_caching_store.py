from cloudfusion.store.store import Store, NoSuchFilesytemObjectError
from cloudfusion.util import *
import time
from cloudfusion.util.cache import Cache
import os.path
import logging
from copy import deepcopy
from cloudfusion.store.store_worker import GetFreeSpaceWorker
from cloudfusion.util.mp_synchronize_proxy import MPSynchronizeProxy
from cloudfusion.store.bulk_get_metadata import BulkGetMetadata

class Entry(object):
    def __init__(self):
        self.modified = None
        self.size = 0
        self.is_dir = None
        self.is_file = None
        self.listing = None
    def set_is_file(self):
        self.is_file = True
        self.is_dir = False 
        self.listing = None
    def set_is_dir(self):
        self.is_file = False 
        self.is_dir = True
        self.size = 0
    def set_modified(self, modified=None):
        if not modified:
            self.modified= time.time()
        else:
            self.modified = modified
    def add_to_listing(self, path):
        if self.listing == None:
            return
        if not path in self.listing: 
            self.listing.append(path)
    def remove_from_listing(self, path):
        if self.listing == None:
            return
        if path in self.listing: 
            self.listing.remove(path)

class MetadataCachingStore(Store):
    def __init__(self, store, cache_expiration_time=60):
        self.store = store
        self.logger = logging.getLogger(self.get_logging_handler())
        self.logger.debug("creating MetadataCachingStore object")
        self.entries = MPSynchronizeProxy( Cache(cache_expiration_time) ) 
        if cache_expiration_time < 240:
            self.logger.warning("Be aware of the synchronization issue https://github.com/joe42/CloudFusion/issues/16 \
                    or to avoid the issue set cache_expiration_time to more than 240 seconds.")
        self.store_metadata = Cache(cache_expiration_time)
        self.free_space_worker = GetFreeSpaceWorker(deepcopy(store), self.logger)
        self.free_space_worker.start()
        self._last_cleaned = time.time()
    
    def _is_valid_path(self, path):
        return self.store._is_valid_path(path)
    
    def _raise_error_if_invalid_path(self, path):
        self.store._raise_error_if_invalid_path(path)
        
    def get_name(self):
        if not self.store_metadata.exists('store_name'):
            self.store_metadata.write('store_name', self.store.get_name())
        return self.store_metadata.get_value('store_name')
    
    def get_file(self, path_to_file):
        self.logger.debug("meta cache get_file %s", path_to_file)
        ret = self.store.get_file(path_to_file)
        if not self.entries.exists(path_to_file):
            self.entries.write(path_to_file, Entry())
        entry = self.entries.get_value(path_to_file)
        entry.set_is_file()
        try:
            entry.size = len(ret)
            self.entries.write(path_to_file, entry)
        except Exception, e:
            self.entries.delete(path_to_file)
        self.logger.debug("meta cache returning %s", repr(ret)[:10])
        self._add_to_parent_dir_listing(path_to_file)
        return ret
    
    def _add_to_parent_dir_listing(self, path):
        if path != '/':   
            parent_dir = os.path.dirname(path)
            self._add_parent_dir_listing(path)
            entry = self.entries.get_value(parent_dir)
            entry.add_to_listing(path)
            self.entries.write(parent_dir, entry)
    
    
    def _add_parent_dir_listing(self, path):
        '''Add listing for parent directory of path to cache if it does not yet exist'''
        if path != '/':   
            parent_dir = os.path.dirname(path)
            if not self.entries.exists(parent_dir):
                self.entries.write(parent_dir, Entry())
            entry = self.entries.get_value(parent_dir) 
            if entry.listing == None:
                entry.listing = self.store.get_directory_listing(parent_dir)
            entry.set_is_dir()
            self.entries.write(parent_dir, entry)
        
    def _does_not_exist_in_parent_dir_listing(self, path):
        '''':returns: True if path does not exist in the cached directory listing'''
        parent_dir = os.path.dirname(path)
        if path == '/':
            return False
        if self.entries.exists(parent_dir):
            entry = self.entries.get_value(parent_dir)
            if entry.listing != None and (not unicode(path) in entry.listing and not path in entry.listing):
                self.logger.debug("%s does not exist in parent directory: %s..."%(path, repr(entry.listing[0:5])))
                return True
        return False
    
    def _remove_from_parent_dir_listing(self, path):
        parent_dir = os.path.dirname(path)
        if self.entries.exists(parent_dir):
            entry = self.entries.get_value(parent_dir)
            entry.remove_from_listing(path)
            self.entries.write(parent_dir, entry)
        
    def store_file(self, path_to_file, dest_dir="/", remote_file_name = None, interrupt_event=None):
        if dest_dir == "/":
            dest_dir = ""
        if not remote_file_name:
            remote_file_name = os.path.basename(path_to_file)
        self.logger.debug("meta cache store_file %s", dest_dir + "/" + remote_file_name)
        with open(path_to_file) as fileobject:
            fileobject.seek(0,2)
            data_len = fileobject.tell()
        path = dest_dir + "/" + remote_file_name
        self.logger.debug("meta cache store_file %s", path)
        ret = self.store.store_file(path_to_file, dest_dir, remote_file_name, interrupt_event)
        if not self.entries.exists(path):
            self.entries.write(path, Entry())
        entry = self.entries.get_value(path)
        entry.set_is_file()
        entry.size = data_len
        entry.set_modified()
        self.entries.write(path, entry)
        self._add_to_parent_dir_listing(path)
        return ret
    
    def __get_size(self, fileobject):
        pos = fileobject.tell()
        fileobject.seek(0,2)
        size = fileobject.tell()
        fileobject.seek(pos, 0)
        return size
        
    def store_fileobject(self, fileobject, path, interrupt_event=None):
        self.logger.debug("meta cache store_fileobject %s", path)
        data_len = self.__get_size(fileobject)
        try:
            ret = self.store.store_fileobject(fileobject, path, interrupt_event)
        finally:
            fileobject.close()
        if not self.entries.exists(path):
            self.entries.write(path, Entry())
        entry = self.entries.get_value(path)
        entry.set_is_file()
        entry.size = data_len
        entry.set_modified()
        self._add_to_parent_dir_listing(path)
        self.entries.write(path, entry)
        return ret
            
    def delete(self, path, is_dir): 
        self.logger.debug("meta cache delete %s", path)
        self.store.delete(path, is_dir)
        self.entries.delete(path)
        self._remove_from_parent_dir_listing(path)
          
    def account_info(self):
        if not self.store_metadata.exists('account_info'):
            self.store_metadata.write('account_info', self.store.account_info())
        return self.store_metadata.get_value('account_info')
    
    def get_free_space(self):
        return self.free_space_worker.get_free_bytes_in_remote_store()
    
    def get_overall_space(self):
        if not self.store_metadata.exists('overall_space') or self.store_metadata.is_expired('overall_space'):
            self.store_metadata.write('overall_space', self.store.get_overall_space())
        return self.store_metadata.get_value('overall_space')
    
    def get_used_space(self):
        if not self.store_metadata.exists('used_space') or self.store_metadata.is_expired('used_space'):
            self.store_metadata.write('used_space', self.store.get_used_space())
        return self.store_metadata.get_value('used_space')

    def create_directory(self, directory):
        self.logger.debug("meta cache create_directory %s", directory)
        ret = self.store.create_directory(directory)
        if not self.entries.exists(directory):
            self.entries.write(directory, Entry())
        entry = self.entries.get_value(directory)
        entry.set_is_dir()
        entry.listing = []
        entry.set_modified()
        self.entries.write(directory, entry)
        self._add_to_parent_dir_listing(directory)
        return ret
        
    def duplicate(self, path_to_src, path_to_dest):
        self.logger.debug("meta cache duplicate %s to %s", path_to_src, path_to_dest)
        ret = self.store.duplicate(path_to_src, path_to_dest)
        if self.entries.exists(path_to_src):
            entry = deepcopy(self.entries.get_value(path_to_src))
            self.entries.write(path_to_dest, entry)
        else:
            self.entries.write(path_to_dest, Entry())
        entry = self.entries.get_value(path_to_dest)
        entry.set_modified()
        self.entries.write(path_to_dest, entry)
        self._add_to_parent_dir_listing(path_to_dest)
        self.logger.debug("duplicated %s to %s", path_to_src, path_to_dest)
        return ret
        
    def move(self, path_to_src, path_to_dest):
        self.logger.debug("meta cache move %s to %s", path_to_src, path_to_dest)
        self.store.move(path_to_src, path_to_dest)
        if self.entries.exists(path_to_src):
            entry = self.entries.get_value(path_to_src)
            self.entries.write(path_to_dest, entry)
        else:
            self.entries.write(path_to_dest, Entry())
        entry = self.entries.get_value(path_to_src)
        entry.set_modified()
        self.entries.write(path_to_dest, entry)
        self.entries.delete(path_to_src)
        self._remove_from_parent_dir_listing(path_to_src)
        self._add_to_parent_dir_listing(path_to_dest)
 
    def get_modified(self, path):
        self.logger.debug("meta cache get_modified %s", path)
        if self.entries.exists(path):
            entry = self.entries.get_value(path)
            if not entry.modified == None:
                return entry.modified
        modified = self.store.get_modified(path)
        if not self.entries.exists(path):
            self.entries.write(path, Entry())
            entry = self.entries.get_value(path)
        entry.set_modified(modified)
        self.entries.write(path, entry)
        return entry.modified
    
    def get_directory_listing(self, directory):
        self.logger.debug("meta cache get_directory_listing %s", directory)
        if self.entries.exists(directory):
            entry = self.entries.get_value(directory)
            if not entry.listing == None:
                self.logger.debug("return cached listing %s", repr(entry.listing))
                return list(entry.listing)
        listing =  self.store.get_directory_listing(directory)
        self.logger.debug("meta cache caching %s", repr(listing))
        if not self.entries.exists(directory):
            self.entries.write(directory, Entry())
            entry = self.entries.get_value(directory)
        entry.listing =  listing
        self._add_existing_items(entry, directory)
        self.entries.write(directory, entry)
        self.logger.debug("asserted %s", repr(self.entries.get_value(directory).listing))
        assert self.entries.get_value(directory).listing == entry.listing
        return list(entry.listing)
    
    def _add_existing_items(self, dir_entry, dir_entry_path):
        '''Add existing files or directories to *dir_entry* because they might have been 
        uploaded recently and might not be retrievable by a directory listing from the storage provider.'''
        for path in self.entries.get_keys():
            if not self.entries.is_expired(path):
                if os.path.dirname(path) == dir_entry_path:
                    dir_entry.add_to_listing(path)
    
    def get_bytes(self, path):
        self.logger.debug("meta cache get_bytes %s", path)
        if self.entries.exists(path):
            entry = self.entries.get_value(path)
            if not entry.size == None:
                return entry.size
        size = self.store.get_bytes(path)
        if not self.entries.exists(path):
            self.entries.write(path, Entry())
            entry = self.entries.get_value(path)
        entry.size =  size
        self.entries.write(path, entry)
        return entry.size
    
    def exists(self, path):
        self.logger.debug("meta cache exists %s", path)
        if not self.entries.exists(path):
            if self.store.exists(path):
                self.entries.write(path, Entry())
        return self.entries.exists(path)
    
    def clean_expired_cache_entries(self):
        '''Delete all expired cache entries.'''
        for path in self.entries.get_keys():
            if self.entries.is_expired(path):
                self.entries.delete(path)
    
    def __clean_cache(self):
        '''Delete all expired cache entries only if last called 
        after *cache_expiration_time* seconds as defined in the constructor.'''
        if self._last_cleaned + self.entries.expire < time.time():
            self.clean_expired_cache_entries()
            self._last_cleaned = time.time()
    
    def get_metadata(self, path):
        '''As a side effect cleans expired cache entries from time to time'''
        self.logger.debug("meta cache get_metadata %s", path)
        self.__clean_cache()
        self._add_parent_dir_listing(path)
        if self._does_not_exist_in_parent_dir_listing(path):
            raise NoSuchFilesytemObjectError(path,0)
        if self.entries.exists(path):
            entry = self.entries.get_value(path)
            self.logger.debug("entry exists")
            if not None in [entry.is_dir, entry.modified, entry.size]:
                return {'is_dir': entry.is_dir, 'modified': entry.modified, 'bytes': entry.size}
        self.logger.debug("meta cache get_metadata entry does not exist or is expired")
        metadata = self.store.get_metadata(path)
        entry = self._prepare_entry(path, metadata)
        self.entries.write(path, entry)
        if not entry.is_dir and isinstance(self.store, BulkGetMetadata):
            self._prefetch_directory(os.path.dirname(path))
        return {'is_dir': entry.is_dir, 'modified': entry.modified, 'bytes': entry.size}
    
    def _prepare_entry(self, path, metadata):
        if self.entries.exists(path):
            entry = self.entries.get_value(path) #preserve listings
        else:
            entry = Entry()
        if metadata['is_dir']:
            entry.set_is_dir()
        else:
            entry.set_is_file()
        entry.modified = metadata['modified']
        entry.size = metadata['bytes']
        return entry
    
    def _prefetch_directory(self, path):
        self.logger.debug("prefetch %s", path)
        bulk = self.store.get_bulk_metadata(path)
        bulk.items()
        for path, metadata in bulk.items():
            e = Entry()
            self.entries.write(path, self._prepare_entry(path,metadata))
        self.logger.debug("prefetch succeeded %s", path)

    def is_dir(self, path):
        self.logger.debug("meta cache is_dir %s", path)
        if self.entries.exists(path):
            entry = self.entries.get_value(path)
            if not entry.is_dir == None:
                return entry.is_dir
        is_dir = self.store.is_dir(path)
        if not self.entries.exists(path):
            self.entries.write(path, Entry())
            entry = self.entries.get_value(path)
        if is_dir:
            entry.set_is_dir()
        self.entries.write(path, entry)
        return entry.is_dir
    
    def get_logging_handler(self):
        return self.store.get_logging_handler()
    
    def set_configuration(self, config):
        self.store.set_configuration(config)
    
    def get_configuration(self, config):
        return self.store.get_configuration()
    
    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k == 'logger':
                setattr(result, k, self.logger)
            elif k == '_logging_handler':
                setattr(result, k, self._logging_handler)
            elif k == 'entries':
                setattr(result, k, self.entries)
            else:
                setattr(result, k, deepcopy(v, memo))
        return result
    
    def get_max_filesize(self):
        """Return maximum number of bytes per file"""
        return self.store.get_max_filesize()
