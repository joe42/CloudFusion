from cloudfusion.store.store import Store
from cloudfusion.util import *
import time
from cloudfusion.util.cache import Cache
import os.path
import logging

class Entry(object):
    def __init__(self):
        self.modified = None
        self.size = None
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

#class Entries(Cache):
        
class MetadataCachingStore(Store):
    def __init__(self, store, cache_expiration_time):
        self.store = store
        self.logger = logging.getLogger(self.get_logging_handler())
        self.logger.debug("creating MetadataCachingStore object")
        self.entries = Cache(cache_expiration_time)
        self.store_metadata = Cache(cache_expiration_time)
    
    def _is_valid_path(self, path):
        return self.store._is_valid_path(path)
    
    def _raise_error_if_invalid_path(self, path):
        self.store._raise_error_if_invalid_path(path)
        
    def get_name(self):
        if not self.store_metadata.exists('store_name'):
            self.store_metadata.write('store_name', self.store.get_name())
        return self.store_metadata.get_value('store_name')
    
    def get_file(self, path_to_file):
        self.logger.debug("meta cache get_file %s" % path_to_file)
        ret = self.store.get_file(path_to_file)
        self.logger.debug("meta cache got %s" % repr(ret)[:10])
        if not self.entries.exists(path_to_file):
            self.entries.write(path_to_file, Entry())
        entry = self.entries.get_value(path_to_file)
        entry.set_is_file()
        try:
            entry.size = len(ret)
        except:
            pass
        self.logger.debug("meta cache returning %s" % repr(ret)[:10])
        return ret
    
    def _add_to_parent_dir_listing(self, path):
        parent_dir = os.path.dirname(path)
        if not self.entries.exists(parent_dir):
            self.entries.write(parent_dir, Entry())
        entry = self.entries.get_value(parent_dir)
        entry.set_is_dir()
        entry.add_to_listing(path)
        
    def _remove_from_parent_dir_listing(self, path):
        parent_dir = os.path.dirname(path)
        if self.entries.exists(parent_dir):
            entry = self.entries.get_value(parent_dir)
            entry.remove_from_listing(path)
        
    def store_file(self, path_to_file, dest_dir="/", remote_file_name = None):
        if dest_dir == "/":
            dest_dir = ""
        if not remote_file_name:
            remote_file_name = os.path.basename(path_to_file)
        self.logger.debug("meta cache store_file %s" % dest_dir + "/" + remote_file_name)
        fileobject = open(path_to_file)
        self.store_fileobject(fileobject, dest_dir + "/" + remote_file_name)
        fileobject.close()
        
    def store_fileobject(self, fileobject, path):
        self.logger.debug("meta cache store_fileobject %s" % path)
        fileobject.seek(0)
        data_len = len(fileobject.read())
        fileobject.seek(0)
        self.store.store_fileobject(fileobject, path)
        fileobject.close()
        if not self.entries.exists(path):
            self.entries.write(path, Entry())
        entry = self.entries.get_value(path)
        entry.set_is_file()
        entry.size = data_len
        entry.set_modified()
        self._add_to_parent_dir_listing(path)
            
    def delete(self, path): 
        self.logger.debug("meta cache delete %s" % path)
        self.store.delete(path)
        self.entries.delete(path)
        self._remove_from_parent_dir_listing(path)
          
    def account_info(self):
        return self.store.account_info()
    
    def get_free_space(self):
        return self.store.get_free_space()
    
    def get_overall_space(self):
        return self.store.get_overall_space()
    
    def get_used_space(self):
        return self.store.get_used_space()

    def create_directory(self, directory):
        self.logger.debug("meta cache create_directory %s" % directory)
        ret = self.store.create_directory(directory)
        if not self.entries.exists(directory):
            self.entries.write(directory, Entry())
        entry = self.entries.get_value(directory)
        entry.set_is_dir()
        entry.listing = []
        entry.set_modified()
        self._add_to_parent_dir_listing(directory)
        return ret
        
    def duplicate(self, path_to_src, path_to_dest):
        self.logger.debug("meta cache duplicate %s to %s" % (path_to_src, path_to_dest))
        ret = self.store.duplicate(path_to_src, path_to_dest)
        if self.entries.exists(path_to_src):
            entry = self.entries.get_value(path_to_src)
            self.entries.write(path_to_dest, entry)
        else:
            self.entries.write(path_to_dest, Entry())
        entry = self.entries.get_value(path_to_src)
        entry.set_modified()
        self._add_to_parent_dir_listing(path_to_dest)
        self.logger.debug("duplicated %s to %s" % (path_to_src, path_to_dest))
        return ret
        
    def move(self, path_to_src, path_to_dest):
        self.logger.debug("meta cache move %s to %s" % (path_to_src, path_to_dest))
        self.store.move(path_to_src, path_to_dest)
        if self.entries.exists(path_to_src):
            entry = self.entries.get_value(path_to_src)
            self.entries.write(path_to_dest, entry)
        else:
            self.entries.write(path_to_dest, Entry())
        entry = self.entries.get_value(path_to_src)
        entry.set_modified()
        self.entries.delete(path_to_src)
        self._remove_from_parent_dir_listing(path_to_src)
        self._add_to_parent_dir_listing(path_to_dest)
 
    def get_modified(self, path):
        self.logger.debug("meta cache get_modified %s" % path)
        if self.entries.exists(path):
            entry = self.entries.get_value(path)
            if not entry.modified == None:
                return entry.modified
        modified = self.store.get_modified(path)
        if not self.entries.exists(path):
            self.entries.write(path, Entry())
            entry = self.entries.get_value(path)
        entry.set_modified(modified)
        return entry.modified
    
    def get_directory_listing(self, directory):
        self.logger.debug("meta cache get_directory_listing %s" % directory)
        if self.entries.exists(directory):
            entry = self.entries.get_value(directory)
            if not entry.listing == None:
                self.logger.debug("return cached listing %s" % repr(entry.listing))
                return list(entry.listing)
        listing =  self.store.get_directory_listing(directory)
        self.logger.debug("meta cache caching %s" % repr(listing))
        if not self.entries.exists(directory):
            self.entries.write(directory, Entry())
            entry = self.entries.get_value(directory)
        entry.listing =  listing
        assert self.entries.get_value(directory).listing == entry.listing
        self.logger.debug("asserted %s" % repr(self.entries.get_value(directory).listing))
        return list(entry.listing)
    
    def get_bytes(self, path):
        self.logger.debug("meta cache get_bytes %s" % path)
        if self.entries.exists(path):
            entry = self.entries.get_value(path)
            if not entry.size == None:
                return entry.size
        size = self.store.get_bytes(path)
        if not self.entries.exists(path):
            self.entries.write(path, Entry())
            entry = self.entries.get_value(path)
        entry.size =  size
        return entry.size
    
    def exists(self, path):
        self.logger.debug("meta cache exists %s" % path)
        if not self.entries.exists(path):
            if self.store.exists(path):
                self.entries.write(path, Entry())
        return self.entries.exists(path)
    
    def _get_metadata(self, path):
        self.logger.debug("meta cache _get_metadata %s" % path)
        if self.entries.exists(path):
            entry = self.entries.get_value(path)
            self.logger.debug("entry exists")
            if not None in [entry.is_dir, entry.modified, entry.size]:
                return {'is_dir': entry.is_dir, 'modified': entry.modified, 'bytes': entry.size}
        self.logger.debug("meta cache _get_metadata entry does not exist")
        metadata = self.store._get_metadata(path)
        if not self.entries.exists(path):
            self.entries.write(path, Entry())
            entry = self.entries.get_value(path)
        if metadata['is_dir']:
            entry.set_is_dir()
        else:
            entry.set_is_file()
        entry.modified = metadata['modified']
        entry.size = metadata['bytes']
        return {'is_dir': entry.is_dir, 'modified': entry.modified, 'bytes': entry.size}

    def is_dir(self, path):
        self.logger.debug("meta cache is_dir %s" % path)
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
        return entry.is_dir
    
    def get_logging_handler(self):
        return self.store.get_logging_handler()
    
    def flush(self):
        self.store.flush()
