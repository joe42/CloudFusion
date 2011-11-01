'''
Created on 25.04.2011

@author: joe
'''
'''
Created on 08.04.2011

@author: joe
'''
    
import tempfile
from cloudfusion.util.cache import Cache
from cloudfusion.store.dropbox.file_decorator import *
from cloudfusion.store.store import *
import time
import logging

"""Wrapped store needs a logger as an attribute called logger """

class CachingStore(Store):
    def __init__(self, store, cache_expiration_time):
        """":param store: the store whose access should be cached 
            :param cache_expiration_time: the time in seconds until any cache entry is expired""" 
        self.store = store
        self.logger = logging.getLogger(self.get_logging_handler())
        self.logger.debug("creating CachingStore object")
#        self.temp_file = tempfile.SpooledTemporaryFile()
        self.cache_expiration_time = cache_expiration_time
        self.time_of_last_flush = time.time()
        self.entries = Cache(cache_expiration_time)
    
    def get_cache_expiration_time(self):
        """:returns: the time in seconds until any cache entry is expired"""
        return self.cache_expiration_time;
    
    def get_time_of_last_flush(self):
        """:returns: the most recent point of time at which the cache has been flushed"""
        return self.time_of_last_flush;
    
    def _is_valid_path(self, path):
        return self.store._is_valid_path(path)
    
    def _raise_error_if_invalid_path(self, path):
        self.store._raise_error_if_invalid_path(path)
        
    def get_name(self):
        return self.store.get_name()
    
    def _refresh_cache(self, path_to_file):
        """ Reloads the existing entries entry with the key :param:`path_to_file` from the wrapped store, if the wrapped store version is newer.
        The entries entries last updated time is set to the current point of time.
        Makes a new entries entry if it does not exist yet.
        If it was changed in the wrapped store after the entries entry's modified time:
        
        * The modified time stamp of the entries entry is set to the wrapped stores modified time stamp.
        * The entry is set to not dirty.
        * The entry's value is set to the wrapped store's value.
        :raises: NoSuchFilesytemObjectError if file does not exist in wrapped store.
        TODO: exception raising does not work and needs to be implemented 
        """
        cached_version_is_invalid = self.is_cached_version_invalid(path_to_file)
        if cached_version_is_invalid:
            actual_modified_date = self._get_actual_modified_date(path_to_file)
            data = self.store.get_file(path_to_file)
            modified = actual_modified_date
            self.entries.refresh(path_to_file, data, modified)
            self.logger.debug("refreshed cached fileobject from store")
        self.entries.update(path_to_file)
        
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
        """ :returns: string -- the data of the file with the path :param: path_to_file
        If the files entry is dirty and expired it is written to the wrapped store first and set to not dirty and update time is set to the current point of time.
        If the file was updated in the wrapped store, then its content will be updated if its entry is expired. 
        """
        self.logger.debug("cached get_file %s" % path_to_file)
        if not self.entries.exists(path_to_file):
            self._refresh_cache(path_to_file)
            self.logger.debug("cached get_file from new entry")
            return self.entries.get_value(path_to_file)
        if self.entries.is_expired(path_to_file): 
            if self.entries.is_dirty(path_to_file): #dirty&expired->flush
                self.logger.debug("cached get_file flushing")
                self.__flush(path_to_file)
            else: #not dirty&expired->update cache from store
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
        """ Stores a fileobject to the :class:`~cloudfusion.util.entries.Cache` and if the existing fileobject has expired it is also flushed to the wrapped store.
        The entry's updated and modified attributes will be reset to the current point of time.
        The entry's dirty flag is set to False if the entry has expired and was hence written to the store. Otherwise it is set to True.
        :param fileobject: The file object with the method read() returning data as a string 
        :param path: The path where the file object's data should be stored, including the filename
        """
        self.logger.debug("cached storing %s" % path)
        flush = self.entries.exists(path) and self.entries.is_expired(path)
        self.entries.write(path, fileobject.read())
        self.logger.debug("cached storing value %s..." %self.entries.get_value(path)[:10]) 
        if flush:
            self.logger.debug("cache entry for %s is expired -> flushing" % path) 
            self._flush(path)

    def delete(self, path):#delete from metadata 
        self.entries.delete(path)
        if self.store.exists(path):  
            self.store.delete(path)
          
    def account_info(self):
        self.flush()
        return self.store.account_info()
    
    def get_free_space(self):
        self.flush()
        return self.store.get_free_space()
    
    def get_overall_space(self):
        self.flush()
        return self.store.get_overall_space()
    
    def get_used_space(self):
        self.flush()
        return self.store.get_used_space()

    def create_directory(self, directory):
        return self.store.create_directory(directory)
        
    def duplicate(self, path_to_src, path_to_dest):
        self.__flush(path_to_src)
        self.__flush(path_to_dest)
        self.logger.debug("cached storing duplicate %s to %s" % (path_to_src, path_to_dest))
        ret = self.store.duplicate(path_to_src, path_to_dest)
        if self.entries.exists(path_to_src):
            self.entries.write(path_to_dest, self.entries.get_value(path_to_src))
        return ret
        
    def move(self, path_to_src, path_to_dest):
        if self.entries.exists(path_to_src) and self.entries.is_dirty(path_to_src): #less bandwith usage if it is copied locally
            self.entries.write(path_to_dest, self.entries.get_value(path_to_src))
            self.entries.delete(path_to_src)
            return
        #it already was up to date at the remote server:
        self.store.move(path_to_src, path_to_dest)
        if self.entries.exists(path_to_src): 
            self.entries.write(path_to_dest, self.entries.get_value(path_to_src))
            self.entries.delete(path_to_src)
 
    def get_modified(self, path):
        return self._get_metadata(path)["modified"]
    
    def get_directory_listing(self, directory):
        self.flush()
        return self.store.get_directory_listing(directory) #so far only cached by dropbox
    
    def get_bytes(self, path):
        return self._get_metadata(path)['bytes']
    
    def exists(self, path):
        if self.entries.exists(path):
            return True
        try:
            self._get_metadata(path)
            return True
        except NoSuchFilesytemObjectError:
            return False;
    
    def _get_metadata(self, path):
        self.flush()
        return self.store._get_metadata(path)

    def is_dir(self, path):
        return self._get_metadata(path)["is_dir"]
    
    def flush(self):
        """ Writes all dirty  entries to the wrapped store."""
        self.logger.debug("flushing entries")
        self.time_of_last_flush = time.time()
        for path in self.entries.get_keys():
            self.__flush(path)
            
    def __flush(self, path):
        """ Writes the entry with the key :param:`path` to the wrapped store, only if it is dirty."""
        if not self.entries.exists(path):
            return
        self.entries.update(path)
        if self.entries.is_dirty(path):
            file = DataFileWrapper(self.entries.get_value(path))
            file.fileno()#
            self.store.store_fileobject(file, path)
            self.entries.flush(path)
            self.logger.debug("flushing %s with content starting with %s" % (path, self.entries.get_value(path)[0:10]))
            
            
    def get_logging_handler(self):
        return self.store.get_logging_handler()