'''
Created on 08.04.2011

@author: joe
'''

import os
import time
import boto
from boto.gs.key import Key
from sh import gsutil, awk

from cloudfusion.store.store import *
import logging
from cloudfusion.util.exponential_retry import retry
from cloudfusion.mylogging import db_logging_thread
import sys

class GoogleStore(Store):
    def __init__(self, config):
        super(GoogleStore, self).__init__()
        self.name = 'google'
        self._logging_handler = self.name
        self.logger = logging.getLogger(self._logging_handler)
        self.logger = db_logging_thread.make_logger_multiprocessingsave(self.logger)
        self.logger.info("creating %s store", self.name)
        self.bucket_name = config['bucket_name']
        self.reconnect()
        self.logger.info("api initialized")
    
    def reconnect(self):
        access_key_id = 'GOOG4ZCB7TGXDIBQB764'
        secret_access_key = 'Sk2QJePjCIdOKqVVstiurdDy06mUxn/BloKcEMog'
        self.conn = boto.connect_gs(access_key_id,secret_access_key)
        buckets = map( lambda x: x.name, self.conn.get_all_buckets())
        if not self.bucket_name in buckets:
            try:
                self.conn.create_bucket(self.bucket_name)
                self.logger.debug('Successfully created bucket "%s"' % self.bucket_name)
            except boto.exception.StorageCreateError, e:
                self.logger.debug('Failed to create bucket:'+ repr(e))
                sys.exit()
        self.bucket = self.conn.get_bucket('cloudfusion')

        
    def __deepcopy__(self, memo):
        from copy import deepcopy
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k == 'logger':
                setattr(result, k, self.logger)
            elif k == '_logging_handler':
                setattr(result, k, self._logging_handler)
            else:
                setattr(result, k, deepcopy(v, memo))
        return result
        
    def get_name(self):
        self.logger.info("getting name")
        return self.name
    
    def get_file(self, path_to_file): 
        self.logger.debug("getting file: %s", path_to_file)
        #self._raise_error_if_invalid_path(path_to_file)
        k = Key(self.bucket)
        k.key = os.path.basename(os.path.basename(path_to_file))
        object_contents = k.get_contents_as_string()
        return object_contents
        
    def __get_size(self, fileobject):
        pos = fileobject.tell()
        fileobject.seek(0,2)
        size = fileobject.tell()
        fileobject.seek(pos, 0)
        return size
    
    @retry((Exception), tries=1, delay=0) 
    def store_fileobject(self, fileobject, path, interrupt_event=None):
        size = self.__get_size(fileobject)
        self.logger.debug("Storing file object of size %s to %s", size, path)
        k = Key(self.bucket)
        k.key = os.path.basename(path)
        k.set_contents_from_file(fileobject) # does not return bytes written
        return int(time.time())
    
    
    # worst case: object still exists and takes up space or is appended to, by mistake
    # with caching_store, the entry in cache is deleted anyways 
    @retry((Exception), tries=5, delay=0) 
    def delete(self, path, is_dir=False): #is_dir parameter does not matter to dropbox
        self.logger.debug("deleting %s", path)
        self._raise_error_if_invalid_path(path)
        path = os.path.basename(path)
        k = Key(self.bucket)
        k.key = path
        k.delete()
        
    @retry((Exception))
    def account_info(self):
        self.logger.debug("retrieving account info")
        return "Google Storage with bucket "+self.bucket_name

    @retry((Exception))
    def create_directory(self, directory):
        self.logger.debug("creating directory %s", directory)
        
    def duplicate(self, path_to_src, path_to_dest):
        self.logger.debug("duplicating %s to %s", path_to_src, path_to_dest)
    
    def move(self, path_to_src, path_to_dest):
        self.logger.debug("moving %s to %s", path_to_src, path_to_dest)
    
    def get_overall_space(self):
        self.logger.debug("retrieving all space")
        return 1000000000

    def get_used_space(self):
        self.logger.debug("retrieving used space")
        return int( awk(gsutil('du', '-s', 'gs://cloudfusion'), '{print $1}') )
        
    @retry((Exception))
    def get_directory_listing(self, directory):
        self.logger.debug("getting directory listing for %s", directory)
        #self._raise_error_if_invalid_path(directory)
        ret = map(lambda x: '/'+x.name, self.bucket.list())        
        return ret
        
    @retry((Exception))
    def _get_metadata(self, path):
        self.logger.debug("getting metadata for %s", path)
        self._raise_error_if_invalid_path(path)
        if path == "/": # workaraund for root metadata
            ret = {}
            ret["bytes"] = 0
            ret["modified"] = time.time()
            ret["path"] = "/"
            ret["is_dir"] = True
            return ret
        k = self.bucket.get_key(os.path.basename(path))
        ret = {}
        ret["bytes"] = k.size
        ret["modified"] = int(time.mktime( time.strptime(k.last_modified, '%a, %d %b %Y %H:%M:%S %Z') ))
        ret["path"] = path
        ret["is_dir"] = False
        return ret
        
    def _get_time_difference(self):
        self.logger.debug("getting time difference")
        return 0
    
    def get_logging_handler(self):
        return self._logging_handler
    
    def get_max_filesize(self):
        """Return maximum number of bytes per file"""
        return 1000*1000*1000*1000