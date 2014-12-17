'''
Created on 08.04.2011

@author: joe
'''
from cloudfusion.store.store import NoSuchFilesytemObjectError, StoreAccessError,\
    AlreadyExistsError 
import time
from cloudfusion.store.store import *
import logging
from cloudfusion.util.exponential_retry import retry
from cloudfusion.mylogging import db_logging_thread
import tempfile
from cloudfusion.store.webdav.tinydav_client import TinyDAVClient
from datetime import datetime

class WebdavStore(Store):
    def __init__(self, config):
        '''*config* is a dictionary with the keys user, password, and URL. For instance::
        
                #url can also contain an existing subfolder to access, i.e. https://webdav.mediencenter.t-online.de/myfolder
                #url can also contain the port for the WebDAV server, i.e. https://webdav.mediencenter.t-online.de:443
                config['url'] = 'https://webdav.mediencenter.t-online.de' 
                config['user'] = 'me@emailserver.com' #your account username/e-mail address
                config['password'] = 'MySecret!23$' #your account password

        :param config: dictionary with key value pairs'''
        super(WebdavStore, self).__init__()
        self.name = 'webdav'
        self._logging_handler = self.name
        self.logger = logging.getLogger(self._logging_handler)
        self.logger = db_logging_thread.make_logger_multiprocessingsave(self.logger)
        self.logger.info("creating %s store", self.name)
        self.tinyclient = TinyDAVClient(config['url'], config['user'], config['password'] )
        self.logger.info("api initialized")
        
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
            elif k == 'tinyclient':
                setattr(result, k, self.tinyclient)
            else:
                setattr(result, k, deepcopy(v, memo))
        return result
        
    def get_name(self):
        self.logger.info("getting name")
        return self.name
    
    @retry((Exception), tries=14, delay=0.1, backoff=2)
    def get_file(self, path_to_file): 
        self.logger.info("getting file: %s", path_to_file)
        self._raise_error_if_invalid_path(path_to_file)
        return self.tinyclient.get_file(path_to_file) 
        
    def __get_size(self, fileobject):
        pos = fileobject.tell()
        fileobject.seek(0,2)
        size = fileobject.tell()
        fileobject.seek(pos, 0)
        return size
    
    @retry((Exception), tries=2, delay=0) 
    def store_fileobject(self, fileobject, path, interrupt_event=None):
        size = self.__get_size(fileobject)
        self.logger.info("Storing file object of size %s to %s", size, path)
        if hasattr(fileobject, 'name') and fileobject.name is not None:
            file_name = fileobject.name
            self.tinyclient.upload(file_name, path)
        else:
            with tempfile.NamedTemporaryFile() as fh:
                for line in fileobject:
                    fh.write(line)
                fh.flush()
                file_name = fh.name
                self.tinyclient.upload(file_name, path)
        return int(time.time())
    
    
    # worst case: object still exists and takes up space or is appended to, by mistake
    # with caching_store, the entry in cache is deleted anyways 
    @retry((Exception), tries=5, delay=0) 
    def delete(self, path, is_dir=False):
        self.logger.info("deleting %s", path)
        self._raise_error_if_invalid_path(path)
        if is_dir:
            self.tinyclient.rmdir(path)
        else:
            self.tinyclient.rm(path)
        
    def account_info(self):
        self.logger.debug("retrieving account info")
        return "Webdav"

    @retry((Exception), tries=14, delay=0.1, backoff=2)
    def create_directory(self, directory):
        self.logger.info("creating directory %s", directory)
        self.tinyclient.mkdir(directory)
        
    @retry((Exception), tries=1)
    def duplicate(self, path_to_src, path_to_dest):
        self.logger.info("duplicating %s to %s", path_to_src, path_to_dest)
        self.tinyclient.copy(path_to_src, path_to_dest)
    
    @retry((Exception), tries=1)
    def move(self, path_to_src, path_to_dest):
        self.logger.info("moving %s to %s", path_to_src, path_to_dest)
        self.tinyclient.move(path_to_src, path_to_dest)
    
    @retry((Exception), tries=4, delay=0.1, backoff=2) 
    def get_overall_space(self):
        self.logger.debug("retrieving all space") 
        return self.tinyclient.get_overall_space()

    @retry((Exception), tries=4, delay=0.1, backoff=2) #Got HTTP error BAD REQUEST 400 from t-online once
    def get_used_space(self):
        self.logger.debug("retrieving used space")
        return self.tinyclient.get_used_space()
        
    @retry((Exception), tries=14, delay=0.1, backoff=2)
    def get_directory_listing(self, directory):
        self.logger.debug("getting directory listing for %s", directory)
        return self.tinyclient.get_directory_listing(directory)
    
    def _handle_error(self, error, stacktrace, method_name, remaining_tries, *args, **kwargs):
        if method_name == 'get_file': #box.com does not instantly see files that are written to it (eventual consistency)
            if isinstance(error, NoSuchFilesytemObjectError) and remaining_tries != 0:
                return False
        if isinstance(error, NoSuchFilesytemObjectError) or \
            isinstance(error, AlreadyExistsError) or \
            isinstance(error, StoreAccessError):
            self.logger.error("Error could not be handled: \n%s", stacktrace)
            raise error
        if remaining_tries == 0: # throw error after last try 
            raise StoreAccessError(str(error), 0) 
        return False
        
    @retry((Exception), tries=14, delay=0.1, backoff=2)
    def get_metadata(self, path):
        self.logger.debug("getting metadata for %s", path)
        return self.tinyclient.get_metadata(path)
        
    def _get_time_difference(self):
        self.logger.debug("getting time difference")
        return 0
    
    def get_logging_handler(self):
        return self._logging_handler
    
    def get_max_filesize(self):
        """Return maximum number of bytes per file"""
        return 1000*1000*1000*1000