'''
Created on 08.04.2011

@author: joe
'''

import os
import time
import boto
from boto.s3.key import Key
from boto.exception import *
from cloudfusion.store.store import *
import logging
from cloudfusion.util.exponential_retry import retry
from cloudfusion.mylogging import db_logging_thread
import sys
from ConfigParser import DuplicateSectionError
from cloudfusion.util.string import get_id_key, get_secret_key, get_uuid,\
    to_unicode, to_str

class AmazonStore(Store):
    '''Subclass of Store implementing an interface to the Amazon S3 storage.
    File objects are stored in a top level container called bucket. 
    There are no directories. They are simulated by appending a slash to a file object.
    I.e. "somepath/" is a directory name, but "somepath" without the slash is considered a file.
    Amazon S3 provides an interface to list file objects with a certain prefix, and a delimiter, 
    which is used to list single directories. I.e. bucket.list("somepath/", delimiter="/") 
    returns a list of  all file paths beginning with "somepath/" that do not contain a slash after the prefix or end in a slash.
    For instance: ["somepath/file1", "somepath/dir1/", "somepath/file2"]. For compatibility with other tools,
    file objects with content type "application/x-directory" or file objects ending with _$folder$ are recognized as directories, too.'''
    
    def __init__(self, config):
        '''*config* is a dictionary with the keys id (access_key_id), secret (secret_access_key), and bucket_name. For instance::

                config['id'] = 'FDS54548SDF8D2S311DF' 
                config['secret'] = 'D370JKD=564++873ZHFD9FDKDD'
                config['bucket_name'] = 'cloudfusion'
                
            The bucket will be created if it does not exist. A bucket is similar to a subfolder,
            to which access with CloudFusion is restricted.         
            Id and secret can be obtained from the console.aws.amazon.com/s3/home
            
            * Click on your name on the top left and select Security Credentials form the drop down menu.
            * Go to Access Keys and Generate New Access Keys to generate the new key pair.
            
        :param config: dictionary with key value pairs'''
        super(AmazonStore, self).__init__()
        self.name = 'amazon'
        self._logging_handler = self.name
        self.logger = logging.getLogger(self._logging_handler)
        self.logger = db_logging_thread.make_logger_multiprocessingsave(self.logger)
        self.logger.info("creating %s store", self.name)
        self.bucket_name = config['bucket_name']
        id_key = get_id_key(config)
        secret_key = get_secret_key(config)
        self.access_key_id = config[id_key]
        self.secret_access_key = config[secret_key]
        try:
            boto.config.add_section('Boto')
        except DuplicateSectionError, e:
            pass
        boto.config.set('Boto','debug','0') # no debug messages from boto itself
        boto.config.set('Boto','http_socket_timeout','10') # Set sensible timeout value
        self.reconnect()
        self.logger.info("api initialized")
     
    def reconnect(self):
        self.conn = boto.connect_s3(self.access_key_id,self.secret_access_key)
        buckets = map( lambda x: x.name, self.conn.get_all_buckets())
        if not self.bucket_name in buckets:
            try:
                self.conn.create_bucket(self.bucket_name)
                self.logger.info('Successfully created bucket "%s"' % self.bucket_name)
            except boto.exception.S3CreateError, e:
                uuid = "cloudfusion_"+get_uuid()
                msg = "Failed to create bucket %s; You can try another bucket name, for instance %s" % (self.bucket_name, uuid)
                if len(buckets) > 0:
                    msg += "\nor an already existing bucket: %s" % buckets
                self.logger.error('Failed to create bucket:'+ repr(e))
                self.logger.debug(msg)
                print msg
                sys.exit()
            except boto.exception.StorageCreateError, e:
                self.logger.error('Failed to create bucket:'+ repr(e))
                sys.exit()
        self.bucket = self.conn.get_bucket(self.bucket_name)

        
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
            elif k == 'conn':
                setattr(result, k, self.conn)
            elif k == 'bucket':
                setattr(result, k, self.bucket)
            else:
                setattr(result, k, deepcopy(v, memo))
        return result
        
    def get_name(self):
        self.logger.info("getting name")
        return self.name
    
    def get_file(self, path_to_file): 
        self.logger.info("getting file: %s", path_to_file)
        #self._raise_error_if_invalid_path(path_to_file)
        k = Key(self.bucket)
        k.key = path_to_file[1:]
        object_contents = k.get_contents_as_string()
        return object_contents
        
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
        k = Key(self.bucket)
        k.key = path[1:]
        k.set_contents_from_file(fileobject) # does not return bytes written
        return int(time.time())
    
    
    # worst case: object still exists and takes up space or is appended to, by mistake
    # with caching_store, the entry in cache is deleted anyways 
    @retry((Exception), tries=5, delay=0) 
    def delete(self, path, is_dir=False): 
        self.logger.info("deleting %s", path)
        self._raise_error_if_invalid_path(path)
        delete_list = []
        delete_list.append(path[1:])
        if is_dir:
            if not path.endswith('_$folder$'): #compatibility with different systems
                delete_list.append(path[1:]+'/')
        self.bucket.delete_keys(delete_list)
        
    @retry((Exception))
    def account_info(self):
        self.logger.debug("retrieving account info")
        return "Amazon S3 with bucket "+self.bucket_name

    @retry((Exception))
    def create_directory(self, directory):
        self.logger.info("creating directory %s", directory)
        k = Key(self.bucket)
        k.key = directory[1:]+'/'
        k.set_metadata('Content-Type', 'application/x-directory')
        k.set_contents_from_string('')
        
        
    @retry((Exception), tries=1)
    def duplicate(self, path_to_src, path_to_dest):
        self.logger.info("duplicating %s to %s", path_to_src, path_to_dest)
        path_to_dest = path_to_dest[1:] #remove / from beginning
        path_to_src = path_to_src[1:]
        k = self.bucket.get_key(path_to_src)
        is_dir = k == None 
        if not is_dir:
            k.copy(self.bucket, path_to_dest)
            return
        path_to_dest += '/'  
        path_to_src += '/'  
        listing = self.bucket.list(path_to_src, '/')  
        directories = [to_str(d) for d in listing if self._is_dir(d)]
        files = [to_str(f) for f in listing if not self._is_dir(d)]
        for key in directories:
            new_path = path_to_dest+to_str(key.name).split(path_to_src,1)[1]
            self.create_directory('/'+new_path[:-1])
        for key in files:
            new_path = path_to_dest+to_str(key.name).split(path_to_src,1)[1]
            key.copy(self.bucket, new_path)
    
    @retry((Exception), tries=1)
    def move(self, path_to_src, path_to_dest):
        self.logger.info("moving %s to %s", path_to_src, path_to_dest)
        path_to_dest = path_to_dest[1:] #remove / from beginning
        path_to_src = path_to_src[1:]
        k = self.bucket.get_key(path_to_src)
        #assume that src is a directory, if the key does not exist 
        #a more secure way would be to check if the directory exists by appending a slash to the path and trying to get it
        is_dir = k == None 
        if not is_dir:
            k.copy(self.bucket, path_to_dest)
            k.delete()
            return
        path_to_dest += '/'  
        path_to_src += '/'  
        listing = self.bucket.list(path_to_src, '/')  
        directories = [d for d in listing if self._is_dir(d)]
        files = [f for f in listing if not self._is_dir(d)]
        for key in directories:
            new_path = path_to_dest+to_str(key.name).split(path_to_src,1)[1]
            self.create_directory('/'+new_path[:-1])
        for key in files:
            new_path = path_to_dest+to_str(key.name).split(path_to_src,1)[1]
            key.copy(self.bucket, new_path)
        self.bucket.delete_keys([to_unicode(key.name) for key in listing])
    
    def get_overall_space(self):
        self.logger.debug("retrieving all space")
        return 1000*1000*1000*1000 #set to 1 TB 

    def get_used_space(self):
        self.logger.debug("retrieving used space")
        ret = 0
        for key in self.bucket:
            ret += key.size
        return ret
        
    @retry((Exception))
    def get_directory_listing(self, directory):
        self.logger.debug("getting directory listing for %s", directory)
        #self._raise_error_if_invalid_path(directory)
        directory += '/' if directory != '/' else ''
        listing = self.bucket.list(directory[1:], "/") 
        if directory != '/':            
            listing = [to_str(o) for o in listing if to_str(o.name) != directory[1:]]           
        listing = [to_str('/'+o.name) if o.name[-1] != '/' else '/'+to_str(o.name[:-1]) for o in listing] #remove trailing slash and add preceding slash
        return listing

    def _handle_error(self, error, stacktrace, method_name, remaining_tries, *args, **kwargs):
        """Used by retry decorator to react to errors."""
        if isinstance(error, AttributeError):
            self.logger.error("Retrying on funny socket error: %s", error)
            #funny socket error in httplib2: AttributeError 'NoneType' object has no attribute 'makefile'
        elif isinstance(error, NoSuchFilesytemObjectError):
                self.logger.debug("Error could not be handled: %s", error)
                raise error
        elif isinstance(error, S3ResponseError) and error.status == 404:
            self.logger.debug('file object does not exist in %s: %s' % (method_name, str(S3ResponseError)))
            raise NoSuchFilesytemObjectError('file object does not exist in %s: %s' % (method_name, str(S3ResponseError)))
        else:
            self.logger.error("Error could not be handled: \n%s", stacktrace)
        if remaining_tries == 0: # throw error after last try
            raise StoreAccessError(str(error), 0) 
        return False
        
    @retry((Exception))
    def get_metadata(self, path):
        self.logger.debug("getting metadata for %s", path)
        self._raise_error_if_invalid_path(path)
        if path == "/": # workaraund for root metadata
            ret = {}
            ret["bytes"] = 0
            ret["modified"] = time.time()
            ret["path"] = "/"
            ret["is_dir"] = True
            return ret
        k = self.bucket.get_key(path[1:])
        if k == None:
            k = self.bucket.get_key(path[1:]+'/')
        if k == None:
            self.logger.debug('get_metadata(%s) does not exist' % path)
            raise NoSuchFilesytemObjectError('%s does not exist' % path)
        ret = {}
        ret["bytes"] = k.size
        ret["modified"] = int(time.mktime( time.strptime(k.last_modified, '%a, %d %b %Y %H:%M:%S %Z') ))
        ret["path"] = path
        ret["is_dir"] = self._is_dir(k)
        return ret
        
    def _is_dir(self, key):
        ''':param key: instance of boto's Key class
        :returns: True iff key is a directory '''
        return to_str(key.name)[-1] == '/' or to_str(key.name).endswith('_$folder$') or key.get_metadata('Content-Type') == 'application/x-directory'
    
    def _get_time_difference(self):
        self.logger.debug("getting time difference")
        return 0
    
    def get_logging_handler(self):
        return self._logging_handler
    
    def get_max_filesize(self):
        """Return maximum number of bytes per file"""
        return 1000*1000*1000*1000