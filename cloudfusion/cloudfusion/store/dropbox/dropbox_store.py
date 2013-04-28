'''
Created on 08.04.2011

@author: joe
'''

import time
import datetime
from cloudfusion.dropbox import client, session
from cloudfusion.dropbox import rest
from cloudfusion.store.store import *
import logging
import logging.config
import os.path
from cloudfusion.store.dropbox.file_decorator import NameableFile
import tempfile
import ConfigParser
import StringIO
import cloudfusion
import webbrowser
from cloudfusion.store.dropbox import file_decorator




class ServerError(StoreAccessError):
    def __init__(self, msg):
        super(ServerError, self).__init__(msg) 
class DropboxError(object):
    def __init__(self, status, operation_name):
        if status == 507:
            msg = "User over quota."
        elif status == 503:
            msg = "Too many requests."
        elif status == 403:
            msg = "Operation forbidden (path exists, wrong token, expired timestamp?)."
        super(ServerError, self).__init__(msg) 



class DropboxStore(Store):
    def __init__(self, config):
        self._logging_handler = 'dropbox'
                #TODO: check if is filehandler
        self.logger = logging.getLogger(self._logging_handler)
        self.dir_listing_cache = {}
        self.logger.debug("get Dropbox session")
        if not config['root'] in ['dropbox', 'app_folder']:
            raise StoreAccessError("Configuration error: root must be one of dropbox or app_folder, check your configuration file", 0)
        sess = session.DropboxSession(config['consumer_key'], config['consumer_secret'], config['root'])
        request_token = sess.obtain_request_token()
        url = sess.build_authorize_url(request_token)
        # Make the user sign in and authorize this token
        print "url:", url
        print "Please visit this website and press the 'Allow' button in the next Minute."
        webbrowser.open(url)
        access_token = None
        error = None
        for i in range(0,20):
            time.sleep(3)
            try:
                access_token = sess.obtain_access_token(request_token)
                break
            except Exception as e:
                error = e
                pass
        if not access_token:
            print "Sorry, please try copying the config file again."
            raise StoreAccessError("Authorization error: "+str(error), 0)
        self.logger.debug("get DropboxClient")
        self.client = client.DropboxClient(sess)
        self.root = config['root']
        self.time_difference = self._get_time_difference()
        self.logger.info("api initialized")
        super(DropboxStore, self).__init__() 
        
    def get_name(self):
        self.logger.info("getting name")
        return "Dropbox"
    
    def get_file(self, path_to_file): 
        self.logger.debug("getting file: " +path_to_file)
        self._raise_error_if_invalid_path(path_to_file)
        try:
            resp = self.client.get_file(path_to_file)
        except Exception, e:
            try:
                resp = self.client.get_file(path_to_file)
            except rest.ErrorResponse as resp:
                msg= "could not get file: " +path_to_file
                self._log_http_error("get_file", path_to_file, resp, msg)
                return ""
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        ret = resp.read()
        return ret
    
    def store_small_fileobject(self, fileobject, path):
        self.logger.debug("storing file object to "+path)
        remote_file_name = os.path.basename(path)
        namable_file = NameableFile( fileobject, remote_file_name )
        try:
            resp = self.client.put_file(path, namable_file)
        except Exception, e:
            try:
                resp = self.client.put_file(path, namable_file)
            except rest.ErrorResponse as resp:
                msg= "could not store file: " +path+remote_file_name
                self._log_http_error("store_fileobject", path, resp, msg)
                raise StoreAccessError("Transfer error: "+str(e), 0)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)   
        return resp 
            

    def __get_size(self, fileobject):
        pos = fileobject.tell()
        fileobject.seek(0,2)
        size = fileobject.tell()
        fileobject.seek(pos, 0)
        return size
    
    def store_fileobject(self, fileobject, path):
        self.logger.debug("storing file object to "+path)
        remote_file_name = os.path.basename(path)
        size = self.__get_size(fileobject)
        if size < 100000:
            return self.store_small_fileobject(fileobject, path)
        nameable_file = NameableFile( fileobject, remote_file_name )
        uploader = self.client.get_chunked_uploader(nameable_file, size)
        retry = 5
        while uploader.offset < size:
            try:
                upload = uploader.upload_chunked()
            except rest.ErrorResponse, e:
                retry -= 1
                if retry == 0:
                    msg= "could not store file: " +path+remote_file_name 
                    self._log_http_error("store_fileobject", path, resp, msg)
                    raise StoreAccessError("Transfer error: "+str(e), 0)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        try:
            resp = uploader.finish(path, overwrite=True)
        except Exception, e:
            try:
                resp = uploader.finish(path, overwrite=True)
            except rest.ErrorResponse as resp:
                msg= "could not store file: " +path+remote_file_name 
                self._log_http_error("store_fileobject", path, resp, msg)
                raise StoreAccessError("Transfer error: "+str(e), 0)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        return resp #return metadata
    
    
    def delete(self, path):
        self.logger.debug("deleting " +path)
        self._raise_error_if_invalid_path(path)
        remote_file_name = os.path.basename(path)
        dest_dir = os.path.dirname(path);
        empty_file = NameableFile( file_decorator.DataFileWrapper(""), remote_file_name )
        try:
          #  if not self.is_dir(path):
                #self.client.put_file('/'+dest_dir, empty_file) 
            resp = self.client.file_delete(path)
        except Exception, e:
            try:
                #if not self.is_dir(path):
                    #self.client.put_file('/'+dest_dir, empty_file) 
                resp = self.client.file_delete(path)
            except rest.ErrorResponse as resp:
                msg= "could not delete " +path
                self._log_http_error("delete", path, resp, msg)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
            #assert_all_in(resp.data.keys(), [u'is_deleted', u'thumb_exists', u'bytes', u'modified',u'path', u'is_dir', u'size', u'root', u'mime_type', u'icon'])
        
    def account_info(self):
        self.logger.debug("retrieving account info")
        try:
            resp =  self.client.account_info()
        except Exception, e:
            try:
                resp =  self.client.account_info()
            except rest.ErrorResponse as resp:
                msg= "could not get account_info "
                self._log_http_error("delete", None, resp, msg)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
            #assert_all_in(resp.data.keys(), [u'country', u'display_name', u'uid', u'quota_info'])
        return str(resp)

    def create_directory(self, directory):
        self.logger.debug("creating directory " +directory)
        self._raise_error_if_invalid_path(directory)
        if directory == "/":
            return
        try:
            resp = self.client.file_create_folder(directory)
        except Exception, e:
            try:
                resp = self.client.file_create_folder(directory)
            except rest.ErrorResponse as resp:
                msg= "could not create directory: " +directory
                self._log_http_error("create_directory", directory, resp, msg)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        #assert_all_in(resp.data.keys(), [u'thumb_exists', u'bytes', u'modified', u'path', u'is_dir', u'size', u'root', u'icon'])
        
    def duplicate(self, path_to_src, path_to_dest):
        self.logger.debug("duplicating " +path_to_src+" to "+path_to_dest)
        self._raise_error_if_invalid_path(path_to_src)
        self._raise_error_if_invalid_path(path_to_dest)
        try:
            resp = self.client.file_copy(path_to_src, path_to_dest)
        except Exception, e:
            try:
                resp = self.client.file_copy(path_to_src, path_to_dest)
            except rest.ErrorResponse as resp:
                msg= "could not duplicate " +path_to_src+" to "+path_to_dest
                self._log_http_error("duplicate", None, resp, msg)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        #ssert_all_in(resp.data.keys(), [u'thumb_exists', u'bytes', u'modified',u'path', u'is_dir', u'size', u'root', u'mime_type', u'icon'])
    
    def move(self, path_to_src, path_to_dest):
        self.logger.debug("moving " +path_to_src+" to "+path_to_dest)
        self._raise_error_if_invalid_path(path_to_src)
        self._raise_error_if_invalid_path(path_to_dest)
        try:
            resp = self.client.file_move(path_to_src, path_to_dest)
        except Exception, e:
            try:
                resp = self.client.file_move(path_to_src, path_to_dest)
            except rest.ErrorResponse as resp:
                if resp.status == 400:
                    self.delete(path_to_dest)
                    self.move(path_to_src, path_to_dest)
                else:
                    msg= "could not move " +path_to_src+" to "+path_to_dest
                    self._log_http_error("move", None, resp, msg)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
    
    def get_overall_space(self):
        self.logger.debug("retrieving all space")
        try:
            resp =  self.client.account_info()
        except Exception, e:
            try:
                resp =  self.client.account_info()
            except rest.ErrorResponse as resp:
                msg= "could not retrieve all space"
                self._log_http_error("move", None, resp, msg)
                return 0
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        return resp[u'quota_info']["quota"]

    def get_used_space(self):
        self.logger.debug("retrieving used space")
        try:
            resp =  self.client.account_info()
        except Exception, e:
            try:
                resp =  self.client.account_info()
            except rest.ErrorResponse as resp:
                msg= "could not retrieve used space"
                self._log_http_error("move", None, resp, msg)
                return 0
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        return resp[u'quota_info']["shared"] + resp[u'quota_info']["normal"]
        
    def get_directory_listing(self, directory):
        self.logger.debug("getting directory listing for "+directory)
        self._raise_error_if_invalid_path(directory)
        hash = None
        if directory in self.dir_listing_cache:
            hash = self.dir_listing_cache[directory]['hash']
        try:
            resp = self.client.metadata(directory, hash=hash, list=True)
        except Exception, e:
            try:
                resp = self.client.metadata(directory, hash=hash, list=True)
            except rest.ErrorResponse as resp:
                if resp.status == 304: 
                    self.logger.debug("retrieving listing from cache " +directory)
                    ret = self.dir_listing_cache[directory]['dir_listing']
                    return ret
                else:
                    msg= "could not get directory listing for " +directory
                    self._log_http_error("get_directory_listing", None, resp, msg)
                    raise StoreAccessError("Transfer error: "+str(e), 0)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        ret = self._parse_dir_list(resp)
        self.dir_listing_cache[directory] = {}
        self.dir_listing_cache[directory]['hash'] = resp["hash"]
        self.dir_listing_cache[directory]['dir_listing'] = ret
        return ret.keys()
    
    def _parse_dir_list(self, data):
        #OverflowError or ValueError
        ret = {}
        for obj in data["contents"]:
            file_sys_obj = self._parse_filesys_obj(obj)
            path = file_sys_obj['path']
            ret[path] = file_sys_obj
        return ret
      
    def _log_http_error(self, method_name, path, resp, msg = None):
        log = method_name + " failed with status: "+str(resp.status)
        if hasattr(resp,"data") and 'error' in resp.body: # data does not exist, on get_file?
            log += " -- error:"+resp.body['error']
        else:
            log += " -- reason: "+str(resp.reason) 
        if msg:
            log += " -- msg:"+msg
        self.logger.warn(log)
        
    def _get_metadata(self, path):
        self.logger.debug("getting metadata for "+path)
        self._raise_error_if_invalid_path(path)
        if path == "/": # workaraund for root metadata
            ret = {}
            ret["bytes"] = 0
            ret["modified"] = time.time()
            ret["path"] = "/"
            ret["is_dir"] = True
            return ret;

        try:
            resp = self.client.metadata(path, list=False)
        except Exception, e:
            try:
                resp = self.client.metadata(path, list=False)
            except rest.ErrorResponse as resp:
                if resp.status == 404:
                    msg = None
                    self._log_http_error("_get_metadata", path, resp, msg)
                    raise NoSuchFilesytemObjectError(path, resp.status)
                elif resp.status != 200:
                    self._log_http_error("_get_metadata", path, resp)
                    raise RetrieveMetadataError(path, resp.body['error'], resp.status)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        object_is_deleted = 'is_deleted' in resp and resp['is_deleted']
        if object_is_deleted:
            msg = "filesystem object has been deleted"
            raise NoSuchFilesytemObjectError(path, 0)
        self.logger.info(repr(resp))
        return self._parse_filesys_obj(resp)
    
    def _parse_filesys_obj(self, data):
        #OverflowError or ValueError
        ret = {}
        ret["bytes"] = data["bytes"]
        try:
            ret["modified"] = int(time.mktime( time.strptime(data["modified"], "%a, %d %b %Y %H:%M:%S +0000") ) - self.time_difference)
        except Exception as x:
            self.logger.warn("Time conversion error: %s" % str(data["modified"]))
            raise DateParseError("Error parsing modified attribute: %s" % str(x));
        ret["path"] = data["path"]
        ret["is_dir"] = data["is_dir"]
        return ret;
        
    def _handleError(self, status):
        pass;
    
    def _get_time_difference(self):
        self.logger.debug("getting time difference")
        return 0
    
    
    def get_logging_handler(self):
        return self._logging_handler
    
        












