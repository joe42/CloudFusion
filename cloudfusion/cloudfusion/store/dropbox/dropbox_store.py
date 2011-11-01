'''
Created on 08.04.2011

@author: joe
'''

import time
import datetime
from dropbox import client, auth
from cloudfusion.store.store import *
import logging
import logging.config
import os.path
from cloudfusion.store.dropbox.file_decorator import NameableFile
import tempfile
import ConfigParser
import StringIO
import cloudfusion




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
        self.logger.debug("get Authenticator")
        dba = auth.Authenticator(config)
        self.logger.debug("get access_token: for: "+config['user']+" "+ config['password'])
        access_token = dba.obtain_trusted_access_token(config['user'], config['password'])
        self.logger.debug("get DropboxClient")
        try:
            self.client = client.DropboxClient(config['server'], config['content_server'], config['port'], dba, access_token)
        except Exception, e:
            try:
                self.client = client.DropboxClient(config['server'], config['content_server'], config['port'], dba, access_token)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
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
            resp = self.client.get_file(self.root, path_to_file)
        except Exception, e:
            try:
                resp = self.client.get_file(self.root, path_to_file)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        if resp.status != 200:
            msg= "could not get file: " +path_to_file
            self._log_http_error("get_file", path_to_file, resp, msg)
            return ""
        ret = resp.read()
        return ret
    
    def store_fileobject(self, fileobject, path):
        self.logger.debug("storing file object to "+path)
        remote_file_name = os.path.basename(path)
        dest_dir = os.path.dirname(path);
        namable_file = NameableFile( fileobject, remote_file_name )
        try:
            resp = self.client.put_file(self.root, dest_dir, namable_file) 
        except Exception, e:
            try:
                resp = self.client.put_file(self.root, dest_dir, namable_file) 
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        if resp.status != 200:
            msg= "could not store file: " +dest_dir+remote_file_name 
            self._log_http_error("store_fileobject", path, resp, msg)
    
    def delete(self, path):
        self.logger.debug("deleting " +path)
        self._raise_error_if_invalid_path(path)
        try:
            resp = self.client.file_delete(self.root, path)
        except Exception, e:
            try:
                resp = self.client.file_delete(self.root, path)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        if resp.status != 200:
            msg= "could not delete " +path
            self._log_http_error("delete", path, resp, msg)
            #assert_all_in(resp.data.keys(), [u'is_deleted', u'thumb_exists', u'bytes', u'modified',u'path', u'is_dir', u'size', u'root', u'mime_type', u'icon'])
        
    def account_info(self):
        self.logger.debug("retrieving account info")
        try:
            resp =  self.client.account_info()
        except Exception, e:
            try:
                resp =  self.client.account_info()
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        if resp.status != 200:
            msg= "could not get account_info "
            self._log_http_error("delete", None, resp, msg)
            #assert_all_in(resp.data.keys(), [u'country', u'display_name', u'uid', u'quota_info'])
        return str(resp.data)

    def create_directory(self, directory):
        self.logger.debug("creating directory " +directory)
        self._raise_error_if_invalid_path(directory)
        if directory == "/":
            return
        try:
            resp = self.client.file_create_folder(self.root, directory)
        except Exception, e:
            try:
                resp = self.client.file_create_folder(self.root, directory)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        if resp.status != 200:
            msg= "could not create directory: " +directory
            self._log_http_error("create_directory", directory, resp, msg)
        #assert_all_in(resp.data.keys(), [u'thumb_exists', u'bytes', u'modified', u'path', u'is_dir', u'size', u'root', u'icon'])
        
    def duplicate(self, path_to_src, path_to_dest):
        self.logger.debug("duplicating " +path_to_src+" to "+path_to_dest)
        self._raise_error_if_invalid_path(path_to_src)
        self._raise_error_if_invalid_path(path_to_dest)
        try:
            resp = self.client.file_copy(self.root, path_to_src, path_to_dest)
        except Exception, e:
            try:
                resp = self.client.file_copy(self.root, path_to_src, path_to_dest)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        if resp.status != 200:
            msg= "could not duplicate " +path_to_src+" to "+path_to_dest
            self._log_http_error("duplicate", None, resp, msg)
        #ssert_all_in(resp.data.keys(), [u'thumb_exists', u'bytes', u'modified',u'path', u'is_dir', u'size', u'root', u'mime_type', u'icon'])
    
    def move(self, path_to_src, path_to_dest):
        self.logger.debug("moving " +path_to_src+" to "+path_to_dest)
        self._raise_error_if_invalid_path(path_to_src)
        self._raise_error_if_invalid_path(path_to_dest)
        try:
            resp = self.client.file_move(self.root, path_to_src, path_to_dest)
        except Exception, e:
            try:
                resp = self.client.file_move(self.root, path_to_src, path_to_dest)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        if resp.status != 200:
            if resp.status == 400:
                self.delete(path_to_dest)
                self.move(path_to_src, path_to_dest)
            else:
                msg= "could not move " +path_to_src+" to "+path_to_dest
                self._log_http_error("move", None, resp, msg)
    
    def get_overall_space(self):
        self.logger.debug("retrieving all space")
        try:
            resp =  self.client.account_info()
        except Exception, e:
            try:
                resp =  self.client.account_info()
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        if resp.status != 200:
            msg= "could not retrieve all space"
            self._log_http_error("move", None, resp, msg)
            return 0
        return resp.data[u'quota_info']["quota"]

    def get_used_space(self):
        self.logger.debug("retrieving used space")
        try:
            resp =  self.client.account_info()
        except Exception, e:
            try:
                resp =  self.client.account_info()
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        if resp.status != 200:
            msg= "could not retrieve used space"
            self._log_http_error("move", None, resp, msg)
            return 0
        return resp.data[u'quota_info']["shared"] + resp.data[u'quota_info']["normal"]
        
    def get_directory_listing(self, directory):
        self.logger.debug("getting directory listing for "+directory)
        self._raise_error_if_invalid_path(directory)
        hash = None
        if directory in self.dir_listing_cache:
            hash = self.dir_listing_cache[directory]['hash']
        try:
            resp = self.client.metadata(self.root, directory, hash=hash, list=True)
        except Exception, e:
            try:
                resp = self.client.metadata(self.root, directory, hash=hash, list=True)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        if resp.status != 200: 
            if resp.status == 304: 
                self.logger.debug("retrieving listing from cache " +directory)
                ret = self.dir_listing_cache[directory]['dir_listing']
            else:
                msg= "could not get directory listing for " +directory
                self._log_http_error("move", None, resp, msg)
        else:
            ret = self._parse_dir_list(resp.data)
            self.dir_listing_cache[directory] = {}
            self.dir_listing_cache[directory]['hash'] = resp.data["hash"]
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
        if hasattr(resp,"data") and 'error' in resp.data: # data does not exist, on get_file?
            log += " -- error:"+resp.data['error']
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
            resp = self.client.metadata(self.root, path, list=False)
        except Exception, e:
            try:
                resp = self.client.metadata(self.root, path, list=False)
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        object_is_deleted = 'is_deleted' in resp.data and resp.data['is_deleted']
        self.logger.info(repr(resp.data))
        if resp.status == 404 or object_is_deleted:
            msg = None
            if object_is_deleted:
                msg = "filesystem object has been deleted"
            self._log_http_error("_get_metadata", path, resp, msg)
            raise NoSuchFilesytemObjectError(path, resp.status)
        elif resp.status != 200:
            self._log_http_error("_get_metadata", path, resp)
            raise RetrieveMetadataError(path, resp.data['error'], resp.status)
        elif resp.status == 200:
            return self._parse_filesys_obj(resp.data)
    
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
        try:
            resp =  self.client.account_info()
        except Exception, e:
            try:
                resp =  self.client.account_info()
            except Exception, e:
                raise StoreAccessError("Transfer error: "+str(e), 0)
        return time.mktime( time.strptime(resp.headers['date'], "%a, %d %b %Y %H:%M:%S GMT") ) - time.time()
    
    
    def get_logging_handler(self):
        return self._logging_handler
    
        












