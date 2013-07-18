'''
Created on 08.04.2011

@author: joe
'''

import time
from cloudfusion.dropbox import client, session
from cloudfusion.dropbox import rest
from cloudfusion.dropbox.rest import RESTSocketError
from cloudfusion.store.store import *
import logging
from cloudfusion.store.dropbox.file_decorator import NameableFile
import webbrowser
import base64
import shelve
from multiprocessing import Manager
import atexit
from cloudfusion.util.exponential_retry import retry

DATABASE_DIR = '/tmp/cloudfusion'

class HTTP_STATUS(object):
    OK = range(200, 300)
    NOT_CHANGED = 304
    SERVER_ERROR = range(500,600)
    BAD_REQUEST = 400   
    AUTHORIZATION_REQUIRED = 401  
    FORBIDDEN = 403
    NOT_FOUND = 404   
    UNEXPECTED_REQUEST = 405  
    TOO_MANY_ITEMS = 406  
    TOO_MANY_REQUESTS = 503          
    OVER_STORAGE_LIMIT = 507    
    
    @staticmethod
    def generate_exception(code, msg, method_name=''):
        if code in HTTP_STATUS.OK:
            pass
        elif code == HTTP_STATUS.NOT_CHANGED:
            pass
        elif code == HTTP_STATUS.AUTHORIZATION_REQUIRED:
            raise StoreAutorizationError("StoreAutorizationError Message: "+msg+"\nStatus description: "+HTTP_STATUS.get_status_desc(code), code)
        elif code == HTTP_STATUS.OVER_STORAGE_LIMIT:
            raise StoreSpaceLimitError("StoreSpaceLimitError Message: "+msg+"\nStatus description: "+HTTP_STATUS.get_status_desc(code), code)
        elif code == HTTP_STATUS.NOT_FOUND:
            raise NoSuchFilesytemObjectError("NoSuchFilesytemObjectError Message: "+msg+"\nStatus description: "+HTTP_STATUS.get_status_desc(code), code)
        elif (method_name == 'create_directory' and code == HTTP_STATUS.FORBIDDEN):
                raise AlreadyExistsError("Directory does already existt:" +msg, code)
        else:
            raise StoreAccessError("StoreAccessError Message: "+msg+"\nStatus description: "+HTTP_STATUS.get_status_desc(code), code)
            
    @staticmethod
    def get_status_desc(code):
        if code in HTTP_STATUS.OK:
            return "Request was successful."
        elif code == HTTP_STATUS.NOT_CHANGED:
            return "The folder contents have not changed (relies on hash parameter)."
        elif code == HTTP_STATUS.TOO_MANY_ITEMS:
            return "There are too many file entries to return. Or too many files and directories involved in the operation."
        elif code == HTTP_STATUS.TOO_MANY_REQUESTS:
            return "Your app is making too many requests and is being rate limited. 503s can trigger on a per-app or per-user basis.\n\
            The limit is currently 10.000 files and folders."
        elif code == HTTP_STATUS.OVER_STORAGE_LIMIT:
            return "User is over Dropbox storage quota."
        elif code == HTTP_STATUS.SERVER_ERROR:
            return "A server error occured. Check DropboxOps."
        elif code == HTTP_STATUS.UNEXPECTED_REQUEST:
            return "Request method not expected (generally should be GET or POST)."
        elif code == HTTP_STATUS.BAD_REQUEST:
            return "Bad input parameter. Error message should indicate which one and why. \n\
            Or, the file extension is on Dropbox's ignore list (e.g. thumbs.db or .ds_store).\n\
            Or, the request does not contain an upload_id or if there is no chunked upload matching the given upload_id."
        elif code == HTTP_STATUS.AUTHORIZATION_REQUIRED:
            return "Bad or expired token. This can happen if the user or Dropbox revoked or expired an access token. To fix, you should re-authenticate the user."
        elif code == HTTP_STATUS.FORBIDDEN:
            return "Bad OAuth request (wrong consumer key, bad nonce, expired timestamp...). Unfortunately, re-authenticating the user won't help here.\n\
            Or, an invalid copy/move operation was attempted (e.g. there is already a file at the given destination, or trying to copy a shared folder)."
        elif code == HTTP_STATUS.NOT_FOUND:
            return "File or folder not found at the specified path." 
        else:
            return "No error description available."

class DropboxStore(Store):
    def __init__(self, config):
        self._logging_handler = 'dropbox'
                #TODO: check if is filehandler
        self.logger = logging.getLogger(self._logging_handler)
        self.dir_listing_cache = {}
        self.logger.debug("get Dropbox session")
        if not config['root'] in ['dropbox', 'app_folder']:
            raise StoreAccessError("Configuration error: root must be one of dropbox or app_folder, check your configuration file", 0)
        self.sess = session.DropboxSession(base64.b64decode(config['consumer_key']),base64.b64decode(config['consumer_secret']), config['root'])
        self.request_token = self.sess.obtain_request_token()
        url = self.sess.build_authorize_url(self.request_token)
        # Make the user sign in and authorize this token
        print "url:", url
        print "Please visit this website and press the 'Allow' button in the next Minute."
        webbrowser.open(url)
        access_token = self.reconnect()
        if not access_token:
            print "Sorry, please try copying the config file again."
            raise StoreAccessError("Authorization error", 0)
        self.logger.debug("get DropboxClient")
        self.client = client.DropboxClient(self.sess)
        self.root = config['root']
        self.time_difference = self._get_time_difference()
        self.logger.info("api initialized")
        manager = Manager()
        self._revisions = manager.dict()
        cache_id = "" # add actual cache_id
        self._revision_db_dir = "/tmp/cloudfusion/cachingstore_"+cache_id
        self._revision_db_path = self._revision_db_dir + "Dropbox_revisions.db"
        try:
            last_session_revisions = shelve.open(self._revision_db_path)
            self._revisions.update(last_session_revisions)
        except:
            self.logger.debug("Revision database from last session could not be loaded.")
        self._is_copy = False
        atexit.register( lambda : self._close() )
        super(DropboxStore, self).__init__()
        
    
    def _handle_error(self, error, method_name, *args, **kwargs):
        """Used by retry decorator to react to errors."""
        if isinstance(error, AttributeError):
            self.logger.debug("Retrying on funny socket error: "+str(error) )
            #funny socket error in httplib2: AttributeError 'NoneType' object has no attribute 'makefile'
        elif isinstance(error, StoreAutorizationError):
            self.logger.debug("Trying to handle authorization error by reconnecting: "+str(error) )
            self.reconnect()
        elif isinstance(error, StoreAccessError):
            if error.status == HTTP_STATUS.OVER_STORAGE_LIMIT or \
                error.status == HTTP_STATUS.TOO_MANY_ITEMS or \
                error.status == HTTP_STATUS.BAD_REQUEST or \
                error.status == HTTP_STATUS.FORBIDDEN or \
                isinstance(error, AlreadyExistsError) or \
                isinstance(error, NoSuchFilesytemObjectError):
                self.logger.debug("Error could not be handled: " + str(error) )
                raise error
        else:
            self.logger.debug("Error is not covered by _handle_error: "+str(error))
        return False
        
    def reconnect(self):
        access_token = None
        for i in range(0,20):
            time.sleep(3)
            try:
                access_token = self.sess.obtain_access_token(self.request_token)
                break
            except Exception as e:
                self.logger.error("Authorization error: "+str(e))
                pass
        return access_token
        
    def _close(self):
        if not self._is_copy:
            try:
                os.makedirs(self._revision_db_dir)
            except Exception, e: 
                self.logger.error("Could not create directory to store revisions: "+str(e))
            self.myshelve = shelve.open(self._revision_db_path)
            self.myshelve.update(self._revisions)
            self.myshelve.close()
    
    def __deepcopy__(self, memo):
        from copy import deepcopy
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k == '_revisions':
                setattr(result, k, self._revisions)
            elif k == '_is_copy':
                setattr(result, k, True)
            elif k == 'logger':
                setattr(result, k, self.logger)
            elif k == '_logging_handler':
                setattr(result, k, self._logging_handler)
            else:
                setattr(result, k, deepcopy(v, memo))
        return result
        
    def get_name(self):
        self.logger.info("getting name")
        return "Dropbox"
    
    @retry((Exception,RESTSocketError))
    def get_file(self, path_to_file): 
        self.logger.debug("getting file: " +path_to_file)
        self._raise_error_if_invalid_path(path_to_file)
        try:
            data, metadata = self.client.get_file_and_metadata(path_to_file)
        except rest.ErrorResponse as resp:
            msg= "Could not get file: " +path_to_file
            self._log_http_error("get_file", path_to_file, resp, msg)
            HTTP_STATUS.generate_exception(resp.status, str(resp))
        self._add_revision(path_to_file, metadata['rev'])
        return data.read()
    
    def _get_revision(self, path):
        return self._revisions.get(path, None)
    
    def _add_revision(self, path, revision):
        self._revisions[path] = revision
        
    def _remove_revision(self, path):
        if path in self._revisions:
            del self._revisions[path]
    
    def store_small_fileobject(self, fileobject, path):
        self.logger.debug("storing file object size< 100000 to "+path)
        remote_file_name = os.path.basename(path)
        namable_file = NameableFile( fileobject, remote_file_name )
        try:
            resp = self.client.put_file(path, namable_file, overwrite=False, parent_rev=self._get_revision(path))
        except rest.ErrorResponse as resp:
            msg= "Could not store file: " +path+remote_file_name
            self._log_http_error("store_fileobject", path, resp, msg)
            HTTP_STATUS.generate_exception(resp.status, str(resp))
        self._add_revision(path, resp['rev'])
        self._backup_overwritten(path, resp['path'])  
        return resp 
            

    def __get_size(self, fileobject):
        pos = fileobject.tell()
        fileobject.seek(0,2)
        size = fileobject.tell()
        fileobject.seek(pos, 0)
        return size
    

    def _backup_overwritten(self, path, resp_path):
        """Store *path* in the directory '/overwritten' and rename the new file *resp_path* to *path*.
        If a new file is stored to path, the response from dropbox may say it was stored in to resp_path, instead.
        This means there has already been a file stored to path and instead of overwriting it, the new file was stored to resp_path, instead.
        """
        while resp_path != path:
            if not self.exists('/overwritten'):
                self.create_directory('/overwritten')
            self.move(path, '/overwritten' + path)
            self.move(resp_path, path)

    # retry does not really matter with caching_store
    @retry((Exception,RESTSocketError), tries=1, delay=0) 
    def store_fileobject(self, fileobject, path):
        size = self.__get_size(fileobject)
        self.logger.debug("Storing file object of size %s to %s" % (size,path))
        remote_file_name = os.path.basename(path)
        if size < 1000000:
            return self.store_small_fileobject(fileobject, path)
        nameable_file = NameableFile( fileobject, remote_file_name )
        uploader = self.client.get_chunked_uploader(nameable_file, size)
        retry = 5
        while uploader.offset < size:
            try:
                resp = uploader.upload_chunked(1 * 1000 * 1000)
            except rest.ErrorResponse, e:
                retry -= 1
                if retry == 0:
                    msg= "Could not store file: " +path+remote_file_name 
                    self._log_http_error("store_fileobject", path, resp, msg)
                    HTTP_STATUS.generate_exception(e.status, str(e))
        try:
            resp = uploader.finish(path, overwrite=False, parent_rev=self._get_revision(path))
        except Exception, e:
            try:
                resp = uploader.finish(path, overwrite=False, parent_rev=self._get_revision(path))
            except rest.ErrorResponse as resp:
                msg= "Could not store file: " +path+remote_file_name 
                self._log_http_error("store_fileobject", path, resp, msg)
                HTTP_STATUS.generate_exception(resp.status, str(resp))
        self._add_revision(path, resp['rev'])
        self._backup_overwritten(path, resp['path'])
        return self._parse_filesys_obj(resp) #return metadata
    
    
    # worst case: object still exists and takes up space or is appended to, by mistake
    # with caching_store, the entry in cache is deleted anyways 
    @retry((Exception,RESTSocketError), tries=5, delay=0) 
    def delete(self, path):
        self.logger.debug("deleting " +path)
        self._raise_error_if_invalid_path(path)
        try:
            resp = self.client.file_delete(path)
        except rest.ErrorResponse as resp:
            msg= "could not delete " +path
            self._log_http_error("delete", path, resp, msg)
            if not resp.status == HTTP_STATUS.NOT_FOUND:
                HTTP_STATUS.generate_exception(resp.status, str(resp))
        self._remove_revision(path)
        
    @retry((Exception,RESTSocketError))
    def account_info(self):
        self.logger.debug("retrieving account info")
        try:
            resp =  self.client.account_info()
        except rest.ErrorResponse as resp:
            msg= "could not get account_info "
            self._log_http_error("delete", None, resp, msg)
            HTTP_STATUS.generate_exception(resp.status, str(resp))
        return str(resp)

    @retry((Exception,RESTSocketError))
    def create_directory(self, directory):
        self.logger.debug("creating directory " +directory)
        self._raise_error_if_invalid_path(directory)
        if directory == "/":
            return
        try:
            resp = self.client.file_create_folder(directory)
        except rest.ErrorResponse as resp:
            msg= "could not create directory: " +directory
            self._log_http_error("create_directory", directory, resp, msg)
            HTTP_STATUS.generate_exception(resp.status, str(resp), "create_directory")
        
    # worst case: should happen mostly with user interaction, so fast feedback is more important
    @retry((Exception,RESTSocketError), tries=1, delay=0)
    def duplicate(self, path_to_src, path_to_dest):
        self.logger.debug("duplicating " +path_to_src+" to "+path_to_dest)
        self._raise_error_if_invalid_path(path_to_src)
        self._raise_error_if_invalid_path(path_to_dest)
        try:
            resp = self.client.file_copy(path_to_src, path_to_dest)
        except rest.ErrorResponse as resp:
            msg= "could not duplicate " +path_to_src+" to "+path_to_dest
            self._log_http_error("duplicate", None, resp, msg)
            HTTP_STATUS.generate_exception(resp.status, str(resp))
        self._add_revision(path_to_dest, resp['rev'])
    
    # worst case: should happen mostly with user interaction, so fast feedback is more important
    @retry((Exception,RESTSocketError), tries=1, delay=0)
    def move(self, path_to_src, path_to_dest):
        self.logger.debug("moving " +path_to_src+" to "+path_to_dest)
        self._raise_error_if_invalid_path(path_to_src)
        self._raise_error_if_invalid_path(path_to_dest)
        try:
            try:
                resp = self.client.file_move(path_to_src, path_to_dest)
            except rest.ErrorResponse as resp: # catch error "destination exists"
                if resp.status == HTTP_STATUS.FORBIDDEN:
                    self.delete(path_to_dest)
                    resp = self.client.file_move(path_to_src, path_to_dest)
                else:
                    raise
        except rest.ErrorResponse as resp:
            msg= "could not move " +path_to_src+" to "+path_to_dest
            self._log_http_error("move", None, resp, msg)
            HTTP_STATUS.generate_exception(resp.status, str(resp))
        self._remove_revision(path_to_src)
        self._add_revision(path_to_dest, resp['rev'])
    
    @retry((Exception,RESTSocketError))
    def get_overall_space(self):
        self.logger.debug("retrieving all space")
        try:
            resp =  self.client.account_info()
        except rest.ErrorResponse as resp:
            msg= "could not retrieve all space"
            self._log_http_error("get_overall_space", None, resp, msg)
            HTTP_STATUS.generate_exception(resp.status, str(resp))
        return resp[u'quota_info']["quota"]

    @retry((Exception,RESTSocketError))
    def get_used_space(self):
        self.logger.debug("retrieving used space")
        try:
            resp =  self.client.account_info()
        except rest.ErrorResponse as resp:
            msg= "could not retrieve used space"
            self._log_http_error("get_used_space", None, resp, msg)
            HTTP_STATUS.generate_exception(resp.status, str(resp))
        return resp[u'quota_info']["shared"] + resp[u'quota_info']["normal"]
        
    @retry((Exception,RESTSocketError))
    def get_directory_listing(self, directory):
        self.logger.debug("getting directory listing for "+directory)
        self._raise_error_if_invalid_path(directory)
        dir_hash = None
        if directory in self.dir_listing_cache:
            dir_hash = self.dir_listing_cache[directory]['hash']
        try:
            resp = self.client.metadata(directory, hash=dir_hash, list=True)
        except rest.ErrorResponse as resp:
            if resp.status == HTTP_STATUS.NOT_CHANGED: 
                self.logger.debug("retrieving listing from cache " +directory)
                ret = self.dir_listing_cache[directory]['dir_listing']
                return ret.keys()
            else:
                msg= "could not get directory listing for " +directory
                self._log_http_error("get_directory_listing", None, resp, msg)
                HTTP_STATUS.generate_exception(resp.status, str(resp))
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
        self.logger.error(log)
        
    @retry((Exception,RESTSocketError))
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
            resp = self.client.metadata(path, list=False) #optimize to return & cache direcory listing
        except rest.ErrorResponse as resp:
            if resp.status == HTTP_STATUS.NOT_FOUND:
                msg = None
                self._log_http_error("_get_metadata", path, resp, msg)
                HTTP_STATUS.generate_exception(resp.status, str(resp))
            elif resp.status != 200:
                self._log_http_error("_get_metadata", path, resp)
                HTTP_STATUS.generate_exception(resp.status, str(resp))
        object_is_deleted = 'is_deleted' in resp and resp['is_deleted']
        if object_is_deleted:
            msg = "filesystem object has been deleted"
            raise NoSuchFilesytemObjectError(path, 0)
        self.logger.info(repr(resp))
        if not resp["is_dir"]: # TODO: cache folder entries
            self._add_revision(path, resp['rev'])
        return self._parse_filesys_obj(resp)
    
    def _parse_filesys_obj(self, data):
        #OverflowError or ValueError
        ret = {}
        ret["bytes"] = data["bytes"]
        try:
            ret["modified"] = int(time.mktime( time.strptime(data["modified"], "%a, %d %b %Y %H:%M:%S +0000") ) - self.time_difference)
        except Exception as x:
            self.logger.warning("Time conversion error: %s" % str(data["modified"]))
            raise DateParseError("Error parsing modified attribute: %s" % str(x));
        ret["path"] = data["path"]
        ret["is_dir"] = data["is_dir"]
        return ret;
        
    def _get_time_difference(self):
        self.logger.debug("getting time difference")
        return 0
    
    
    def get_logging_handler(self):
        return self._logging_handler
    
    def get_max_filesize(self):
        """Return maximum number of bytes per file"""
        return 150*1000*1000
        












