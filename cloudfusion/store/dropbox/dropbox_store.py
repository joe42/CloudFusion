'''
Created on 08.04.2011

@author: joe
'''

import time
import random
from cloudfusion.dropbox import client, session
from cloudfusion.dropbox import rest
from cloudfusion.dropbox.rest import RESTSocketError
from cloudfusion.store.store import *
import logging
from cloudfusion.util.file_decorator import NameableFile
import webbrowser
import base64
import shelve
from multiprocessing import Manager
import atexit
from cloudfusion.util.exponential_retry import retry
import re
import hashlib
from cloudfusion.util.string import get_id_key, get_secret_key, to_str
'''  requests bug with requests 2.0.1, so use local requests version 1.2.3:
#    File "/usr/local/lib/python2.7/dist-packages/requests/cookies.py", line 311, in _find_no_duplicates
#    raise KeyError('name=%r, domain=%r, path=%r' % (name, domain, path))
#    KeyError: "name=Cookie(version=0, name='bang', value='QUFCQmh5c3FET1RnMUZkcXlrMXNBNXV4eFhaU080NWtzYndmUDlDa0p1SEFHZw%3D%3D', port=None, port_specified=False, domain='.dropbox.com', domain_specified=True, domain_initial_dot=False, path='/', path_specified=True, secure=False, expires=1383395405, discard=False, comment=None, comment_url=None, rest={'httponly': None}, rfc2109=False), domain=None, path=None"
'''
import cloudfusion.third_party.requests_1_2_3.requests as requests
from bs4 import BeautifulSoup
from cloudfusion.mylogging import db_logging_thread

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
    CONFLICT_OCCURRED = 409
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
        '''*config* can be obtained from the function :func:`cloudfusion.store.dropbox.dropbox_store.DropboxStore.get_config`,
        but you need to add user and password::
        
            config = DropboxStore.get_config()
            config['user'] = 'me@emailserver.com' #your account username/e-mail address
            config['password'] = 'MySecret!23$' #your account password
        
        You may add a cache id, so that you can continue previous sessions. If you use the same cache id in a later session, 
        the store will remember some metadata and does not need to rely on auto-login (since the auto-login feature often breaks because Dropbox changes their interface)::
        
            config['cache_id'] = 'dropbox_db' 
             
        You can also choose between full access to dropbox or to a single subfolder by setting the value for 'root'::
        
            config['root'] = 'dropbox' #for full dropbox access (this is the default) or 
            config['root'] = 'app_folder' #for restricted access to one subfolder
        
        Or you can use a configuration file that already has password and username set by specifying a path::
        
            path_to_my_config_file = '/home/joe/MyDropbox.ini'       
            config = DropboxStore.get_config(path_to_my_config_file)
        
        :param config: dictionary with key value pairs'''
        self._logging_handler = 'dropbox'
                #TODO: check if is filehandler
        self.logger = logging.getLogger(self._logging_handler)
        self.logger = db_logging_thread.make_logger_multiprocessingsave(self.logger)
        self.dir_listing_cache = {}
        self.logger.info("get Dropbox session")
        if not config['root'] in ['dropbox', 'app_folder']:
            raise StoreAccessError("Configuration error: root must be one of dropbox or app_folder, check your configuration file", 0)
        self._cache_dir = self._get_cachedir_name(config)
        self.create_session(config, self._cache_dir)
        self.logger.info("get DropboxClient")
        self.client = client.DropboxClient(self.sess)
        self.root = config['root']
        self.time_difference = self._get_time_difference()
        self.logger.info("api initialized")
        manager = Manager()
        self._revisions = manager.dict()
        self._revision_db_path = self._cache_dir + "/Dropbox_revisions.db"
        try:
            last_session_revisions = shelve.open(self._revision_db_path)
            self._revisions.update(last_session_revisions)
        except Exception, e:
            self.logger.debug("Revision database from last session could not be loaded.")
        self._is_copy = False
        atexit.register( lambda : self._close() )
        super(DropboxStore, self).__init__()
    
    @staticmethod
    def get_config(path_to_configfile=None):
        '''Get initial dropbox configuration to initialize :class:`cloudfusion.store.dropbox.dropbox_store.DropboxStore`
        :param path_to_configfile: path to a configuration file or None, which will use the default configuration file'''
        from ConfigParser import SafeConfigParser
        import cloudfusion
        config = SafeConfigParser()
        if not path_to_configfile:
            path_to_configfile = os.path.dirname(cloudfusion.__file__)+"/config/Dropbox.ini"
        config_file = open(path_to_configfile, "r")
        config.readfp(config_file)
        return dict(config.items('auth'))

    def _get_cachedir_name(self, config):
        if 'cache_id' in config:
            cache_id = config['cache_id'] # add actual cache_id
        else:
            cache_id = str(random.random())
        if 'cache_dir' in config:
            cache_dir = config['cache_dir']
            cache_dir = cache_dir[:-1] if cache_dir[-1:] == '/' else cache_dir # remove slash at the end
        else:
            cache_dir = os.path.expanduser("~")+'/.cache/cloudfusion'
        cache_dir = cache_dir+"/cachingstore_" + cache_id
        try:
            os.makedirs(cache_dir)
        except OSError,e: # exists
            pass
        return cache_dir
        
    def create_session(self, config, cache_dir):
        key = hashlib.sha224(config['user']+config['password']).hexdigest() #key for token from last session
        try:
            credentials_db = shelve.open(cache_dir+'/credentials', protocol=-1) # use protocol -1 since token defines slots
        except Exception, e:
            self.logger.debug("Credentials database could not be loaded.")
            credentials_db = {}
        id_key = get_id_key(config)
        secret_key = get_secret_key(config)
        self.sess = session.DropboxSession(base64.b64decode(config[id_key]), base64.b64decode(config[secret_key]), config['root'])
        if key in credentials_db:
            self.sess.token = credentials_db[key]
            return self.sess
        self.request_token = self.sess.obtain_request_token()
        url = self.sess.build_authorize_url(self.request_token)
        try:
            self._auto_connect(url, config['user'], config['password'])
        except Exception, e:
            self.logger.exception("Automatic login failed, falling back to manual login")
        access_token = self.reconnect(1)
        if not access_token:
            # Make the user sign in and authorize this token
            print "url:", url
            print "Please visit this website and press the 'Allow' button in the next Minute."
            webbrowser.open(url)
            access_token = self.reconnect()
            if not access_token:
                print "Sorry, please try copying the config file again."
                raise StoreAccessError("Authorization error", 0)
        credentials_db[key] = self.sess.token #store token for further sessions  
        try:
            credentials_db.close()
        except Exception, e:
            pass     
        
    def _auto_connect(self, authorize_url, user, password):
        headers = {'Host' : 'www.dropbox.com', 'User-Agent' : 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0', 'Accept' :  'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 'Accept-Language' : 'en-gb,en;q=0.5', 'Accept-Encoding' : 'gzip, deflate', 'DNT' : '1', 'Connection' : 'keep-alive'}
        auth_page_req = requests.get(authorize_url, headers=headers, allow_redirects=False)
        cookies = auth_page_req.cookies
        if 'location' in auth_page_req.headers:
            redirect_path = auth_page_req.headers['location']
            auth_page_req = requests.get('https://www.dropbox.com'+redirect_path, headers=headers, allow_redirects=False, cookies=cookies)
        attr={} # collect input for login form
        soup = BeautifulSoup(auth_page_req.text) #authorization page
        for input_tag in soup.form.find_all("input"):
            if input_tag.has_attr('value'):
                attr[input_tag['name']] = input_tag['value']
        attr['login_email'] = user
        attr['login_password'] = password
        token = re.search(r'"TOKEN": "(.*?)"', auth_page_req.text).group(1)
        attr['t'] = token
        attr['is_xhr'] = "true"
        attr['cont'] = ""
        login_req = requests.post('https://www.dropbox.com/ajax_login',attr, headers=headers,cookies=cookies, allow_redirects=False)
        cookies.update(login_req.cookies)
        if 'location' in login_req.headers:
            redirect_path = login_req.headers['location']
            login_req = requests.get(redirect_path, headers=headers, allow_redirects=False, cookies=cookies)
        cookies.update(login_req.cookies)
        login_req = requests.get(authorize_url, headers=headers,cookies=cookies, allow_redirects=False)
        attr = {}
        soup  = BeautifulSoup(login_req.text)
        for input_tag in soup.form.find_all("input"):
            if input_tag['name'] != 'deny_access':
                if input_tag.has_attr('value'):
                    attr[input_tag['name']] = input_tag['value']
                else:
                    attr[input_tag['name']] = ''
        attr['allow_access'] = '1'
        cookies.update(login_req.cookies)
        # acknowledge application access authorization
        res = requests.post('https://www.dropbox.com/1/oauth/authorize_submit', attr, headers=headers,cookies=cookies)

    
    def _handle_error(self, error, stacktrace, method_name, remaining_tries, *args, **kwargs):
        """Used by retry decorator to react to errors."""
        if isinstance(error, AttributeError):
            self.logger.exception("Retrying on funny socket error: %s", error)
            #funny socket error in httplib2: AttributeError 'NoneType' object has no attribute 'makefile'
        elif isinstance(error, StoreAutorizationError):
            self.logger.exception("Trying to handle authorization error by reconnecting: %s", error)
            self.reconnect()
            if remaining_tries == 0: # throw error after last try
                raise error 
        elif isinstance(error, StoreAccessError):
            if error.status == HTTP_STATUS.OVER_STORAGE_LIMIT or \
                error.status == HTTP_STATUS.TOO_MANY_ITEMS or \
                error.status == HTTP_STATUS.BAD_REQUEST or \
                error.status == HTTP_STATUS.FORBIDDEN or \
                isinstance(error, AlreadyExistsError) or \
                isinstance(error, NoSuchFilesytemObjectError):
                self.logger.exception("Error could not be handled: %s", error)
                raise error
            elif error.status == HTTP_STATUS.TOO_MANY_REQUESTS:
                self.logger.exception("Trying to handle TOO_MANY_REQUESTS error by delaying next request: %s", error)
                time.sleep(random.uniform(0.5, 5))
        else:
            if isinstance(error, RESTSocketError):
                error = Exception(str(error)) #wrap exception, as logger cannot handle socket.error exceptions
            self.logger.error("Error could not be handled: \n%s", stacktrace)
        if remaining_tries == 0: # throw error after last try
            raise StoreAccessError(str(error), 0) 
        return False
        
    def reconnect(self, tries = 20):
        access_token = None
        for i in range(0,tries):
            time.sleep(3)
            try:
                access_token = self.sess.obtain_access_token(self.request_token)
                break
            except Exception as e:
                self.logger.error("Authorization error: %s", e)
                pass
        return access_token
        
    def _close(self):
        if not self._is_copy:
            try:
                if not os.path.exists(self._cache_dir):
                    os.makedirs(self._cache_dir)
                self.myshelve = shelve.open(self._revision_db_path)
                self.myshelve.update(self._revisions)
                self.myshelve.close()
            except Exception, e: 
                import sys
                sys.stderr("Could not store Dropbox file revisions: %s" % e)
    
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
        self.logger.info("getting file: %s", path_to_file)
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
        self.logger.debug("storing file object size< 6MB to %s", path)
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
        return self._parse_filesys_obj(resp)["modified"] 
            

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
        resp_path = to_str(resp_path)
        if resp_path != path:
            if not self.exists('/overwritten'):
                self.create_directory('/overwritten')
            
            self.move(path, '/overwritten' + path)
            self.move(resp_path, path)

    # retry does not really matter with caching_store
    @retry((Exception,RESTSocketError), tries=10, delay=0.2) 
    def store_fileobject(self, fileobject, path, interrupt_event=None):
        size = self.__get_size(fileobject)
        self.logger.info("Storing file object of size %s to %s", size, path)
        remote_file_name = os.path.basename(path)
        if size < 6000000:
            return self.store_small_fileobject(fileobject, path)
        nameable_file = NameableFile( fileobject, remote_file_name )
        uploader = self.client.get_chunked_uploader(nameable_file, size)
        retry = 5
        while uploader.offset < size:
            try:
                if interrupt_event and interrupt_event.is_set():
                    self.logger.debug("terminating stale upload of %s", path)
                    raise InterruptedException("Stale upload has been interrupted.")
                resp = uploader.upload_chunked(5 * 1000 * 1000)
            except rest.ErrorResponse, e:
                retry -= 1
                if e.status == HTTP_STATUS.TOO_MANY_REQUESTS:
                    self.logger.exception("Trying to handle TOO_MANY_REQUESTS error during upload by delaying next request: %s", e)
                    time.sleep(random.uniform(0.5, 5))
                if retry == 0:
                    msg= "Could not store file: " +path+remote_file_name 
                    self._log_http_error("store_fileobject", path, resp, msg)
                    HTTP_STATUS.generate_exception(e.status, str(e))
        try:
            resp = uploader.finish(path, overwrite=False, parent_rev=self._get_revision(path))
        except rest.ErrorResponse as resp:
            if resp.status == HTTP_STATUS.CONFLICT_OCCURRED:
                # There already exists a file with the same name: 
                # This is handled by _backup_overwritten 
                pass
        except Exception, e:
            msg= "Could not store file: " +path+remote_file_name 
            self._log_http_error("store_fileobject", path, resp, msg)
            HTTP_STATUS.generate_exception(resp.status, str(resp))
        self._add_revision(path, resp['rev'])
        self._backup_overwritten(path, resp['path'])
        return self._parse_filesys_obj(resp)["modified"]
    
    
    # worst case: object still exists and takes up space or is appended to, by mistake
    # with caching_store, the entry in cache is deleted anyways 
    @retry((Exception,RESTSocketError), tries=5, delay=0) 
    def delete(self, path, is_dir=False): #is_dir parameter does not matter to dropbox
        self.logger.info("deleting %s", path)
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
        self.logger.info("creating directory %s", directory)
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
    @retry((Exception,RESTSocketError), tries=2, delay=0)
    def duplicate(self, path_to_src, path_to_dest):
        self.logger.info("duplicating %s to %s", path_to_src, path_to_dest)
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
    @retry((Exception,RESTSocketError), tries=3, delay=0.5)
    def move(self, path_to_src, path_to_dest):
        self.logger.info("moving %s to %s", path_to_src, path_to_dest)
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
        self.logger.debug("getting directory listing for %s", directory)
        self._raise_error_if_invalid_path(directory)
        dir_hash = None
        if directory in self.dir_listing_cache:
            dir_hash = self.dir_listing_cache[directory]['hash']
        try:
            resp = self.client.metadata(directory, hash=dir_hash, list=True)
        except rest.ErrorResponse as resp:
            if resp.status == HTTP_STATUS.NOT_CHANGED: 
                self.logger.debug("retrieving listing from cache %s", directory)
                ret = self.dir_listing_cache[directory]['dir_listing']
                return [to_str(path) for path in ret.keys()]
            else:
                msg= "could not get directory listing for " +directory
                self._log_http_error("get_directory_listing", None, resp, msg)
                HTTP_STATUS.generate_exception(resp.status, str(resp))
        ret = self._parse_dir_list(resp)
        self.dir_listing_cache[directory] = {}
        self.dir_listing_cache[directory]['hash'] = resp["hash"]
        self.dir_listing_cache[directory]['dir_listing'] = ret
        return [to_str(path) for path in ret.keys()]
    
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
    def get_metadata(self, path):
        self.logger.debug("getting metadata for %s", path)
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
                self._log_http_error("get_metadata", path, resp, msg)
                HTTP_STATUS.generate_exception(resp.status, str(resp))
            elif resp.status != 200:
                self._log_http_error("get_metadata", path, resp)
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
            self.logger.warning("Time conversion error: %s", data["modified"])
            raise DateParseError("Error parsing modified attribute: %s", x)
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
        












