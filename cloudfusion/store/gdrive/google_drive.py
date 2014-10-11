'''
Created on 08.04.2011

@author: joe
'''

import os
import time

from cloudfusion.store.store import *
import logging
from cloudfusion.util.exponential_retry import retry
from cloudfusion.mylogging import db_logging_thread
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive as Drive
import shelve
from oauth2client.client import OAuth2Credentials
import random
from apiclient import errors
import cloudfusion.third_party.parsedatetime.parsedatetime as pdt
from string import Template
from cloudfusion.util.string import get_id_key, get_secret_key

class GoogleDrive(Store):
    '''Subclass of Store implementing an interface to the Google Drive.'''
    #hardcode client configuration
    #client_id 1086226639551-skahka58ohaabuq28vnogehtjrpocu8s.apps.googleusercontent.com
    #client_secret nDbxJU06oANOqdQIACTRfaIQ
    CLIENT_AUTH_TEMPLATE = Template('{"installed":{"auth_uri":"https://accounts.google.com/o/oauth2/auth","client_secret":"$SECRET","token_uri":"https://accounts.google.com/o/oauth2/token","client_email":"","redirect_uris":["urn:ietf:wg:oauth:2.0:oob","oob"],"client_x509_cert_url":"","client_id":"$ID","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs"}}')
    
    def __init__(self, config):
        '''*config* can be obtained from the function :func:`cloudfusion.store.gdrive.google_drive.GoogleDrive.get_config`,
        but you need to add the id and secret, which can be obtained by creating an id and secret for an "Installed Application" in the developer console: 
        https://console.developers.google.com/project, as described in https://developers.google.com/drive/web/quickstart/quickstart-python::
        
            config = GoogleDrive.get_config()
            config['id'] = '4523154788555-kjsdfj87sdfjh44dfsdfj45kjj.apps.googleusercontent.com' #your id            
            config['secret'] = 'sdfjk3h5j444jnjfo0' #your secret
        
        You may add a cache id, so that you can continue previous sessions. If you use the same cache id in a later session, 
        the store will remember some metadata and won't need the id and secret for authentication (just use empty strings in this case)::
        
            config['cache_id'] = 'gdrive_db'
        
        Or you can use a configuration file that already has id and secret set by specifying a path::
        
            path_to_my_config_file = '/home/joe/gdrive.ini'       
            config = GoogleDrive.get_config(path_to_my_config_file)
        
        :param config: dictionary with key value pairs'''
        super(GoogleDrive, self).__init__()
        self.name = 'google_drive'
        self._logging_handler = self.name
        self.logger = logging.getLogger(self._logging_handler)
        self.logger = db_logging_thread.make_logger_multiprocessingsave(self.logger)
        self.logger.info("creating %s store", self.name)
        id_key = get_id_key(config)
        secret_key = get_secret_key(config)
        self.client_auth = self.CLIENT_AUTH_TEMPLATE.substitute(SECRET=config[secret_key], ID=config[id_key])
        self.gauth = self._reconnect()
        
        self._credentials_db_path = self._get_credentials_db_path(config)
        key = self._credentials_db_path# use db path as database key to store credentials under
        try:
            credentials_db = shelve.open(self._credentials_db_path)
        except Exception, e:
            self.logger.debug("Credentials database could not be loaded.")
            credentials_db = {}
        if key in credentials_db: 
            self.gauth.credentials = credentials_db[key]
        else: # get credentials manually
            self.gauth.LocalWebserverAuth()
        credentials_db[key] = self.gauth.credentials #store token for further sessions  
        try:
            credentials_db.close()
        except Exception, e:
            pass     
        self.gauth.Authorize()
        self.drive = Drive(self.gauth)
        self.logger.info("api initialized")
        
    @staticmethod
    def get_config(path_to_configfile=None):
        '''Get initial google drive configuration to initialize :class:`cloudfusion.store.gdrive.google_drive.GoogleDrive`
        :param path_to_configfile: path to a configuration file or None, which will use the default configuration file'''
        from ConfigParser import SafeConfigParser
        import cloudfusion
        config = SafeConfigParser()
        if not path_to_configfile:
            path_to_configfile = os.path.dirname(cloudfusion.__file__)+"/config/GDrive.ini"
        config_file = open(path_to_configfile, "r")
        config.readfp(config_file)
        return dict(config.items('auth'))
        
    def _get_credentials_db_path(self, config):
        dir_name = self._get_cachedir_name(config)
        ret = dir_name + "/credentials" 
        return ret 

    def _reconnect(self):
        with open('client_secrets.json', 'w') as client_secrets:
            client_secrets.write(self.client_auth)
        gauth = GoogleAuth()
        return gauth
    
    def _get_fileobject_id(self, path):
        if len(path)>0 and path[0] == '/':
            path = path[1:]
        if path == '':
            return 'root'
        cannot_be_split = len(path.rsplit('/', 1)) == 1
        if cannot_be_split:
            fileobjects = self.drive.ListFile({'q': "'root' in parents and title='%s'" % (path) }).GetList()
            if len(fileobjects) == 0:
                raise NoSuchFilesytemObjectError(path, 404)
            return fileobjects[0]['id']
        parent, title = path.rsplit('/', 1)
        #print "'%s' in parents and title='%s'" % (parent, title)
        parent_id = self._get_fileobject_id(parent)
        #print "pid:"+repr(parent_id)
        fileobjects = self.drive.ListFile({'q': "'%s' in parents and title='%s'" % (parent_id, title) }).GetList()
        if len(fileobjects) == 0:
            raise NoSuchFilesytemObjectError(path, 404)
        return fileobjects[0]['id']
        
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

    def _copy_drive(self):
        '''Create a deep copy of the GoogleAuth object,
        and the GoogleDrive object for use by another thread.
        :returns: a tuple with the copies of the GoogleAuth and the GoogelDrive object'''
        gauth = GoogleAuth()
        gauth.credentials = self.gauth.credentials
        self.gauth.credentials.authorize(httplib2.Http())
        drive = Drive(gauth)
        return (gauth, drive)
        
    def __deepcopy__(self, memo):
        from copy import deepcopy
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        (gauth, drive) = self._copy_drive()
        for k, v in self.__dict__.items():
            if k == 'logger':
                setattr(result, k, self.logger)
            elif k == '_logging_handler':
                setattr(result, k, self._logging_handler)
            elif k == 'gauth':
                self.logger.debug("copy gauth")
                setattr(result, k, gauth)
            elif k == 'drive':
                self.logger.debug("copy drive")
                setattr(result, k, drive)
            else:
                setattr(result, k, deepcopy(v, memo))
        return result
        
    def get_name(self):
        self.logger.info("getting name")
        return self.name
    
    
    def get_file(self, path_to_file): 
        self.logger.debug("getting file: %s", path_to_file)
        #self._raise_error_if_invalid_path(path_to_file)
        path_to_file = path_to_file[1:]
        file_id = self._get_fileobject_id(path_to_file)
        file = self.drive.CreateFile({'id': file_id}) # Initialize GoogleDriveFile instance with file id
        content = file.GetContentString() 
        return content
        
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
        path = path[1:]
        if self.exists(path):
            file_id = self._get_fileobject_id(path)
            file = self.drive.CreateFile({'id': file_id})
        else:
            title = os.path.basename(path)
            parent_dir = os.path.dirname(path)
            parent_id = self._get_fileobject_id(parent_dir)
            file = self.drive.CreateFile({ "parents": [{"id": parent_id}], 'title':title}) #does this work if path is a nested file? 
        file.SetContentString(fileobject.read())
        file.Upload()
        return int(time.time()) 
    
    # worst case: object still exists and takes up space or is appended to, by mistake
    # with caching_store, the entry in cache is deleted anyways 
    @retry((Exception), tries=5, delay=0) 
    def delete(self, path, is_dir=False): 
        self.logger.debug("deleting %s", path)
        try: 
            file_id = self._get_fileobject_id(path)
        except NoSuchFilesytemObjectError:
            return
        self.drive.auth.service.files().delete(fileId=file_id).execute()
        
    @retry((Exception))
    def account_info(self):
        self.logger.debug("retrieving account info")
        return "Google Drive"

    def exists(self, path):
        try:
            self._get_fileobject_id(path)
            return True
        except NoSuchFilesytemObjectError:
            return False

    @retry((Exception))
    def create_directory(self, directory):
        self.logger.debug("creating directory %s", directory)
        parent = os.path.dirname(directory)
        title = os.path.basename(directory)
        if self.exists(directory):
            raise AlreadyExistsError(directory, 0)
        new_folder = self.drive.CreateFile({'title': title})
        new_folder['parents'] = [{"id":self._get_fileobject_id(parent)}]
        new_folder['mimeType'] = "application/vnd.google-apps.folder"
        new_folder.Upload()
        
    def duplicate(self, path_to_src, path_to_dest):
        self.logger.debug("duplicating %s to %s", path_to_src, path_to_dest)
        dest_name = os.path.basename(path_to_dest)
        dest_dir  = os.path.dirname(path_to_dest)
        dest_dir_id = self._get_fileobject_id( dest_dir )
        src_id = self._get_fileobject_id(path_to_src)
        if self.is_dir(path_to_src):
            #make destination directory: (might exist)
            if not self.exists(path_to_dest):
                self.create_directory(path_to_dest)
            #copy all files from original directory:
            for item in self.drive.ListFile({'q': "'"+src_id+"' in parents and trashed=false"}).GetList():
                if item['mimeType'] == 'application/vnd.google-apps.folder':#copy all folders from original directory
                    self.duplicate(path_to_src+"/"+item['name'], path_to_dest+"/"+item['name'])
                else:
                    copied_file = {"parents": [{"id": dest_dir_id}], 'title': dest_name}
                    new_file_id = self.drive.auth.service.files().copy(fileId=item['id'], body=copied_file).execute()
        else:
            #if dest exists remove
            try:
                meta = self.get_metadata(path_to_dest)
            except Exception, e:
                meta = None
            if meta: #object exists
                resp = self.delete(path_to_dest, meta["is_dir"])
            copied_file = {"parents": [{"id": dest_dir_id}], 'title': dest_name}
            new_file_id = self.drive.auth.service.files().copy(fileId=src_id, body=copied_file).execute()
    
    def get_overall_space(self):
        self.logger.debug("retrieving all space")
        about = self.drive.auth.service.about().get().execute()
        return int(about['quotaBytesTotal'])

    def get_used_space(self):
        self.logger.debug("retrieving used space")
        about = self.drive.auth.service.about().get().execute()
        return int(about['quotaBytesUsed'])
        
    @retry((Exception))
    def get_directory_listing(self, directory):
        self.logger.debug("getting directory listing for %s", directory)
        #self._raise_error_if_invalid_path(directory)
        listing = []
        dir_id = self._get_fileobject_id(directory)
        fileobjects = self.drive.ListFile({'q': "'"+dir_id+"' in parents and trashed=false"}).GetList()
        for fileobj in fileobjects: #fileobj['title'], fileobj['id']
            listing.append(fileobj['title'])
        DELIMITER = '' if directory == '/' else '/' 
        listing = [ directory + DELIMITER + basename for basename in listing] 
        return listing
    
    def _handle_error(self, error, stacktrace, method_name, remaining_tries, *args, **kwargs):
        """Used by retry decorator to react to errors."""
        #AuthenticationError and RefreshError
        self.gauth, self.drive = self._copy_drive()
        if isinstance(error, AttributeError):
            self.logger.debug("Retrying on funny socket error: %s", error)
            #funny socket error in httplib2: AttributeError 'NoneType' object has no attribute 'makefile'
        elif isinstance(error, NoSuchFilesytemObjectError):
                self.logger.debug("Error could not be handled: %s", error)
                raise error
        #elif isinstance(error, GSResponseError) and error.status == 404:
        #    self.logger.debug('file object does not exist in %s: %s' % (method_name, str(GSResponseError)))
        #    raise NoSuchFilesytemObjectError('file object does not exist in %s: %s' % (method_name, str(GSResponseError)))
        else:
            self.logger.debug("Error is not covered by _handle_error: %s", error)
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
        file_id = self._get_fileobject_id(path)
        fileobj = self.drive.CreateFile({'id': file_id}) # Initialize GoogleDriveFile instance with file id
        if fileobj == None:
            self.logger.debug('get_metadata(%s) does not exist' % path)
            raise NoSuchFilesytemObjectError('%s does not exist' % path)
        ret = {}
        ret["bytes"] = int(fileobj['quotaBytesUsed']) 
        cal = pdt.Calendar()
        mod_date =  int(time.mktime(cal.parse(fileobj['modifiedDate'])[0]))
        ret["modified"] = mod_date
        ret["path"] = path
        ret["is_dir"] = fileobj['mimeType'] == 'application/vnd.google-apps.folder'
        return ret
        
    def _get_time_difference(self):
        self.logger.debug("getting time difference")
        return 0
    
    def get_logging_handler(self):
        return self._logging_handler
    
    def get_max_filesize(self):
        """Return maximum number of bytes per file"""
        return 1000*1000*1000*1000