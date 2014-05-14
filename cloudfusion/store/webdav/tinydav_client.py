from cloudfusion.store.store import NoSuchFilesytemObjectError, StoreAccessError,\
    AlreadyExistsError
import time
import cloudfusion.third_party.parsedatetime.parsedatetime as pdt
from tinydav import WebDAVClient, WebDAVResponse
from httplib import HTTPResponse
from tinydav.exception import HTTPServerError, HTTPUserError
import re
from cloudfusion.util.exponential_retry import retry
from bs4 import BeautifulSoup
from xml.etree import ElementTree
import logging
try:
    from urllib3 import quote
except:
    try:
        from urllib2 import quote
    except:
        from urllib import quote

class TinyDAVClient(object):
    '''A parital WebDAV client implementation based on tinydav, since cadaver v0.23.3 cannot handle 
    the response of https://webdav.4shared.com, which sends properties without a namespace.'''


    def __init__(self, url, user, pwd):
        '''
        Create a WebDAV client connecting to the WebDAV server at *url* with the credentials username and a password.
        :param url: the WebDAV server url; i.e. https://webdav.4shared.com
        :param user: the username
        :param pwd: the password
        '''
        self.url = url
        self.user = user
        self.pwd = pwd
        self.root = ''
        self.port = 80
        #parse url (port)
        if self.url.lower().startswith('https://'):
            self.port = 443
            self.url = self.url[len('https://'):]
        elif self.url.lower().startswith('http://'):
            self.url = self.url[len('http://'):]
        if len(self.url.split('/',1)) > 1:       #"webdav.mediencenter.t-online.de/sdf/2".split('/',1) -> ['webdav.mediencenter.t-online.de', 'sdf/2']
            self.root = '/'+self.url.split('/',1)[1] 
            self.url = self.url.split('/',1)[0]
        if len(self.url.split(':',1)) > 1:       #"webdav.mediencenter.t-online.de:23".split(':',1) -> ['webdav.mediencenter.t-online.de', '23']
            self.port = int(self.url.split(':',1)[1]) 
            self.url = self.url.split(':',1)[0]
        self.name = 'webdav'
        self._logging_handler = self.name
        self.logger = logging.getLogger(self._logging_handler)
    
    def _handle_error(self, error, method_name, remaining_tries, *args, **kwargs):
        if isinstance(error, HTTPUserError):
            http_response = error.response.response # error.response is HTTPResponse or a WebDAVResponse, and error.response.response is a httplib.HTTPResponse
            if http_response.status == 404:               #'WebDAVResponse' object has no attribute 'status
                raise NoSuchFilesytemObjectError(http_response.reason, http_response.status)
            elif http_response.status == 405 or http_response.status == 409:#405 is method not allowed; 4shared responds with 409 (Conflict) if directory already exists
                raise AlreadyExistsError(http_response.reason, http_response.status)
        if isinstance(error, HTTPServerError):
            self.logger.error("Error could not be handled: %s", error)
            raise StoreAccessError(error,0) # do not retry (error cannot be handled)
        if remaining_tries == 0: # throw error after last try 
            raise StoreAccessError(str(error), 0) 
        return False
        
    @retry((Exception), tries=1, delay=0)
    def get_metadata(self, path):
        ''':raises: StoreAccessError if propfind does not return getcontentlength or getlastmodified property
        :raises: NoSuchFilesytemObjectError if path does not exist'''
        response = self._get_client().propfind(self.root + path, depth=0)
        response_soup = BeautifulSoup(response.content)
        response = response_soup.find(re.compile(r'(?i)[a-z0-9]:response'))
        ret = {} 
        ret["path"] = response.find(re.compile(r'(?i)[a-z0-9]:href')).text
        mod_date = response.find(re.compile(r'(?i)[a-z0-9]:getlastmodified')).text
        cal = pdt.Calendar()
        mod_date =  int(time.mktime(cal.parse(mod_date)[0]))
        ret["modified"] = mod_date
        resource_type = response.find(re.compile(r'(?i)[a-z0-9]:resourcetype'))
        if resource_type.findChild() == None:
            ret["is_dir"] = False #GMX Mediacenter does not necessarily return a type in resourcetype tag, so we just assume it is a file
        else: 
            ret["is_dir"] = resource_type.findChild().name.split(':')[-1] == 'collection'
        if not ret["is_dir"]:
            ret["bytes"] = int(response.find(re.compile(r'(?i)[a-z0-9]:getcontentlength')).text)
        else:
            ret["bytes"] = 0
        if not ( 'is_dir' in ret and 'bytes' in ret and 'modified' in ret):
            raise StoreAccessError("Error in get_metadata(%s): \n no getcontentlength or getlastmodified property in %s" % (path, response))

        return ret 
    
    @retry((Exception), tries=1, delay=0)
    def get_overall_space(self):
        response = self._get_client().propfind(self.root, depth=0)
        response_soup = BeautifulSoup(response.content)
        response = response_soup.find(re.compile(r'(?i)[a-z0-9]:response'))
        ret = response.find(re.compile(r'(?i)[a-z0-9]:quota-available-bytes')).text
        if ret:
            return int(ret)
        return 1000000000
    
    @retry((Exception), tries=1, delay=0)
    def get_used_space(self):
        ret = 0
        responses = self._get_client().propfind(self.root, depth=1)
        for status in responses:
            response_soup = BeautifulSoup(ElementTree.tostring(status.response, 'utf8'))
            response = response_soup.find(re.compile(r'(?i)[a-z0-9]:response'))
            size = response.find(re.compile(r'(?i)[a-z0-9]:getcontentlength'))
            if size:
                ret += int(size.text)
        return ret
    
    @retry((Exception), tries=1, delay=0)
    def upload(self, local_file_path, remote_file_path):
        '''Upload the file at *local_file_path* to the path *remote_file_path* at the remote server'''
        with open(local_file_path) as fd:
            self._get_client().put(remote_file_path, fd)
        
    @retry((Exception), tries=1, delay=0)    
    def get_file(self, path_to_file): 
        response = self._get_client().get(path_to_file)
        return response.content 
    
    def _get_client(self):
        client = WebDAVClient(self.url, self.port)
        client.setbasicauth(self.user, self.pwd)
        return client
    
    @retry((Exception), tries=1, delay=0)
    def move(self, source, target):
        ''':raises: NoSuchFilesytemObjectError if source does not exist'''
        target = quote(target)
        self._get_client().move(source, target, overwrite=True)
        
    @retry((Exception), tries=1, delay=0)
    def copy(self, source, target):
        ''':raises: NoSuchFilesytemObjectError if source does not exist'''
        target = quote(target)
        self._get_client().copy(source, target, overwrite=True)
    
    @retry((Exception), tries=1, delay=0)
    def rmdir(self, directory):
        ''':raises: StoreAccessError if the directory cannot be deleted
        :raises: NoSuchFilesytemObjectError if path does not exist'''
        if not directory.endswith('/'):
            directory += '/'
        self._get_client().delete(self.root + directory, {'Depth':"infinity"})
    
    @retry((Exception), tries=1, delay=0)
    def rm(self, filepath):
        ''':raises: StoreAccessError if the file cannot be deleted
        :raises: NoSuchFilesytemObjectError if path does not exist'''
        if filepath.endswith('/'):
            filepath = filepath[0:-1]
        self._get_client().delete(self.root + filepath).content
        
    @retry((Exception), tries=1, delay=0)
    def mkdir(self, dirpath):
        ''':raises: StoreAccessError if the directory cannot be created'''
        self._get_client().mkcol(self.root + dirpath).content
    
    @retry((Exception), tries=1, delay=0)
    def get_directory_listing(self, directory):
        ''':raises: StoreAccessError if the directory cannot be listed
        :raises: NoSuchFilesytemObjectError if path does not exist'''
        response = self._get_client().propfind(self.root + directory, depth=1)
        if response.content == '':
            return []
        if not response in range(200, 300):
            raise StoreAccessError(response.statusline, int(response))
        response_soup = BeautifulSoup(response.content)
        multi_response = response_soup.findAll(re.compile(r'(?i)[a-z0-9]:response'))
        ret = []
        for response in multi_response:
            path = response.find(re.compile(r'(?i)[a-z0-9]:href')).text
            if path.endswith('/'):
                path = path[:-1]
            if path != self.root + directory:
                ret.append( path )
        return ret 
    
    
    @retry((Exception), tries=1, delay=0)
    def get_bulk_metadata(self, directory):
        ''':returns: A dictionary mapping the path of every file object in, and including *directory* to a dictionary with the keys 
        'modified', 'bytes' and 'is_dir' containing the corresponding metadata for the file object.
        The value for 'modified' is a date in seconds, stating when the file object was last modified.  
        The value for 'bytes' is the number of bytes of the file object. It is 0 if the object is a directory.
        The value for 'is_dir' is True if the file object is a directory and False otherwise.
        
        :raises: NoSuchFilesytemObjectError if the directory does not exist
        '''
        response = self._get_client().propfind(self.root + directory, depth=1)
        response_soup = BeautifulSoup(response.content)
        multi_response = response_soup.findAll(re.compile(r'(?i)[a-z0-9]:response'))
        ret = {}
        for response in multi_response:
            path = response.find(re.compile(r'(?i)[a-z0-9]:href')).text
            path = unquote(path)
            item = {}
            if path.endswith('/') and path != '/':
                path = path[:-1]
            item["path"] = path
            mod_date = response.find(re.compile(r'(?i)[a-z0-9]:getlastmodified')).text
            cal = pdt.Calendar()
            mod_date =  int(time.mktime(cal.parse(mod_date)[0]))
            item["modified"] = mod_date
            resource_type = response.find(re.compile(r'(?i)[a-z0-9]:resourcetype'))
            if resource_type.findChild() == None:
                item["is_dir"] = False #GMX Mediacenter does not necessarily return a type in resourcetype tag, so we just assume it is a file
            else: 
                item["is_dir"] = resource_type.findChild().name.split(':')[-1] == 'collection'
            if not item["is_dir"]:
                item["bytes"] = int(response.find(re.compile(r'(?i)[a-z0-9]:getcontentlength')).text)
            else:
                item["bytes"] = 0
            if not ( 'is_dir' in item and 'bytes' in item and 'modified' in item):
                raise StoreAccessError("Error in get_metadata(%s): \n no getcontentlength or getlastmodified property in %s" % (path, response))
            ret[path] = item 

        return ret 