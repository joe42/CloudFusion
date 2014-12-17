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
import socket, errno
try:
    from urllib3 import quote, unquote
except Exception, e:
    try:
        from urllib2 import quote, unquote
    except Exception, e:
        from urllib import quote, unquote

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
        # Remove last slash from url.
        if self.url[-1] == '/':
            self.url = self.url[:-1]
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
    
    def _handle_error(self, error, stacktrace, method_name, remaining_tries, *args, **kwargs):
        if isinstance(error, HTTPUserError):
            if method_name == 'upload':
                fname = args[1]  # position of fileobject name for error message depends on method
            else:
                fname = args[0]
            http_response = error.response.response # error.response is HTTPResponse or a WebDAVResponse, and error.response.response is a httplib.HTTPResponse
            if http_response.status == 404:               #'WebDAVResponse' object has no attribute 'status
                #box.com and yandex.com do not instantly see files that are written to it (eventual consistency), so retry once
                if method_name in ['get_metadata', 'rmdir', 'rm', 'move', 'copy']  and remaining_tries != 0: 
                    return False
                raise NoSuchFilesytemObjectError(fname, http_response.status)
            elif http_response.status == 405 or http_response.status == 409:#405 is method not allowed; 4shared responds with 409 (Conflict) if directory already exists
                raise AlreadyExistsError(fname, http_response.status)
        if isinstance(error, socket.error):
            msg = 'Retry on socket error'
            if isinstance(error.args, tuple):
                msg += " with errno %d" % error.errno
                if error.errno == errno.EPIPE:
                    msg += ". Detected remote disconnect"
                self.logger.error(msg+'.')
            return False
        if isinstance(error, HTTPServerError):
            self.logger.error("Error could not be handled: \n%s", stacktrace)
            raise error # do not retry (error cannot be handled)
        if remaining_tries == 0: # throw error after last try 
            raise error
        return False
        
    @retry((Exception), tries=2, delay=1)
    def get_metadata(self, path):
        ''':raises: StoreAccessError if propfind does not return getcontentlength or getlastmodified property
        :raises: NoSuchFilesytemObjectError if path does not exist'''
        # if path is a directory we need to append / or handle a redirect there
        response = self._get_client().propfind(self.root + path, depth=0, headers = {"Content-Type": "application/xml; charset=utf-8"})
        # redirects can mean that this is a directory
        if response.numerator == 301:
            location = response.headers['location']
            _, location_url = location.split('//',1) # remove protocol
            _, location_path = location_url.split('/',1) # remove domain 
            self.logger.debug("redirect to %s", location_path)
            response = self._get_client().propfind(location_path, depth=0, headers = {"Content-Type": "application/xml; charset=utf-8"}) 
        response_soup = BeautifulSoup(response.content)
        response = response_soup.find(re.compile(r'(?i)[a-z0-9]:response'))
        ret = {}
        ret["path"] = path
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
        path = self.root
        if self.root == '':
            path = '/'
        response = self._get_client().propfind(path, depth=0, properties=["quota-available-bytes"], headers = {"Content-Type": "application/xml; charset=utf-8"})
        response_soup = BeautifulSoup(response.content)
        try:
            response = response_soup.find(re.compile(r'(?i)[a-z0-9]:response'))
            ret = response.find(re.compile(r'(?i)[a-z0-9]:quota-available-bytes'))
            return int(ret.text)
        except Exception, e:
            self.logger.error("Get overall space failed with: %s; instead returning default value of 1TB", str(e))
        return 1000*1000*1000*1000 #set to 1 TB 
    
    @retry((Exception), tries=1, delay=0)
    def get_used_space(self):
        ret = 0
        path = self.root
        if self.root == '':
            path = '/'
        responses = self._get_client().propfind(path, depth=1, headers = {"Content-Type": "application/xml; charset=utf-8"})
        try:
            for status in responses:
                response_soup = BeautifulSoup(ElementTree.tostring(status.response, 'utf8'))
                response = response_soup.find(re.compile(r'(?i)[a-z0-9]:response'))
                size = response.find(re.compile(r'(?i)[a-z0-9]:getcontentlength'))
                if size:
                    ret += int(size.text)
        except Exception, e:
            self.logger.error("Get used space failed with: %s; instead returning default value of 0", str(e))
        return ret
    
    @retry((Exception), tries=2, delay=0)
    def upload(self, local_file_path, remote_file_path):
        '''Upload the file at *local_file_path* to the path *remote_file_path* at the remote server'''
        with open(local_file_path) as fd:
            self._get_client().put(self.root + remote_file_path, fd, headers = {"Content-Type": "application/xml; charset=utf-8"})
        
    @retry((Exception), tries=1, delay=0)    
    def get_file(self, path_to_file): 
        response = self._get_client().get(self.root + path_to_file, headers = {"Content-Type": "application/xml; charset=utf-8"})
        return response.content 
    
    def _get_client(self):
        client = WebDAVClient(self.url, self.port)
        client.setbasicauth(self.user, self.pwd) 
        client.timeout = 60 #set reasonable timeout
        return client
    
    @retry((Exception), tries=2, delay=1)
    def move(self, source, target):
        ''':raises: NoSuchFilesytemObjectError if source does not exist'''
        target = quote(target)
        self._get_client().move(self.root + source, self.root + target, overwrite=True, headers = {"Content-Type": "application/xml; charset=utf-8"})
        
    @retry((Exception), tries=2, delay=1)
    def copy(self, source, target):
        ''':raises: NoSuchFilesytemObjectError if source does not exist'''
        target = quote(target)
        self._get_client().copy(self.root + source, self.root + target, overwrite=True, headers = {"Content-Type": "application/xml; charset=utf-8"})
    
    @retry((Exception), tries=2, delay=1)
    def rmdir(self, directory):
        ''':raises: StoreAccessError if the directory cannot be deleted
        :raises: NoSuchFilesytemObjectError if path does not exist'''
        if not directory.endswith('/'):
            directory += '/'
        self._get_client().delete(self.root + directory, headers = {'Depth':"infinity", "Content-Type": "application/xml; charset=utf-8"})
    
    @retry((Exception), tries=2, delay=1)
    def rm(self, filepath):
        ''':raises: StoreAccessError if the file cannot be deleted
        :raises: NoSuchFilesytemObjectError if path does not exist'''
        if filepath.endswith('/'):
            filepath = filepath[0:-1]
        self._get_client().delete(self.root + filepath, headers = {"Content-Type": "application/xml; charset=utf-8"}).content
        
    @retry((Exception), tries=1, delay=0)
    def mkdir(self, dirpath):
        ''':raises: StoreAccessError if the directory cannot be created'''
        self._get_client().mkcol(self.root + dirpath, headers = {"Content-Type": "application/xml; charset=utf-8"}).content
    
    @retry((Exception), tries=1, delay=0)
    def get_directory_listing(self, directory):
        ''':raises: StoreAccessError if the directory cannot be listed
        :raises: NoSuchFilesytemObjectError if path does not exist'''
        if not directory.endswith('/'):
            directory += '/'
        response = self._get_client().propfind(self.root + directory, depth=1, headers = {"Content-Type": "application/xml; charset=utf-8"})
        if response.content == '':
            return []
        response_soup = BeautifulSoup(response.content)
        multi_response = response_soup.findAll(re.compile(r'(?i)[a-z0-9]:response'))
        ret = []
        for response in multi_response:
            path = response.find(re.compile(r'(?i)[a-z0-9]:href')).text.encode("utf8")
            path = unquote(path)
            if path.endswith('/'):
                path = path[:-1]
            if path.startswith(self.root): #cut off root
                path = path[len(self.root):]
            if path == '/' or path == '':
                continue
            if path != directory[:-1]:
                ret.append( path )
        return ret 
    
    
    @retry((Exception), tries=1, delay=0)
    def get_bulk_metadata(self, directory):
        ''':returns: A dictionary mapping the path of every file object in *directory* to a dictionary with the keys\
        'modified', 'bytes' and 'is_dir' containing the corresponding metadata for the file object.
        
        The value for 'modified' is a date in seconds, stating when the file object was last modified.  
        The value for 'bytes' is the number of bytes of the file object. It is 0 if the object is a directory.
        The value for 'is_dir' is True if the file object is a directory and False otherwise.
        
        :raises: NoSuchFilesytemObjectError if the directory does not exist
        '''
        if not directory.endswith('/'):
            directory += '/'
        response = self._get_client().propfind(self.root + directory, depth=1, headers = {"Content-Type": "application/xml; charset=utf-8"})
        response_soup = BeautifulSoup(response.content)
        multi_response = response_soup.findAll(re.compile(r'(?i)[a-z0-9]:response'))
        ret = {}
        for response in multi_response:
            path = response.find(re.compile(r'(?i)[a-z0-9]:href')).text.encode("utf8")
            path = unquote(path)
            item = {}
            if path.endswith('/'):
                path = path[:-1]
            if path == self.root + directory[:-1]:
                continue
            if path.startswith(self.root): #cut off root
                path = path[len(self.root):]
            if path == '/' or path == '':
                continue
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