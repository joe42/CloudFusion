'''
Created on 08.04.2011

@author: joe
'''

import os
import time
from cloudfusion.store.store import *
import pexpect
import time
import cloudfusion.third_party.parsedatetime.parsedatetime as pdt
import re
import logging
from cloudfusion.util.exponential_retry import retry
from cloudfusion.mylogging import db_logging_thread
import sys
import tempfile

class WebdavStore(Store):
    def __init__(self, config):
        super(WebdavStore, self).__init__()
        self.name = 'webdav'
        self._logging_handler = self.name
        self.logger = logging.getLogger(self._logging_handler)
        self.logger = db_logging_thread.make_logger_multiprocessingsave(self.logger)
        self.logger.info("creating %s store", self.name)
        self.url = config['url'] 
        self.user = config['user'] 
        self.pwd = config['password'] 
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
            else:
                setattr(result, k, deepcopy(v, memo))
        return result
        
    def get_name(self):
        self.logger.info("getting name")
        return self.name
    
    @retry((Exception), tries=14, delay=0.1, backoff=2)
    def get_file(self, path_to_file): 
        self.logger.debug("getting file: %s", path_to_file)
        self._raise_error_if_invalid_path(path_to_file)
        with tempfile.NamedTemporaryFile(delete=False) as f:
            tempfile_path = f.name
        self._webdav_cmd('get', os.path.basename(path_to_file), tempfile_path)
        with open(tempfile_path) as f:
            ret = f.read()
        os.remove(tempfile_path)
        return ret 
        
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
        self._webdav_cmd('put', fileobject.name, os.path.basename(path))
        return int(time.time())
    
    
    # worst case: object still exists and takes up space or is appended to, by mistake
    # with caching_store, the entry in cache is deleted anyways 
    @retry((Exception), tries=5, delay=0) 
    def delete(self, path, is_dir=False): #is_dir parameter does not matter to dropbox
        self.logger.debug("deleting %s", path)
        self._raise_error_if_invalid_path(path)
        self._webdav_cmd('delete', os.path.basename(path))
        
    @retry((Exception))
    def account_info(self):
        self.logger.debug("retrieving account info")
        return "Webdav "

    @retry((Exception))
    def create_directory(self, directory):
        self.logger.debug("creating directory %s", directory)
        
    def duplicate(self, path_to_src, path_to_dest):
        self.logger.debug("duplicating %s to %s", path_to_src, path_to_dest)
    
    def move(self, path_to_src, path_to_dest):
        self.logger.debug("moving %s to %s", path_to_src, path_to_dest)
        
    def _webdav_cmd(self, cmd, arg1=None, arg2=None):
        timeout = 30
        if cmd == 'get' or cmd == 'put':
            timeout = 60 * 30 # 1 hour
        if arg2:
            sub_cmd = "%s '%s' '%s'" % (cmd, arg1, arg2)
        elif arg1:
            sub_cmd = "%s '%s'" % (cmd, arg1)
        else:
            sub_cmd = cmd
        red_cmd = "%s '%s/'" % (cmd, arg1) #directory requests are redirected to directory_name/
        child = pexpect.spawn('cadaver -t '+self.url, timeout=timeout)
        i = child.expect (['.*name.*', 'dav:.*/>']) #somehow webdav can sometimes remember the connection and does not need authentication
        if i==0:
            child.sendline(self.user)
            child.expect ('.*assword.*')
            child.sendline(self.pwd)
            child.expect("dav:.*/>")
        
        child.sendline(sub_cmd) #send actual command
        
        if cmd != 'ls':
            i = child.expect (['.*name.*', 'dav:.*/>']) #handle reauthentication
            if i==0:
                child.sendline(self.user)
                child.expect ('.*assword.*')
                child.sendline(self.pwd)
                child.expect("dav:.*/>")
        else: 
                child.expect("dav:.*/>")
        if re.search('.*redirect to .*', child.before): #handle redirect
            child.sendline(red_cmd) 
            child.expect("dav:.*/>")
        return child.before
    
    def get_overall_space(self):
        self.logger.debug("retrieving all space") 
        res = self._webdav_cmd('propget', '.')
        for line in res.splitlines():
            if line.startswith('DAV: quota-available-bytes'):
                match = re.search(".*(\d+).*", line)
                if match:
                    return int(match.group(1))
        return 1000000000

    def get_used_space(self):
        self.logger.debug("retrieving used space")
        res = self._webdav_cmd('ls')
        ret = 0
        for line in res.splitlines():
            match = re.search(".* (\d+)\s+[A-Z][a-z]+\s+[0-9]+\s+[0-9:]+", line) #...  2738  Feb 13 03:24
            if match:
                ret += int(match.group(1))
        return ret
        
    #@retry((Exception))
    def get_directory_listing(self, directory):
        self.logger.debug("getting directory listing for %s", directory)
        res = self._webdav_cmd('ls')
        ret = []
        for line in res.splitlines():
            if line.endswith('failed:'):
                raise StoreAccessError("Error in get_directory_listing(%s): %s"%(directory, res))
            match = re.search("(.*) \d+\s+[A-Z][a-z]+\s+[0-9]+\s+[0-9:]+", line) #...  2738  Feb 13 03:24
            if match:
                line = match.group(1)
                if line.startswith('Coll:'):
                    line = line[5:]
                line = '/'+line.strip()
                ret.append(line)
        return ret
        
    #@retry((Exception))
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
        res = self._webdav_cmd('propget', os.path.basename(path))
        ret = {}
        ret["is_dir"] = False
        for line in res.splitlines():
            if line.startswith('DAV: iscollection = TRUE') or line.startswith('DAV: resourcetype = <DAV:collection>'):
                ret["is_dir"] = True
            if line.startswith('DAV: getcontentlength = '):
                ret["bytes"] = int(line[24:])
            if line.startswith('DAV: getlastmodified = '):
                mod_date = line[23:]
                cal = pdt.Calendar()
                mod_date =  int(time.mktime(cal.parse(mod_date)[0]))
                ret["modified"] = mod_date
        if not ( 'is_dir' in ret and 'bytes' in ret and 'modified' in ret):
            raise StoreAccessError("Error in _get_metadata(%s): \n no getcontentlength or getlastmodified property in %s" % (path, res))
        ret["path"] = path
        return ret
        
    def _get_time_difference(self):
        self.logger.debug("getting time difference")
        return 0
    
    def get_logging_handler(self):
        return self._logging_handler
    
    def get_max_filesize(self):
        """Return maximum number of bytes per file"""
        return 1000*1000*1000*1000