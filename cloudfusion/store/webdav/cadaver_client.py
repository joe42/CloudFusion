import pexpect
import re
from cloudfusion.store.store import NoSuchFilesytemObjectError, StoreAccessError
import time
import cloudfusion.third_party.parsedatetime.parsedatetime as pdt
import tempfile
import os

class CadaverWebDAVClient(object):
    '''WebDav client implemented using the Debian package cadaver.'''


    def __init__(self, url, user, pwd):
        '''
        Create a WebDAV client connecting to the WebDAV server at *url* with the credentials username and a password.
        :param url: the WebDAV server url; i.e. https://dav.box.com/dav
        :param user: the username
        :param pwd: the password
        '''
        self.url = url
        self.user = user
        self.pwd = pwd
    
    def _remove_first_slash(self, path):
        if path != '/' and path [0] == '/':
            return path[1:]
        return path
        

    def move(self, source, target):
        ''':raises: NoSuchFilesytemObjectError if source does not exist'''
        source = self._remove_first_slash(source)
        target = self._remove_first_slash(target)
        self._webdav_cmd('mv', source, target)
        
    def copy(self, source, target):
        ''':raises: NoSuchFilesytemObjectError if source does not exist'''
        source = self._remove_first_slash(source)
        target = self._remove_first_slash(target)
        self._webdav_cmd('cp', source, target)
        
    def rm(self, path):
        ''':raises: NoSuchFilesytemObjectError if path does not exist'''
        path = self._remove_first_slash(path)
        self._webdav_cmd('delete', path)
        
    def rmdir(self, path):
        ''':raises: NoSuchFilesytemObjectError if path does not exist'''
        path = self._remove_first_slash(path)
        self._webdav_cmd('rmcol', path)
        
    def upload(self, local_file_path, remote_file_path):
        '''Upload the file at *local_file_path* to the path *remote_file_path* at the remote server'''
        remote_file_path = self._remove_first_slash(remote_file_path)
        self._webdav_cmd('put', local_file_path, remote_file_path)
        
    def mkdir(self, path):
        path = self._remove_first_slash(path)
        self._webdav_cmd('mkdir', path)
    
    def get_file(self, path_to_file): 
        with tempfile.NamedTemporaryFile(delete=False) as f:
            tempfile_path = f.name
        self._webdav_cmd('get', path_to_file[1:], tempfile_path) # cut first / from path
        with open(tempfile_path) as f:
            ret = f.read()
        os.remove(tempfile_path)
        return ret 
    
    def get_overall_space(self):
        res = self._webdav_cmd('propget', '.')
        for line in res.splitlines():
            if line.startswith('DAV: quota-available-bytes'):
                match = re.search(".*(\d+).*", line)
                if match:
                    return int(match.group(1))
        return 1000000000
    
    def get_used_space(self):
        res = self._webdav_cmd('ls')
        ret = 0
        for line in res.splitlines():
            match = re.search(".* (\d+)\s+[A-Z][a-z]+\s+[0-9]+\s+[0-9:]+", line) #...  2738  Feb 13 03:24
            if not match:
                continue
            if match:
                ret += int(match.group(1))
        return ret
    
    
    def get_directory_listing(self, directory):
        ''':raises: StoreAccessError if the directory cannot be listed
        :raises: NoSuchFilesytemObjectError if path does not exist'''
        res = self._webdav_cmd('ls', directory[1:])
        ret = []
        for line in res.splitlines():
            if line.endswith('failed:'):
                raise StoreAccessError("Error in get_directory_listing(%s): %s"%(directory, res))
            match = re.search("(.*) \d+\s+[A-Z][a-z]+\s+[0-9]+\s+[0-9:]+", line) #...  2738  Feb 13 03:24
            if match:
                line = match.group(1)
                if line.startswith('Coll:'):
                    line = line[5:]
                line = line.strip()
                if line.startswith('*'): #GMX Mediacenter appends asterisk before file objects
                    line = line[1:]
                line = '/'+line
                line = unicode(line, 'unicode-escape')
                ret.append(directory+line)
        return ret
    
    def _get_metadata(self, path):
        ''':raises: StoreAccessError if propfind does not return getcontentlength or getlastmodified property
        :raises: NoSuchFilesytemObjectError if path does not exist'''
        res = self._webdav_cmd('propget', path[1:])
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
        i = child.expect (['Username:', 'dav:.*/>']) #somehow webdav can sometimes remember the connection and does not need authentication
        if i==0:
            child.sendline(self.user)
            child.expect ('Password:')
            child.sendline(self.pwd)
            child.expect("dav:.*/>")
        
        child.sendline(sub_cmd) #send actual command
        
        if cmd != 'ls':
            i = child.expect (['Username:', 'dav:.*/>']) #handle reauthentication
            if i==0:
                child.sendline(self.user)
                child.expect ('Password:')
                child.sendline(self.pwd)
                child.expect("dav:.*/>")
        else: 
                child.expect("dav:.*/>")
        if re.search('.*redirect to .*', child.before): #handle redirect
            child.sendline(red_cmd) 
            child.expect("dav:.*/>")
        if child.before.find("404 Not Found") != -1:
            if arg1:
                raise NoSuchFilesytemObjectError(sub_cmd,404)
        return child.before
        