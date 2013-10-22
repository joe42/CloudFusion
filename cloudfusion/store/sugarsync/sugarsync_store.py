'''
Created on 08.04.2011
'''

import time
from cloudfusion.store.store import *
from cloudfusion.store.sugarsync.client import SugarsyncClient
from cloudfusion.util.xmlparser import DictXMLParser
from cloudfusion.util.string import *
import xml.dom.minidom as dom
import logging
from cloudfusion.util.exponential_retry import retry
import socket
import multiprocessing
import cloudfusion.util.pickle_methods
import signal
import sys

signal.signal(signal.SIGTERM, lambda signum, stack_frame: sys.exit(1))



class HTTP_STATUS(object):
    OK = range(200, 300)
    SERVER_ERROR = range(500,600)
    CREATED = 201        #
    MOVED_PERMANENTLY = 301       
    BAD_REQUEST = 400             
    AUTHORIZATION_REQUIRED = 401  
    FORBIDDEN = 403
    NOT_FOUND = 404               
    NOT_ACCEPTABLE = 406          
    OVER_STORAGE_LIMIT = 413
    INCORRECT_RANGE = 416         
    
    @staticmethod
    def generate_exception(code, msg, method_name=''):
        if code in HTTP_STATUS.OK:
            pass
        elif code == HTTP_STATUS.AUTHORIZATION_REQUIRED:
            raise StoreAutorizationError("StoreAutorizationError Message: "+msg+"\nStatus description: "+HTTP_STATUS.get_status_desc(code), code)
        elif code == HTTP_STATUS.OVER_STORAGE_LIMIT:
            raise StoreSpaceLimitError("StoreSpaceLimitError Message: "+msg+"\nStatus description: "+HTTP_STATUS.get_status_desc(code), code)
        elif code == HTTP_STATUS.NOT_FOUND:
            raise NoSuchFilesytemObjectError("NoSuchFilesytemObjectError Message: "+msg+"\nStatus description: "+HTTP_STATUS.get_status_desc(code), code)
        elif (method_name == 'create_directory' and code == HTTP_STATUS.BAD_REQUEST): # status means "is no folder" in this case
            raise AlreadyExistsError("Directory does already ex ist:" +msg, code)
        else:
            raise StoreAccessError("StoreAccessError Message: "+msg+"\nStatus description: "+HTTP_STATUS.get_status_desc(code), code)
            
        
    @staticmethod
    def log_error(logger, code, method_name, msg):
        if code in HTTP_STATUS.OK:
            pass
        else:
            logger.error("Error in method "+method_name+":\nMessage: "+msg+"\nSTATUS:"+HTTP_STATUS.get_status_desc(code), code)
    
    @staticmethod
    def get_status_desc(code):
        if code == HTTP_STATUS.OK:
            return "Request was successful."
        elif code == HTTP_STATUS.SERVER_ERROR:
            return "A server error occurred."
        elif code == HTTP_STATUS.CREATED:
            return "Created. The Location header of the response contains the URL of the newly created file or folder."
        elif code == HTTP_STATUS.MOVED_PERMANENTLY:
            return "Moved Permanently. The folder's URL changed as a result of the PUT request, and the new URL is in Location header."
        elif code == HTTP_STATUS.BAD_REQUEST:
            return "Bad request. Check for poorly formed or invalid XML."
        elif code == HTTP_STATUS.AUTHORIZATION_REQUIRED:
            return "Authorization required; the presented credentials, if any, are not sufficient."
        elif code == HTTP_STATUS.FORBIDDEN:
            return "Forbidden. Can't access the intended URI or URL or operation is not allowed. (For example, cannot delete system folder like Magic Briefcase.)"
        elif code == HTTP_STATUS.NOT_FOUND:
            return "Not found. Check the path of the URI or URL for the intended target."
        elif code == HTTP_STATUS.NOT_ACCEPTABLE:
            return "Not acceptable. Check for unsupported or invalid Accept header value."
        elif code == HTTP_STATUS.OVER_STORAGE_LIMIT:
            return "Over storage limit. Check PUT request entity size."
        elif code == HTTP_STATUS.INCORRECT_RANGE:
            return "Incorrect range or cannot be satisfied. Check intended values."
        else:
            return "No error description available."

"""exception HttpLib2Error
    The Base Exception for all exceptions raised by httplib2. 

exception RedirectMissingLocation
    A 3xx redirect response code was provided but no Location: header was provided to point to the new location. 

exception RedirectLimit
    The maximum number of redirections was reached without coming to a final URI. 

exception ServerNotFoundError
    Unable to resolve the host name given. 

exception RelativeURIError
    A relative, as opposed to an absolute URI, was passed into request(). 

exception FailedToDecompressContent
    The headers claimed that the content of the response was compressed but the decompression algorithm applied to the content failed. 

exception UnimplementedDigestAuthOptionError
    The server requested a type of Digest authentication that we are unfamiliar with. 

exception UnimplementedHmacDigestAuthOptionError
    The server requested a type of HMACDigest authentication that we are unfamiliar with. """
        
def wait_for_event(event):
    event.wait()
 
class SugarsyncStore(Store):
    def __init__(self, config):
        #self.dir_listing_cache = {}
        self._logging_handler = 'sugarsync'
        self.logger = logging.getLogger(self._logging_handler)
        self.path_cache = {}
        #error handling for authorization error
        self.root = config["root"]
        try:
            self.client = SugarsyncClient(config)
        except Exception, e:
            raise StoreAutorizationError(repr(e), 0)
        self.time_difference = self._get_time_difference()
        self.logger.debug("sugarsync store initialized")
        super(SugarsyncStore, self).__init__() 
        
    def get_name(self):
        self.logger.debug("getting name")
        return "Sugarsync"
    
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
    
    def _translate_path(self, path):
        """Translate unix style path into Sugarsync style path.
        :raise NoSuchFilesytemObjectError: if there is no such path"""
        self.logger.debug("translating path: %s", path) #+" cache: "+str(self.path_cache)
        path = to_unicode( path, "utf8")
        if path in self.path_cache:
            return self.path_cache[path]
        if path == "/":
            return self.root
        else:
            parent_dir = to_unicode( os.path.dirname(path), "utf8")
            self.path_cache[parent_dir] = self._translate_path(parent_dir)
            collection = self._parse_collection(self.path_cache[parent_dir])
            for item in collection:
                if parent_dir[-1] != "/":
                    parent_dir += "/"
                self.path_cache[ parent_dir+to_unicode( item["name"], "utf8") ] = item["reference"]
            if not path in self.path_cache:
                self.logger.warning("could not translate path: %s", path)
                raise NoSuchFilesytemObjectError(path,0)
            return self.path_cache[path]
            
            
    def _parse_collection(self, translated_path):
        """:returns: dict a dictionary with all paths of the collection at *translated_path* as keys and the corresponding nested dictionaries with the key/value pair for is_dir and reference."""
        ret = []
        resp = self.client.get_dir_listing(translated_path)
        if not resp.status in HTTP_STATUS.OK:
            self.logger.warning("could not get directory listing: %s\nstatus: %s reason: %s", translated_path, resp.status, resp.reason)
            HTTP_STATUS.generate_exception(resp.status, str(resp))
        xml_tree = dom.parseString(resp.data)
        for collection in xml_tree.documentElement.getElementsByTagName("collection"): 
            item = {}
            item["is_dir"] = collection.getAttribute("type") == "folder"
            item["name"] = collection.getElementsByTagName("displayName")[0].firstChild.nodeValue.encode('utf8')
            reference_url = collection.getElementsByTagName("ref")[0].firstChild.nodeValue
            reference_uid = regSearchString('.*:(.*)', reference_url)
            item["reference"] = reference_uid
            ret.append(item)
        for collection in xml_tree.documentElement.getElementsByTagName("file"): 
            item = {}
            item["is_dir"] = False
            item["name"] = collection.getElementsByTagName("displayName")[0].firstChild.nodeValue
            item["size"] = collection.getElementsByTagName("size")[0].firstChild.nodeValue
            item["lastModified"] = collection.getElementsByTagName("lastModified")[0].firstChild.nodeValue
            item["presentOnServer"] = collection.getElementsByTagName("lastModified")[0].firstChild.nodeValue
            reference_url = collection.getElementsByTagName("ref")[0].firstChild.nodeValue
            reference_uid = regSearchString('.*:(.*)', reference_url)
            item["reference"] = reference_uid
            ret.append(item)
        return ret
    
    
    def account_info(self):
        self.logger.debug("retrieving account info")
        try:
            info = self.client.user_info()
        except Exception, e:
            self.logger.error("Error retrieving account info.", exc_info=1)
            return "Sugarsync store."
        partial_tree = {"user": {"quota": {"limit": "limit", "usage": "usage"}}}
        DictXMLParser().populate_dict_with_XML_leaf_textnodes(info.data, partial_tree)
        #print response.status, response.reason, response.getheaders()
        ret = {}
        ret['overall_space'] = int(partial_tree['user']['quota']['limit'])
        ret['used_space'] = int(partial_tree['user']['quota']['usage'])
        return "Sugarsync overall space: %s, used space: %s" % (ret['overall_space'], ret['used_space']) 
    
    @retry((Exception,socket.error))
    def get_overall_space(self):
        self.logger.debug("retrieving all space")
        info = self.client.user_info()
        #print response.status, response.reason, response.getheaders()
        if not info.status in HTTP_STATUS.OK:
            self.logger.warning("could not retrieve overall space\nstatus: %s reason: %s", info.status, info.reason)
            HTTP_STATUS.generate_exception(info.status, str(info))
        partial_tree = {"user": {"quota": {"limit": "limit", "usage": "usage"}}}
        DictXMLParser().populate_dict_with_XML_leaf_textnodes(info.data, partial_tree)
        return int(partial_tree['user']['quota']['limit'])
    
    @retry((Exception,socket.error))
    def get_used_space(self):
        self.logger.debug("retrieving used space")
        info = self.client.user_info()
        #print response.status, response.reason, response.getheaders()
        if not info.status in HTTP_STATUS.OK:
            self.logger.warning("could not retrieve used space\nstatus: %s reason: %s", info.status, info.reason)
            HTTP_STATUS.generate_exception(info.status, str(info))
        partial_tree = {"user": {"quota": {"limit": "limit", "usage": "usage"}}}
        DictXMLParser().populate_dict_with_XML_leaf_textnodes(info.data, partial_tree)
        return int(partial_tree['user']['quota']['usage'])
    
    @retry((Exception,socket.error))
    def get_file(self, path_to_file): 
        self.logger.debug("getting file: %s", path_to_file)
        self._raise_error_if_invalid_path(path_to_file)
        resp = self.client.get_file( self._translate_path(path_to_file) )
        if not resp.status in HTTP_STATUS.OK:
            self.logger.warning("could not get file: %s\nstatus: %s reason: %s", path_to_file, resp.status, resp.reason)
            HTTP_STATUS.generate_exception(resp.status, str(resp))
        return resp.data 
    
    # retry does not really matter with caching_store
    @retry((Exception,socket.error), tries=1, delay=0) 
    def store_file(self, path_to_file, dest_dir="/", remote_file_name = None, interrupt_event=None):
        if not remote_file_name:
            remote_file_name = os.path.basename(path_to_file)
        remote_path = dest_dir + "/" + remote_file_name
        self.logger.debug("storing file to %s", remote_path)
        if not self.exists(remote_path):
            self._create_file(remote_path)
            
        result_queue = multiprocessing.Queue()
        write_process = multiprocessing.Process(target=self.client.put_file_async, args=(path_to_file, self._translate_path(remote_path), result_queue))
        write_process.daemon = True
        write_process.start()
        interrupt_process = None
        if interrupt_event:
            interrupt_process = multiprocessing.Process(target=wait_for_event, args=(interrupt_event,))
            interrupt_process.daemon = True
            interrupt_process.start()
        while True:
            write_process.join(1)
            if not write_process.is_alive():
                if interrupt_process:
                    interrupt_process.terminate()
                break
            if interrupt_process and not interrupt_process.is_alive():
                self.logger.debug("terminating stale upload of %s in process %s", remote_path, write_process.pid)
                write_process.terminate()
                if interrupt_process:
                    interrupt_process.terminate()
                self.logger.debug("terminated stale upload of %s in process %s", remote_path, os.getpid())
                raise InterruptedException("Stale upload has been interrupted.")
        
        resp = result_queue.get()   
        if isinstance(resp, Exception):
            raise resp
        
        if not resp.status in HTTP_STATUS.OK:
            self.logger.warning("could not store file to %s\nstatus: %s reason: %s", remote_path, resp.status, resp.reason)
            HTTP_STATUS.generate_exception(resp.status, str(resp))
        return int(time.mktime( time.strptime(resp.headers['Date'], "%a, %d %b %Y %H:%M:%S GMT") ) - self.time_difference) 
            
    @retry((Exception,socket.error), tries=1, delay=0) 
    def store_fileobject(self, fileobject, path_to_file):
        self.logger.debug("storing file object to %s", path_to_file)
        if not self.exists(path_to_file):
            self._create_file(path_to_file)
        resp = self.client.put_file( fileobject, self._translate_path(path_to_file) ) 
        if not resp.status in HTTP_STATUS.OK:
            self.logger.warning("could not store file to %s\nstatus: %s reason: %s", path_to_file, resp.status, resp.reason)
            HTTP_STATUS.generate_exception(resp.status, str(resp))
        return int(time.mktime( time.strptime(resp.headers['Date'], "%a, %d %b %Y %H:%M:%S GMT") ) - self.time_difference) 
            
    def _create_file(self, path, mime='text/x-cloudfusion'):
        self.logger.debug("creating file object %s", path)
        name = os.path.basename(path)
        directory = os.path.dirname(path)
        translated_dir = self._translate_path(directory)
        resp = self.client.create_file(translated_dir, name)
        if not resp.status in HTTP_STATUS.OK:
            self.logger.warning("could not create file %s\nstatus: %s reason: %s", path, resp.status, resp.reason)
            HTTP_STATUS.generate_exception(resp.status, str(resp))
    
    # worst case: object still exists and takes up space or is appended to, by mistake
    # with caching_store, the entry in cache is deleted anyways 
    @retry((Exception,socket.error), tries=5, delay=0) 
    def delete(self, path):
        self.logger.debug("deleting %s", path)
        if path == "/":
            return
        if path[-1] == "/":
            path = path[0:-1]
        self._raise_error_if_invalid_path(path)
        resp = self.client.delete_file( self._translate_path(path) )
        if not resp.status in HTTP_STATUS.OK:
            resp = self.client.delete_folder( self._translate_path(path) )
        if not resp.status in HTTP_STATUS.OK and not resp.status == HTTP_STATUS.NOT_FOUND:
            self.logger.warning("could not delete %s\nstatus: %s reason: %s", path, resp.status, resp.reason)
            HTTP_STATUS.generate_exception(resp.status, str(resp))
        else:
            del self.path_cache[path]
        return resp.status
        
    # worst case: would be annoying  when copying nested directory structure and failure occurs
    @retry((Exception,socket.error))
    def create_directory(self, path):
        self.logger.debug("creating directory %s", path)
        self._raise_error_if_invalid_path(path)
        if path == "/":
            return
        if path[-1] == "/":
            path = path[0:-1]
        if self.exists(path):
            raise AlreadyExistsError("directory %s does already exist"%path, 0)
        name = os.path.basename(path)
        directory = os.path.dirname(path)
        resp = self.client.create_folder( self._translate_path(directory), name ) 
        if not resp.status in HTTP_STATUS.OK:
            self.logger.warning("could not create directory: %s\nstatus: %s reason: %s", path, resp.status, resp.reason)
            HTTP_STATUS.generate_exception(resp.status, str(resp), "create_directory")
        return resp.status
    
    # worst case: might be critical with backups when only updating changed items in nested structure
    @retry((Exception,socket.error))
    def get_directory_listing(self, directory):
        self.logger.debug("getting directory listing for %s", directory)
        ret = []
        translated_dir = self._translate_path(directory)
        collection = self._parse_collection(translated_dir)
        if directory[-1] != "/":
            directory += "/"
        for item in collection:
            ret.append( directory+item['name'] )
        #self.logger.warning(str(ret))
        return ret 
    
    # worst case: should happen mostly with user interaction, so fast feedback is more important
    @retry((Exception,socket.error), tries=1, delay=0)
    def duplicate(self, path_to_src, path_to_dest): #src might be a directory
        self.logger.debug("duplicating %s to %s", path_to_src, path_to_dest)
        self._raise_error_if_invalid_path(path_to_src)
        self._raise_error_if_invalid_path(path_to_dest)
        if path_to_src[-1] == "/":
            path_to_src = path_to_src[0:-1]
        if path_to_dest[-1] == "/":
            path_to_dest = path_to_dest[0:-1]
        dest_name = os.path.basename(path_to_dest)
        dest_dir  = os.path.dirname(path_to_dest)
        translated_dest_dir = self._translate_path( dest_dir )
        translated_src = self._translate_path(path_to_src)
        if self.is_dir(path_to_src):
            #make destination directory: (might exist)
            if not self.exists(path_to_dest):
                self.create_directory(path_to_dest)
            #copy all files from original directory:
            for item in self._parse_collection(translated_src):
                if item['is_dir']:#copy all folders form original directory
                    self.duplicate(path_to_src+"/"+item['name'], path_to_dest+"/"+item['name'])
                else:
                    resp = self.client.duplicate_file(item['reference'], translated_dest_dir, dest_name)
                    if resp.status != 200:
                        self.logger.warning("could not duplicate %s to %s\nstatus: %s reason: %s", path_to_src, path_to_dest, resp.status, resp.reason)
                        HTTP_STATUS.generate_exception(resp.status, str(resp))
        else:
            #if dest exists remove
            if self.exists(path_to_dest):
                self.delete(path_to_dest)
            resp = self.client.duplicate_file(translated_src, translated_dest_dir, dest_name)
            if not resp.status in HTTP_STATUS.OK:
                self.logger.warning("could not duplicate %s to %s\nstatus: %s reason: %s", path_to_src, path_to_dest, resp.status, resp.reason)
                HTTP_STATUS.generate_exception(resp.status, str(resp))
    
    def _handle_error(self, error, method_name, *args, **kwargs):
        if isinstance(error, AttributeError):
            self.logger.debug("Retrying on funny socket error: %s", error)
            #funny socket error in httplib2: AttributeError 'NoneType' object has no attribute 'makefile'
        elif isinstance(error, StoreAutorizationError):
            self.logger.debug("Trying to handle authorization error by reconnecting: %s", error)
            self.reconnect()
        elif isinstance(error, StoreAccessError):
            if error.status == HTTP_STATUS.OVER_STORAGE_LIMIT or \
                error.status == HTTP_STATUS.BAD_REQUEST or \
                error.status == HTTP_STATUS.FORBIDDEN or \
                isinstance(error, AlreadyExistsError) or \
                isinstance(error, NoSuchFilesytemObjectError):
                self.logger.debug("Error could not be handled: %s", error)
                raise error # do not retry (error cannot be handled)
        else:
            self.logger.debug("Error is not covered by _handle_error: %s", error)
        return False
        

    def _parse_file(self, path, resp):
        ret = {}
        partial_tree = {"file":{"size":"", "lastModified":"", "timeCreated":""}}
        DictXMLParser().populate_dict_with_XML_leaf_textnodes(resp.data, partial_tree)
        ret["bytes"] = int(partial_tree['file']['size'])
        if ret["bytes"] == -1: #-1 if file was created but has no data
            ret["bytes"] = 0
        try:
            lastModified = partial_tree['file']['lastModified']
            modified = time.mktime(time.strptime(lastModified[0:-6], "%Y-%m-%dT%H:%M:%S.000"))
            time_offset = time.strptime(lastModified[-5:], "%H:%M")
            time_delta = 60 * 60 * time_offset.tm_hour + 60 * time_offset.tm_min
            modified += time_delta
            ret["modified"] = modified - self.time_difference #"Sat, 21 Aug 2010 22:31:20 +0000"#2011-05-10T06:18:33.000-07:00     Time conversion error: 2011-05-20T05:15:44.000-07:00
        except Exception as x:
            self.logger.warning("Time conversion error: %s reason: %s", partial_tree['file']['lastModified'], x)
            raise DateParseError("Error parsing modified attribute: %s" % str(x))
        ret["created"] = partial_tree['file']['timeCreated']
        ret["path"] = path
        ret["is_dir"] = False
        return ret

    def _parse_directory(self, path, resp):
        ret = {}
        partial_tree = {"folder":{"timeCreated":""}}
        DictXMLParser().populate_dict_with_XML_leaf_textnodes(resp.data, partial_tree)
        ret["bytes"] = 0
        ret["modified"] = time.time()
        ret["created"] = partial_tree['folder']['timeCreated']
        ret["path"] = path
        ret["is_dir"] = True
        return ret
    
    # worst case: can break backups if it fails
    @retry((Exception,socket.error))
    def _get_metadata(self, path):
        self.logger.debug("getting metadata for %s", path)
        self._raise_error_if_invalid_path(path)
        if path == "/": # workaraund for root metadata necessary for sugarsync?
            ret = {}
            ret["bytes"] = 0
            ret["modified"] = time.time()
            ret["path"] = "/"
            ret["is_dir"] = True
            return ret;
        if path[-1] == "/":
            path = path[0:-1]
        is_file = True
        sugarsync_path =  self._translate_path(path)
        resp = self.client.get_file_metadata( sugarsync_path )
        if resp.status == HTTP_STATUS.BAD_REQUEST: # status means "is no folder" in this case
            is_file = False
            resp = self.client.get_folder_metadata( sugarsync_path )
        else: 
            HTTP_STATUS.generate_exception(resp.status, str(resp))
        HTTP_STATUS.generate_exception(resp.status, str(resp))
        if is_file:
            ret = self._parse_file(path, resp)
        else:
            ret = self._parse_directory(path, resp)
        return ret;
            
    @retry((Exception,socket.error))
    def _get_time_difference(self):
        resp =  self.client.user_info()
        HTTP_STATUS.generate_exception(resp.status, str(resp))
        return time.mktime( time.strptime(resp.getheader('date'), "%a, %d %b %Y %H:%M:%S GMT") ) - time.time()
    
    def reconnect(self):
        try:
            self.client._reconnect()
        except Exception, e:
            self.logger.error("Error reconnecting: "+str(e))
        
        
    def get_logging_handler(self):
        return self._logging_handler
    
    def get_max_filesize(self):
        """Return maximum number of bytes per file"""
        return 100*1000*1000
