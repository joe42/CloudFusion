'''
Created on 08.04.2011

@author: joe
'''

import time
import datetime
from cloudfusion.store.store import *
import logging
import logging.config
import os.path
from cloudfusion.store.sugarsync.client import SugarsyncClient
from cloudfusion.util.xmlparser import DictXMLParser
from cloudfusion.util.string import *
import tempfile
import httplib
import xml.dom.minidom as dom
        

 
class SugarsyncStore(Store):
    def __init__(self, config):
        #self.dir_listing_cache = {}
        self.robustness = 10
        self._logging_handler = 'sugarsync'
        self.logger = logging.getLogger(self._logging_handler)
        self.path_cache = {}
        self.root = config["root"]
        self.client = SugarsyncClient(config)
        self.time_difference = self._get_time_difference()
        self.logger.debug("sugarsync store initialized")
        super(SugarsyncStore, self).__init__() 
        
    def get_name(self):
        self.logger.debug("getting name")
        return "Sugarsync"
    
    
    def _translate_path(self, path):
        self.logger.debug("translating path: "+path) #+" cache: "+str(self.path_cache)
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
                self.logger.warn("could not translate path: " +path)
                raise NoSuchFilesytemObjectError(path,0)
            return self.path_cache[path]
            
            
    def _parse_collection(self, translated_path):
        """:returns: dict a dictionary with all paths of the collection at :param:`translated_path` as keys and the corresponding nested dictionaries with the key/value pair for is_dir and reference."""
        ret = []
        resp = self.client.get_dir_listing(translated_path)
        if resp.status <200 or resp.status >= 300:
            self.logger.warn("could not get directory listing: " +translated_path+"\nstatus: %s reason: %s" % (resp.status, resp.reason))
            if resp.status == 401 or resp.status >= 500:
                self._reconnect()
            raise NoSuchFilesytemObjectError(translated_path, resp.status)
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
        info = self.client.user_info()
        partial_tree = {"user": {"quota": {"limit": "limit", "usage": "usage"}}}
        DictXMLParser().populate_dict_with_XML_leaf_textnodes(info.data, partial_tree)
        #print response.status, response.reason, response.getheaders()
        ret = {}
        ret['overall_space'] = int(partial_tree['user']['quota']['limit'])
        ret['used_space'] = int(partial_tree['user']['quota']['usage'])
        return "Sugarsync overall space: %s, used space: %s" % (ret['overall_space'], ret['used_space']) 
    
    def get_overall_space(self):
        self.logger.debug("retrieving all space")
        info = self.client.user_info()
        partial_tree = {"user": {"quota": {"limit": "limit", "usage": "usage"}}}
        DictXMLParser().populate_dict_with_XML_leaf_textnodes(info.data, partial_tree)
        #print response.status, response.reason, response.getheaders()
        if info.status <200 or info.status >= 300:
            self.logger.warn("could not retrieve overall space"+"\nstatus: %s reason: %s" % (info.status, info.reason))
        return int(partial_tree['user']['quota']['limit'])
    
    def get_used_space(self):
        self.logger.debug("retrieving used space")
        info = self.client.user_info()
        partial_tree = {"user": {"quota": {"limit": "limit", "usage": "usage"}}}
        DictXMLParser().populate_dict_with_XML_leaf_textnodes(info.data, partial_tree)
        #print response.status, response.reason, response.getheaders()
        if info.status <200 or info.status >= 300:
            self.logger.warn("could not retrieve used space"+"\nstatus: %s reason: %s" % (info.status, info.reason))
        return int(partial_tree['user']['quota']['usage'])
    
    def get_file(self, path_to_file): 
        self.logger.debug("getting file: " +path_to_file)
        self._raise_error_if_invalid_path(path_to_file)
        file = self.client.get_file( self._translate_path(path_to_file) )
        if file.status <200 or file.status >= 300:
            self.logger.warn("could not get file: %s\nstatus: %s reason: %s" % (path_to_file, file.status, file.reason))
            if file.status == 401 or file.status >= 500:
                self._reconnect()
                self.robustness -= 1
                if self.robustness > 0:
                    self.logger.info("retrying")
                    return self.get_file(path_to_file)
        return file.data 
    
    def store_fileobject(self, fileobject, path_to_file):
        self.logger.debug("storing file object to "+path_to_file)
        if not self.exists(path_to_file):
            self._create_file(path_to_file)
        resp = self.client.put_file( fileobject, self._translate_path(path_to_file) ) 
        if resp.status <200 or resp.status >= 300:
            self.logger.warn("could not store file to " +path_to_file+"\nstatus: %s reason: %s" % (resp.status, resp.reason))
            if resp.status == 401 or resp.status >= 500:
                self._reconnect()
                self.robustness -= 1
                if self.robustness > 0:
                    self.logger.info("retrying")
                    self.store_fileobject(fileobject, path_to_file)
            
    def _create_file(self, path, mime='text/x-cloudfusion'):
        self.logger.debug("creating file object "+path)
        name = os.path.basename(path)
        directory = os.path.dirname(path)
        translated_dir = self._translate_path(directory)
        resp = self.client.create_file(translated_dir, name)
        if resp.status <200 or resp.status >= 300:
            self.logger.warn("could not create file " +path+"\nstatus: %s reason: %s" % (resp.status, resp.reason))
            if resp.status == 401 or resp.status >= 500:
                self._reconnect()
    
    def delete(self, path):
        self.logger.debug("deleting " +path)
        if path == "/":
            return
        if path[-1] == "/":
            path = path[0:-1]
        self._raise_error_if_invalid_path(path)
        resp = self.client.delete_file( self._translate_path(path) )
        if resp.status <200 or resp.status >= 300:
            resp = self.client.delete_folder( self._translate_path(path) )
        if resp.status <200 or resp.status >= 300:
            self.logger.warn("could not delete " +path+"\nstatus: %s reason: %s" % (resp.status, resp.reason))
            if resp.status == 401 or resp.status >= 500:
                self._reconnect()
        else:
            del self.path_cache[path]
        return resp.status
        
    
    def create_directory(self, path):
        self.logger.debug("creating directory " +path)
        self._raise_error_if_invalid_path(path)
        if path == "/":
            return
        if path[-1] == "/":
            path = path[0:-1]
        if self.exists(path):
            raise AlreadyExistsError("directory %s does already exist"%path, 401)
        name = os.path.basename(path)
        directory = os.path.dirname(path)
        resp = self.client.create_folder( self._translate_path(directory), name ) 
        if resp.status <200 or resp.status >= 300:
            self.logger.warn("could not create directory: " +path+"\nstatus: %s reason: %s" % (resp.status, resp.reason))
            if resp.status == 401 or resp.status >= 500:
                self._reconnect()
        return resp.status

    def get_directory_listing(self, directory):
        self.logger.debug("getting directory listing for "+directory)
        ret = []
        translated_dir = self._translate_path(directory)
        collection = self._parse_collection(translated_dir)
        if directory[-1] != "/":
            directory += "/"
        for item in collection:
            ret.append( directory+item['name'] )
        #self.logger.warn(str(ret))
        return ret 
    
    def duplicate(self, path_to_src, path_to_dest): #src might be a directory
        self.logger.debug("duplicating " +path_to_src+" to "+path_to_dest)
        self._raise_error_if_invalid_path(path_to_src)
        self._raise_error_if_invalid_path(path_to_dest)
        if path_to_src[-1] == "/":
            path_to_src = path_to_src[0:-1]
        if path_to_dest[-1] == "/":
            path_to_dest = path_to_dest[0:-1]
        dest_name = os.path.basename(path_to_dest)
        dest_dir  = os.path.dirname(path_to_dest)
        if path_to_dest.startswith(path_to_src) and dest_name == os.path.basename(path_to_src):
            self.logger.warning("cannot copy folder to itself")
            return #DBG raise error
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
                        self.logger.warn("could not duplicate " +path_to_src+" to "+path_to_dest+"\nstatus: %s reason: %s" % (resp.status, resp.reason))
                        if resp.status == 401 or resp.status >= 500:
                            self._reconnect()
        else:
            #if dest exists remove
            if self.exists(path_to_dest):
                self.delete(path_to_dest)
            resp = self.client.duplicate_file(translated_src, translated_dest_dir, dest_name)
            if resp.status < 200 or resp.status >= 300:
                self.logger.warn("could not duplicate " +path_to_src+" to "+path_to_dest+"\nstatus: %s reason: %s" % (resp.status, resp.reason))
                if resp.status == 401 or resp.status >= 500:
                    self._reconnect()

    def _get_metadata(self, path):
        self.logger.debug("getting metadata for "+path)
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
        p=None; resp=None;
        try:
            p =  self._translate_path(path)
        except Exception as e:
            self.logger.debug("getting metadata for  "+path+" failed with: "+str(e))
            
        try:
            resp = self.client.get_file_metadata( p )
        except Exception as e:
            self.logger.debug("getting metadata 5 for "+path+" failed with: "+str(e))
        if resp.status <200 or resp.status >= 300:
            is_file = False
            resp = self.client.get_folder_metadata( self._translate_path(path) )
            if resp.status == 401 or resp.status >= 500:
                self._reconnect()
        if resp.status <200 or resp.status >= 300:
            self.logger.warn("could not get metadata: " +path+"\nstatus: %s reason: %s" % (resp.status, resp.reason))
            raise NoSuchFilesytemObjectError(path, resp.status)
        ret = {}
        if is_file:
            partial_tree = {"file": {"size": "", "lastModified": "", "timeCreated": ""}}
            DictXMLParser().populate_dict_with_XML_leaf_textnodes(resp.data, partial_tree)
            ret["bytes"] = int(partial_tree['file']['size'])
            
            try:#"Sat, 21 Aug 2010 22:31:20 +0000"#2011-05-10T06:18:33.000-07:00     Time conversion error: 2011-05-20T05:15:44.000-07:00
                lastModified = partial_tree['file']['lastModified']
                modified = time.mktime( time.strptime( lastModified[0:-6], "%Y-%m-%dT%H:%M:%S.000") )
                time_offset = time.strptime( lastModified[-5:], "%H:%M") 
                time_delta = 60*60*time_offset.tm_hour + 60*time_offset.tm_min 
                modified += time_delta
                ret["modified"] = modified - self.time_difference
            except Exception as x:
                self.logger.warn("Time conversion error: %s reason: %s" % ( str(partial_tree['file']['lastModified']), str(x)) )
                raise DateParseError("Error parsing modified attribute: %s" % str(x));

            ret["created"] = partial_tree['file']['timeCreated']
            ret["path"] = path
            ret["is_dir"] = False
        else:
            partial_tree = {"folder": {"timeCreated": ""}}
            DictXMLParser().populate_dict_with_XML_leaf_textnodes(resp.data, partial_tree)
            ret["bytes"] = 0
            ret["modified"] = time.time()
            ret["created"] = partial_tree['folder']['timeCreated']
            ret["path"] = path
            ret["is_dir"] = True
        return ret;
            
    def _get_time_difference(self):
        resp =  self.client.user_info()
        return time.mktime( time.strptime(resp.getheader('date'), "%a, %d %b %Y %H:%M:%S GMT") ) - time.time()
    
    def _reconnect(self):
        self.client._reconnect()
        
        
    def get_logging_handler(self):
        return self._logging_handler
