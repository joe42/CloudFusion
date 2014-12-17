'''
Created on 04.05.2011
'''
import httplib2, requests
from cloudfusion.util.xmlparser import DictXMLParser
from cloudfusion.util.string import *
import base64
import xml.dom.minidom 
from requests.auth import AuthBase

class NoAuth(AuthBase):
    """Do nothing authentication handler, to prevent requests from using credentials stored in the .netrc configuration file."""
    def __call__(self, request):
        return request

class SugarsyncClient(object):
    def __init__(self, config):
        self.host = config["host"]
        self.server_url = config["server_url"]
        id_key = get_id_key(config)
        secret_key = get_secret_key(config)
        self.access_key_id = config[id_key]
        self.private_access_key = config[secret_key] 
        self.username = config["user"]
        self.password = config["password"] 
        self.create_user(self.username, self.password)
        self._reconnect()
    
    def create_user(self, username, password):       
        params = '<?xml version="1.0" encoding="UTF-8" ?><user>    <email>%s</email>    <password>%s</password>    <accessKeyId>%s</accessKeyId>    <privateAccessKey>%s</privateAccessKey></user>' % (username, password, base64.b64decode(self.access_key_id), base64.b64decode(self.private_access_key))
        headers = {"Host": self.host}#send application/xml; charset=UTF-8
        conn = httplib2.Http(timeout=30)
        response, content = conn.request("https://provisioning-api.sugarsync.com/users","POST",params,headers)
        ret = HTTPResponse( response, content )
        return ret
    
    def _reconnect(self):
        response = self.create_token()
        self.token = response.getheader("location")
        partial_tree = {"authorization": {"user": ""}}
        DictXMLParser().populate_dict_with_XML_leaf_textnodes(response.data, partial_tree)
        self.uid = to_str( regSearchString(self.server_url+'user/(.*)', partial_tree['authorization']['user']) )
        
    
    def create_token(self):
        params = '<?xml version="1.0" encoding="UTF-8" ?><authRequest>    <username>%s</username>    <password>%s</password>    <accessKeyId>%s</accessKeyId>    <privateAccessKey>%s</privateAccessKey></authRequest>' % (self.username, self.password, base64.b64decode(self.access_key_id), base64.b64decode(self.private_access_key))
        headers = {"Host": self.host}#send application/xml; charset=UTF-8
        conn = httplib2.Http(timeout=30)
        response, content = conn.request("https://"+self.host+ "/authorization","POST",params,headers)
        ret = HTTPResponse( response, content )
        return ret
    
    def get_syncfolders(self):
        headers = {"Host": self.host, "Authorization: ": self.token}
        conn = httplib2.Http(timeout=30)
        response, content = conn.request("https://"+self.host+"/user/%s/folders/contents" % self.uid,"GET",None,headers)
        http_response = HTTPResponse( response, content )
        dom_response = xml.dom.minidom.parseString(http_response.data)
        dom_collections = dom_response.getElementsByTagName('collection')
        ret = {}
        for dom_collection in dom_collections:
            partial_tree = {"collection": {"displayName": "", "contents": ""}}
            DictXMLParser().populate_dict_with_XML_leaf_textnodes(to_str(dom_collection.toxml()), partial_tree)
            #https://api.sugarsync.com/folder/:sc:7585140:36733670_13234/contents
            user_id = regSearchString(".*:sc:"+self.uid+":(.*)/.*", partial_tree['collection']['contents'])
            displayname = partial_tree['collection']['displayName']
            ret[to_str( displayname )] = to_str( user_id )
        return ret

    
    def user_info(self):
        headers = {"Host": self.host, "Authorization: ": self.token}
        conn = httplib2.Http(timeout=30)
        response, content = conn.request("https://"+self.host+ "/user","GET",None,headers)
        ret = HTTPResponse( response, content )
        return ret
    
    def get_file_metadata(self, path_to_file):
        headers = {"Host": self.host, "Authorization: ": self.token}
        conn = httplib2.Http(timeout=30)
        response, content = conn.request("https://"+self.host+ "/file/:sc:%s:%s" % (self.uid, path_to_file),"GET",None,headers)
        ret = HTTPResponse( response, content )
        return ret
#===============================================================================
#        <?xml version="1.0" encoding="UTF-8"?>
# <file>
#     <displayName>Foo</displayName>
#     <size>120233</size>
#     <lastModified>2009-09-25T16:49:56.000-07:00</lastModified>
#     <timeCreated>2009-09-25T16:49:56.000-07:00</timeCreated>
#     <mediaType>image/jpeg</mediaType>
#     <presentOnServer>true</presentOnServer>
#     <parent>http://api.sugarsync.com/folder/xyzzy</parent>
#     <fileData>http://api.sugarsync.com/file/abc123/data</fileData>
# </file>
#===============================================================================
    def get_dir_listing(self, path, index=0, max=1000000):
        headers = {"Host": self.host, "Authorization: ": self.token}
        conn = httplib2.Http(timeout=30)
        url = "https://"+self.host+ "/folder/:sc:%s:%s/contents" % (self.uid, path)
        parameters = "?max=%s&start=%s&order=name" % (max, index)
        response, content = conn.request(url+parameters,"GET",None,headers)
        ret = HTTPResponse( response, content )
        return ret
    
    def get_folder_metadata(self, path):
        headers = {"Host": self.host, "Authorization: ": self.token}
        conn = httplib2.Http(timeout=30)
        response, content = conn.request("https://"+self.host+ "/folder/:sc:%s:%s" % (self.uid, path),"GET",None,headers)
        ret = HTTPResponse( response, content )
        return ret

#===============================================================================
# <?xml version="1.0" encoding="UTF-8"?>
# <folder>
#    <displayName>folder1</displayName>
#    <timeCreated>2009-06-25T11:31:05.000-07:00</timeCreated>
#    <parent>https://api.sugarsync.com/folder/myfolderId</parent>
#    <collections>https://api.sugarsync.com/folder/myfolderId/contents?type=folder
#    </collections>
#    <files>https://api.sugarsync.com/folder/myfolderId/contents?type=file</files>
#    <contents>https://api.sugarsync.com/folder/myfolderId/contents</contents>
# </folder>
#===============================================================================

    
    def get_file(self, path_to_file):
        headers = {"Host": self.host, "Authorization: ": self.token}
        #metadata = self.get_metadata(path_to_file)
        #partial_tree = {"file": {"displayName": "", "size": "", "lastModified": "", "timeCreated": "", "mediaType": "", "presentOnServer": "", "parent": "", "fileData": ""}}
        #DictXMLParser().populate_dict_with_XML_leaf_textnodes(metadata.data, partial_tree)
        conn = httplib2.Http(timeout=30)
        response, content = conn.request("https://"+self.host+ "/file/:sc:%s:%s/data" % (self.uid, path_to_file),"GET",None,headers)
        ret = HTTPResponse( response, content )
        return ret
    
    def put_file(self, fileobject, path_to_file):
        headers = {"Host": self.host, "Authorization": self.token}
        response = requests.put("https://"+self.host+"/file/:sc:%s:%s/data" % (self.uid, path_to_file), data=fileobject, headers=headers, allow_redirects=True, timeout=30, auth=NoAuth())
        ret = HTTPResponse.get_instance( response.status_code, "No reason given", response.headers, response.content)
        return ret
    
    def put_file_async(self, path_to_src, path_to_dest, response_queue):
        headers = {"Host": self.host, "Authorization": self.token}
        try:
            with open(path_to_src) as fileobject:
                response = requests.put("https://"+self.host+"/file/:sc:%s:%s/data" % (self.uid, path_to_dest), data=fileobject, headers=headers, allow_redirects=True, timeout=30, auth=NoAuth())
            ret = HTTPResponse.get_instance( response.status_code, "No reason given", response.headers, response.content)
        except Exception, e:
            ret = e
        response_queue.put(ret)
    
    def create_file(self, directory, name, mime='text/x-cloudfusion'):
        headers = {"Host": self.host, "Authorization: ": self.token}
        params = '<?xml version="1.0" encoding="UTF-8"?><file><displayName>%s</displayName><mediaType>%s</mediaType></file>' % (name, mime)
        conn = httplib2.Http(timeout=30)
        response, content = conn.request("https://"+self.host+ "/folder/:sc:%s:%s" % (self.uid, directory),"POST",params,headers)
        ret = HTTPResponse( response, content )
        return ret
        
    def delete_file(self, path):
        headers = {"Host": self.host, "Authorization: ": self.token}
        conn = httplib2.Http(timeout=30)
        response, content = conn.request("https://"+self.host+ "/file/:sc:%s:%s" % (self.uid, path),"DELETE",None,headers)
        ret = HTTPResponse( response, content )
        return ret
    
    def delete_folder(self, path):
        headers = {"Host": self.host, "Authorization: ": self.token}
        conn = httplib2.Http(timeout=30)
        response, content = conn.request("https://"+self.host+ "/folder/:sc:%s:%s" % (self.uid, path),"DELETE",None,headers)
        ret = HTTPResponse( response, content )
        return ret
    
    def create_folder(self, directory, name):
        headers = {"Host": self.host, "Authorization: ": self.token}
        params = '<?xml version="1.0" encoding="UTF-8"?><folder><displayName>%s</displayName></folder>' % name
        conn = httplib2.Http(timeout=30)
        response, content = conn.request("https://"+self.host+ "/folder/:sc:%s:%s" % (self.uid, directory),"POST",params,headers)
        ret = HTTPResponse( response, content )

        return ret
    
    def duplicate_file(self, path_to_src, path_to_dest, name):
        headers = {"Host": self.host, "Authorization: ": self.token}
        params = '<?xml version="1.0" encoding="UTF-8"?><fileCopy source="%sfile/:sc:%s:%s">   <displayName>%s</displayName></fileCopy>' % (self.server_url, self.uid, path_to_src, name)
        conn = httplib2.Http(timeout=30)
        response, content = conn.request("https://"+self.host+ "/folder/:sc:%s:%s" % (self.uid, path_to_dest),"POST",params,headers)
        ret = HTTPResponse( response, content )
        return ret
        
    
class HTTPResponse(object):
    def __init__(self, response, data):
        self.data = data
        self.status = response.status
        self.reason = response.reason
        self.headers = response
        
    @classmethod 
    def get_instance(cls, status, reason, headers, data):
        self = cls.__new__(cls)
        self.data = data
        self.status = status
        self.reason = reason
        self.headers = headers
        return self
    
    def __str__(self):
        ret = ''
        if self.status:
            ret += 'Response: %s\n' % self.status
        if self.reason:
            ret += 'Reason: %s\n' % self.reason
        return ret
    
    def getheaders(self):
        return self.headers
   
    def getheader(self, name, default=None):
        if not name in self.headers:
            return default
        return self.headers[name]
