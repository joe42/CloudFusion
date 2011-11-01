"""
The main client API you'll be working with most often.  You'll need to 
configure a dropbox.client.Authenticator for this to work, but otherwise
it's fairly self-explanatory.
"""

from dropbox import rest
import urllib
import urllib2
import poster
import httplib

API_VERSION=0
HASH_BLOCK_SIZE=10*1024

class DropboxClient(object):
    """
    The main access point of doing REST calls on Dropbox.  You use it
    by first creating and configuring a dropbox.auth.Authenticator,
    and then configuring a DropboxClient to talk to the service.  The
    DropboxClient then does all the work of properly calling each API
    with the correct OAuth authentication.
    """
    

    def __init__(self, api_host, content_host, port, auth, token):
        """
        The api_host and content_host are normally 'api.dropbox.com' and
        'api-content.dropbox.com' and will use the same port.
        The auth is a dropbox.client.Authenticator that is properly configured.
        The token is a valid OAuth `access token` that you got using 
        dropbox.client.Authenticator.obtain_access_token.
        """
        self.api_rest = rest.RESTClient(api_host, port)
        self.content_rest = rest.RESTClient(content_host, port)
        self.auth = auth
        self.token = token
        self.api_host = api_host
        self.content_host = content_host
        self.api_host = api_host
        self.port = int(port)


    def request(self, host, method, target, params, callback):
        """
        This is an internal method used to properly craft the url, headers, and
        params for a Dropbox API request.  It is exposed for you in case you
        need craft other API calls not in this library or you want to debug it.

        It is only expected to work for GET or POST parameters.
        """
        assert method in ['GET','POST'], "Only 'GET' and 'POST' are allowed for method."

        base = self.build_full_url(host, target)
        headers, params = self.auth.build_access_headers(method, self.token, base, params, callback)

        if method == "GET":
            url = self.build_url(target, params)
        else:
            url = self.build_url(target)

        return url, headers, params


    def account_info(self, status_in_response=False, callback=None):
        """
        Retrieve information about the user's account.

        * callback. Optional. The server will wrap its response of format inside a call to the argument specified by callback. Value must contains only alphanumeric characters and underscores.
        * status_in_response. Optional. Some clients (e.g., Flash) cannot handle HTTP status codes well. If this parameter is set to true, the service will always return a 200 status and report the relevant status code via additional information in the response body. Default is false.
        """
        
        params = {'status_in_response': status_in_response}

        url, headers, params = self.request(self.api_host, "GET", "/account/info", params, callback)

        return self.api_rest.GET(url, headers)


    def put_file(self, root, to_path, file_obj):
        """
        Retrieve or upload file contents relative to the user's Dropbox root or
        the application's sandbox directory within the user's Dropbox.

        * root is one of "dropbox" or "sandbox", most clients will use "sandbox".
        * to_path is the `directory` path to put the file (NOT the full path).
        * file_obj is an open and ready to read file object that will be uploaded.

        The filename is taken from the file_obj name currently, so you can't
        have the local file named differently than it's target name.  This may
        change in future versions.

        Finally, this function is not terribly efficient due to Python's
        HTTPConnection requiring all of the file be read into ram for the POST.
        Future versions will avoid this problem.
        """
        assert root in ["dropbox", "sandbox"]

        path = "/files/%s%s" % (root, to_path)

        params = { "file" : file_obj.name, }

        url, headers, params = self.request(self.content_host, "POST", path, params, None)

        params['file'] = file_obj
        data, mp_headers = poster.encode.multipart_encode(params)
        if 'Content-Length' in mp_headers:
            mp_headers['Content-Length'] = str(mp_headers['Content-Length'])
        headers.update(mp_headers)

        conn = httplib.HTTPConnection(self.content_host, self.port)
        conn.request("POST", url, "".join(data), headers)

        resp = rest.RESTResponse(conn.getresponse())
        conn.close()
        file_obj.close()

        return resp
        

    def get_file(self, root, from_path):
        """
        Retrieves a file from the given root ("dropbox" or "sandbox") based on
        from_path as the `full path` to the file.  Unlike the other calls, this
        one returns a raw HTTPResponse with the connection open.  You should 
        do your read and any processing you need and then close it.
        """
        assert root in ["dropbox", "sandbox"]
        
        path = "/files/%s%s" % (root, from_path)

        url, headers, params = self.request(self.content_host, "GET", path, {}, None)
        return self.content_rest.request("GET", url, headers=headers, raw_response=True)


    def file_copy(self, root, from_path, to_path, callback=None):
        """
        Copy a file or folder to a new location.

        * callback. Optional. The server will wrap its response of format inside a call to the argument specified by callback. Value must contains only alphanumeric characters and underscores.
        * from_path. Required. from_path specifies either a file or folder to be copied to the location specified by to_path. This path is interpreted relative to the location specified by root.
        * root. Required. Specify the root relative to which from_path and to_path are specified. Valid values are dropbox and sandbox.
        * to_path. Required. to_path specifies the destination path including the new name for file or folder. This path is interpreted relative to the location specified by root.
        """
        assert root in ["dropbox", "sandbox"]

        params = {'root': root, 'from_path': from_path, 'to_path': to_path}

        url, headers, params = self.request(self.api_host, "POST", "/fileops/copy", params, callback)

        return self.api_rest.POST(url, params, headers)


    def file_create_folder(self, root, path, callback=None):
        """
        Create a folder relative to the user's Dropbox root or the user's application sandbox folder.

        * callback. Optional. The server will wrap its response of format inside a call to the argument specified by callback. Value must contains only alphanumeric characters and underscores.
        * path. Required. The path to the new folder to create, relative to root.
        * root. Required. Specify the root relative to which path is specified. Valid values are dropbox and sandbox.
        """
        assert root in ["dropbox", "sandbox"]
        params = {'root': root, 'path': path}

        url, headers, params = self.request(self.api_host, "POST", "/fileops/create_folder", params, callback)

        return self.api_rest.POST(url, params, headers)


    def file_delete(self, root, path, callback=None):
        """
        Delete a file or folder.

        * callback. Optional. The server will wrap its response of format inside a call to the argument specified by callback. Value must contains only alphanumeric characters and underscores.
        * path. Required. path specifies either a file or folder to be deleted. This path is interpreted relative to the location specified by root.
        * root. Required. Specify the root relative to which path is specified. Valid values are dropbox and sandbox.
        """
        assert root in ["dropbox", "sandbox"]

        params = {'root': root, 'path': path}

        url, headers, params = self.request(self.api_host, "POST", "/fileops/delete", params,
                                           callback)

        return self.api_rest.POST(url, params, headers)


    def file_move(self, root, from_path, to_path, callback=None):
        """
        Move a file or folder to a new location.

        * callback. Optional. The server will wrap its response of format inside a call to the argument specified by callback. Value must contains only alphanumeric characters and underscores.
        * from_path. Required. from_path specifies either a file or folder to be copied to the location specified by to_path. This path is interpreted relative to the location specified by root.
        * root. Required. Specify the root relative to which from_path and to_path are specified. Valid values are dropbox and sandbox.
        * to_path. Required. to_path specifies the destination path including the new name for file or folder. This path is interpreted relative to the location specified by root.
        """
        assert root in ["dropbox", "sandbox"]

        params = {'root': root, 'from_path': from_path, 'to_path': to_path}

        url, headers, params = self.request(self.api_host, "POST", "/fileops/move", params, callback)

        return self.api_rest.POST(url, params, headers)


    def metadata(self, root, path, file_limit=10000, hash=None, list=True, status_in_response=False, callback=None):
        """
        The metadata API location provides the ability to retrieve file and
        folder metadata and manipulate the directory structure by moving or
        deleting files and folders.

        * callback. Optional. The server will wrap its response of format inside a call to the argument specified by callback. Value must contains only alphanumeric characters and underscores.
        * file_limit. Optional. Default is 10000. When listing a directory, the service will not report listings containing more than file_limit files and will instead respond with a 406 (Not Acceptable) status response.
        * hash. Optional. Listing return values include a hash representing the state of the directory's contents. If you provide this argument to the metadata call, you give the service an opportunity to respond with a "304 Not Modified" status code instead of a full (potentially very large) directory listing. This argument is ignored if the specified path is associated with a file or if list=false.
        * list. Optional. The strings true and false are valid values. true is the default. If true, this call returns a list of metadata representations for the contents of the directory. If false, this call returns the metadata for the directory itself.
        * status_in_response. Optional. Some clients (e.g., Flash) cannot handle HTTP status codes well. If this parameter is set to true, the service will always return a 200 status and report the relevant status code via additional information in the response body. Default is false.
        """ 

        assert root in ["dropbox", "sandbox"]

        path = "/metadata/%s%s" % (root, path)

        params = {'file_limit': file_limit,
                  'list': "true" if list else "false",
                  'status_in_response': status_in_response}
        if hash is not None:
            params['hash'] = hash

        url, headers, params = self.request(self.api_host, "GET", path, params, callback)

        return self.api_rest.GET(url, headers)

    def links(self, root, path):
        assert root in ["dropbox", "sandbox"]
        path = "/links/%s%s" % (root, path)
        return self.build_full_url(self.api_host, path)


    def build_url(self, url, params=None):
        """Used internally to build the proper URL from parameters and the API_VERSION."""
        if type(url) == unicode:
            url = url.encode("utf8")
        target_path = urllib2.quote(url)

        if params:
            return "/%d%s?%s" % (API_VERSION, target_path, urllib.urlencode(params))
        else:
            return "/%d%s" % (API_VERSION, target_path)


    def build_full_url(self, host, target):
        """Used internally to construct the complete URL to the service."""
        port = "" if self.port == 80 else ":%d" % self.port
        base_full_url = "http://%s%s" % (host, port)
        return base_full_url + self.build_url(target)


    def account(self, email='', password='', first_name='', last_name='', source=None):
        params = {'email': email, 'password': password,
                  'first_name': first_name, 'last_name': last_name}

        url, headers, params = self.request(self.api_host, "POST", "/account",
                                            params, None)

        return self.api_rest.POST(url, params, headers)

    
    def thumbnail(self, root, from_path, size='small'):
        assert root in ["dropbox", "sandbox"]
        assert size in ['small','medium','large']
        
        path = "/thumbnails/%s%s" % (root, from_path)

        url, headers, params = self.request(self.content_host, "GET", path,
                                            {'size': size}, None)
        return self.content_rest.request("GET", url, headers=headers, raw_response=True)

