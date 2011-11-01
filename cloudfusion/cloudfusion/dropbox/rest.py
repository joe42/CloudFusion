"""
A simple JSON REST request abstraction that is used by the
dropbox.client module.  You shouldn't need to use this directly
unless you're implementing unsupport methods.
"""


import httplib
import simplejson as json
import urllib


class RESTClient(object):
    """
    An abstraction on performing JSON REST requests that is used internally
    by the Dropbox Client API.  It provides just enough gear to make requests
    and get responses as JSON data.

    It is not designed well for file uploads.
    """
    
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def request(self, method, url, post_params=None, headers=None, raw_response=False):
        """
        Given the method and url this will make a JSON REST request to the
        configured self.host:self.port and returns a RESTResponse for you.
        If you pass in a dict for post_params then it will urlencode them
        into the body.  If you give in a headers dict then it will add
        those to the request headers.

        The raw_response parameter determines if you get a RESTResponse or a 
        raw HTTPResponse object.  In some cases, like getting a file, you 
        don't want any JSON decoding or extra processing.  In that case set
        this to True and you'll get a plain HTTPResponse.
        """
        params = post_params or {}
        headers = headers or {}

        if params:
            body = urllib.urlencode(params)
        else:
            body = None

        if body:
            headers["Content-type"] = "application/x-www-form-urlencoded"

        conn = httplib.HTTPConnection(self.host, self.port)
        conn.request(method, url, body, headers)

        if raw_response:
            return conn.getresponse()
        else:
            resp = RESTResponse(conn.getresponse())
            conn.close()

        return resp

    def GET(self, url, headers=None):
        """Convenience method that just does a GET request."""
        return self.request("GET", url, headers=headers)

    def POST(self, url, params, headers=None):
        """Convenience method that just does a POST request."""
        return self.request("POST", url, post_params=params, headers=headers)


class RESTResponse(object):
    """
    Returned by dropbox.rest.RESTClient wrapping the base http response
    object to make it more convenient.  It contains the attributes
    http_response, status, reason, body, headers.  If the body can
    be parsed into json, then you get a data attribute too, otherwise
    it's set to None.
    """
    
    def __init__(self, http_resp):
        self.http_response = http_resp
        self.status = http_resp.status
        self.reason = http_resp.reason
        self.body = http_resp.read()
        self.headers = dict(http_resp.getheaders())

        try:
            self.data = json.loads(self.body)
        except ValueError:
            # looks like this isn't json, data is None
            self.data = None



