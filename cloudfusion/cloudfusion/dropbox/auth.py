"""
The dropbox.auth module is responsible for making OAuth work for the Dropbox
Client API.  It glues together all the separate parts of the Python OAuth
reference implementation and gives a nicer API to it.  You'll pass a
configure dropbox.auth.Authenticator object to dropbox.client.DropboxClient
in order to work with the API.
"""

import httplib
import urllib
import simplejson as json
from oauth import oauth
from ConfigParser import SafeConfigParser

REALM="No Realm"
HTTP_DEBUG_LEVEL=0

class SimpleOAuthClient(oauth.OAuthClient):
    """
    An implementation of the oauth.OAuthClient class providing OAuth services
    for the Dropbox Client API.  You shouldn't have to use this, but if you need
    to implement your own OAuth, then this is where to look.

    One setting of interest is the HTTP_DEBUG_LEVEL, which you can set to a
    larger number to get detailed HTTP output.
    """
    def __init__(self, server, port=httplib.HTTP_PORT, request_token_url='', access_token_url='', authorization_url=''):
        self.server = server
        self.port = port
        self.request_token_url = request_token_url
        self.access_token_url = access_token_url
        self.authorization_url = authorization_url
        self.connection = httplib.HTTPConnection(self.server, int(self.port))
        self.connection.set_debuglevel(HTTP_DEBUG_LEVEL)

    def fetch_request_token(self, oauth_request):
        """Called by oauth to fetch the request token from Dropbox.  Returns an OAuthToken."""
        self.connection.request(oauth_request.http_method,
                                self.request_token_url,
                                headers=oauth_request.to_header())
        response = self.connection.getresponse()
        data = response.read()
        assert response.status == 200, "Invalid response code %d : %r" % (response.status, data)
        return oauth.OAuthToken.from_string(data)

    def fetch_access_token(self, oauth_request, trusted_url=None):
        """Used to get a access token from Drobpox using the headers.  Returns an OauthToken."""
        url = trusted_url if trusted_url else self.access_token_url

        self.connection.request(oauth_request.http_method, url,
                                headers=oauth_request.to_header()) 

        response = self.connection.getresponse()
        assert response.status == 200, "Invalid response code %d" % response.status
        if trusted_url:
            token = json.loads(response.read())
            token['token'] = str(token['token'])
            token['secret'] = str(token['secret'])
            return oauth.OAuthToken(token['token'], token['secret'])
        else:
            return oauth.OAuthToken.from_string(response.read())

    def authorize_token(self, oauth_request):
        """
        This is not used in the Drobpox API.
        """
        raise NotImplementedError("authorize_token is not implemented via OAuth.")

    def access_resource(self, oauth_request):
        """
        Not used by the Dropbox API.
        """
        raise NotImplementedError("access_resource is not implemented via OAuth.")




class Authenticator(object):
    """
    The Authenticator puts a thin gloss over the oauth.oauth Python library
    so that the dropbox.client.DropboxClient doesn't need to know much about
    your configuration and OAuth operations.

    It uses a configuration file in the standard .ini format that ConfigParser
    understands.  A sample configuration is included in config/testing.ini
    which you should copy and put in your own consumer keys and secrets.

    Because different installations may want to store these configurations
    differently, you aren't required to configure an Authenticator via 
    the .ini method.  As long as you configure it with a dict with the 
    same keys you'll be fine.
    """
    
    def __init__(self, config):
        """
        Configures the Authenticator with all the required settings in config.
        Typically you'll use Authenticator.load_config() to load these from
        a .ini file and then pass the returned dict to here.
        """
        self.client = SimpleOAuthClient(config['server'],
                                        config['port'],
                                        config['request_token_url'], 
                                        config['access_token_url'], 
                                        config['authorization_url'])

        self.trusted_access_token_url = config.get('trusted_access_token_url', None)

        self.consumer = oauth.OAuthConsumer(config['consumer_key'],
                                            config['consumer_secret'])

        self.signature_method_hmac_sha1 = oauth.OAuthSignatureMethod_HMAC_SHA1()

        self.config = config


    @classmethod
    def load_config(self, filename):
        """
        Loads a configuration .ini file, and then pulls out the 'auth' key
        to make a dict you can pass to Authenticator().
        """
        config = SafeConfigParser()
        config_file = open(filename, "r")
        config.readfp(config_file)
        return dict(config.items('auth'))

    def build_authorize_url(self, req_token, callback=None):
        """
        When you send a user to authorize a request token you created, you need
        to make the URL correctly.  This is the method you use.  It will
        return a URL that you can then redirect a user at so they can login to
        Dropbox and approve this request key.
        """
        if callback:
            oauth_callback = "&%s" % urllib.urlencode({'oauth_callback': callback})
        else:
            oauth_callback = ""

        return "%s?oauth_token=%s%s" % (self.config['authorization_url'], req_token.key, oauth_callback)

    
    def obtain_request_token(self):
        """
        This is your first step in the OAuth process.  You call this to get a
        request_token from the Dropbox server that you can then use with
        Authenticator.build_authorize_url() to get the user to authorize it.
        After it's authorized you use this token with
        Authenticator.obtain_access_token() to get an access token.

        NOTE:  You should only need to do this once for each user, and then you
        store the access token for that user for later operations.
        """
        self.oauth_request = oauth.OAuthRequest.from_consumer_and_token(self.consumer,
                                                   http_url=self.client.request_token_url)

        self.oauth_request.sign_request(self.signature_method_hmac_sha1, self.consumer, None)

        token = self.client.fetch_request_token(self.oauth_request)

        return token


    def obtain_access_token(self, token, verifier):
        """
        After you get a request token, and then send the user to the authorize
        URL, you can use the authorized access token with this method to get the
        access token to use for future operations.  Store this access token with 
        the user so that you can reuse it on future operations.

        The verifier parameter is not currently used, but will be enforced in
        the future to follow the 1.0a version of OAuth.  Make it blank for now.
        """
        self.oauth_request = oauth.OAuthRequest.from_consumer_and_token(self.consumer,
                                       token=token,
                                        http_url=self.client.access_token_url,
                                        verifier=verifier)
        self.oauth_request.sign_request(self.signature_method_hmac_sha1, self.consumer, token)

        token = self.client.fetch_access_token(self.oauth_request)

        return token

    def obtain_trusted_access_token(self, user_name, user_password):
        """
        This is for trusted partners using a constrained device such as a mobile
        or other embedded system.  It allows them to use the user's password
        directly to obtain an access token, rather than going through all the
        usual OAuth steps.
        """
        assert user_name, "The user name is required."
        assert user_password, "The user password is required."
        assert self.trusted_access_token_url, "You must set trusted_access_token_url in your config file."
        parameters = {'email': user_name, 'password': user_password}
        params = urllib.urlencode(parameters)
        assert params, "Didn't get a valid params."

        url = self.trusted_access_token_url + "?" + params
        self.oauth_request = oauth.OAuthRequest.from_consumer_and_token(self.consumer, http_url=url, parameters=parameters)
        self.oauth_request.sign_request(self.signature_method_hmac_sha1,
                                        self.consumer, None)
        token = self.client.fetch_access_token(self.oauth_request, url)
        return token

    def build_access_headers(self, method, token, resource_url, parameters, callback=None):
        """
        This is used internally to build all the required OAuth parameters and
        signatures to make an OAuth request.  It's provided for debugging
        purposes.
        """
        params = parameters.copy()

        if callback:
            params['oauth_callback'] = callback

        self.oauth_request = oauth.OAuthRequest.from_consumer_and_token(self.consumer, 
                                    token=token, http_method=method,
                                    http_url=resource_url,
                                                                        parameters=parameters)

        self.oauth_request.sign_request(self.signature_method_hmac_sha1, self.consumer, token)
        return self.oauth_request.to_header(), params

