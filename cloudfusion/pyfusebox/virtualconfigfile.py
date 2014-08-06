'''
Created on 24.08.2011
'''
from ConfigParser import SafeConfigParser
from cloudfusion.pyfusebox.virtualfile import VirtualFile
from cloudfusion.store.dropbox import file_decorator
from cloudfusion.store.transparent_caching_store import TransparentMultiprocessingCachingStore
from cloudfusion.store.metadata_caching_store import MetadataCachingStore
import random
from cloudfusion.store.transparent_chunk_caching_store import TransparentChunkMultiprocessingCachingStore

class VirtualConfigFile(VirtualFile):
    INITIAL_TEXT="""
#explanation of config parameter 1
#explanation of config parameter 2
#explanation of config parameter 3

"""
    def __init__(self, path, pyfusebox):
        super( VirtualConfigFile, self ).__init__(path)
        self.pyfusebox = pyfusebox
        
    def get_service_auth_data(self):
        vtf = file_decorator.DataFileWrapper(self.get_text())
        config = SafeConfigParser()
        config.readfp(vtf)
        return dict(config.items('auth'))
    
    def get_store_config_data(self):
        vtf = file_decorator.DataFileWrapper(self.get_text())
        config = SafeConfigParser()
        config.readfp(vtf)
        return dict(config.items('store'))
    
    def write(self, buf, offset):
        written_bytes = super(VirtualConfigFile, self).write(buf, offset)
        if written_bytes >0: # configuration changed
            self._initialize_store()
        return written_bytes
    
    def _initialize_store(self):
        '''Parametrize the store implementation with the settings in the configuration file
        Also, it is determined which wrappers should envelope the store for caching,
        or to provide a monitoring layer. '''
        self.logger.debug("_initialize_store:")
        conf = self.get_store_config_data()
        service = conf['name']
        self.logger.debug("got service name")
        cache_time = int(conf.get('cache', 0))
        type = conf.get('type', '') #chunk
        max_chunk_size = conf.get(('max_chunk_size', 4)) 
        metadata_cache_time = int(conf.get('metadata_cache', 0))
        cache_size = int(conf.get('cache_size', 2000))
        hard_cache_size_limit = int(conf.get('hard_cache_size_limit', 10000))
        cache_id = str(conf.get('cache_id', random.random()))
        cache_dir = str(conf.get('cache_dir', '/tmp/cloudfusion'))
        self.logger.debug("got cache parameter")
        auth = self.get_service_auth_data()
        auth['cache_id'] = cache_id # workaround; Dropbox needs access to cache_id to create a temporary directory with its name, to distinguish sessions
        if 'access_key_id' in auth:
            auth['consumer_key'] = auth['access_key_id']
            auth['consumer_secret'] = auth['secret_access_key']
        bucket_name = auth.get('bucket_name', 'cloudfusion') 
        auth['bucket_name'] = bucket_name 
        self.logger.debug("got auth data: %s" % auth)
        config = auth
        config['cache_dir'] = cache_dir
        store = self.__get_new_store(service, config) #catch error?
        self.logger.debug("initialized store")
        if type != '':                                                      
            store = TransparentChunkMultiprocessingCachingStore( MetadataCachingStore( store,  24*60*60*365), cache_time, cache_size, hard_cache_size_limit, cache_id, max_chunk_size, cache_dir )
        elif cache_time > 0 and metadata_cache_time > 0:
            store = TransparentMultiprocessingCachingStore( MetadataCachingStore( store, metadata_cache_time ), cache_time, cache_size, hard_cache_size_limit, cache_id, cache_dir )
        elif cache_time > 0:
            store = TransparentMultiprocessingCachingStore(store, cache_time, cache_size, hard_cache_size_limit, cache_id, cache_dir)
        elif metadata_cache_time > 0:
            store = MetadataCachingStore( store, metadata_cache_time )
        self.pyfusebox.store = store
        self.logger.debug("initialized service")
        self.pyfusebox.store_initialized = True
        
    def __get_new_store(self, service, auth):
        '''To add a new implementation of :class:`cloudfusion.store.Store`, add an elif branch in the if statement.
        The parameter service is the value of the variable name specified in the configuration file in the [store] section.
        The parameter auth is a dictionary with every variable specified in the configuration file in the [auth] section, such as password and user.
        :param service: The name of the service to be used. I.e. Sugarsync, Dropbox, or Google Storage.
        :param auth: Dictionary of the variable specified in the configuration file's [auth] section.'''
        self.logger.debug("__get_new_store:")
        try:
            if service.lower() == "sugarsync":
                from cloudfusion.store.sugarsync.sugarsync_store import SugarsyncStore
                store = SugarsyncStore(auth)
            elif service.lower() == "gs" or service.find('oogle') >= 0:
                from cloudfusion.store.gs.bulk_get_metadata_google_store import BulkGetMetadataGoogleStore
                store = BulkGetMetadataGoogleStore(auth)
            elif service.lower() == "s3" or service.find('mazon') >= 0:
                from cloudfusion.store.s3.bulk_get_metadata_amazon_store import BulkGetMetadataAmazonStore
                store = BulkGetMetadataAmazonStore(auth)
            elif service.lower() == "webdav" or service.find('dav') >= 0:
                from cloudfusion.store.webdav.bulk_get_metadata_webdav_store import BulkGetMetadataWebdavStore
                store = BulkGetMetadataWebdavStore(auth)
            else: # default
                from cloudfusion.store.dropbox.dropbox_store import DropboxStore
                store = DropboxStore(auth)
            self.logger.debug("got store")
        except Exception as e:
            import traceback
            self.logger.debug(traceback.format_exc())
            raise e
        return store