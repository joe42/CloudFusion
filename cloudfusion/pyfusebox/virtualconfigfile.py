'''
Created on 24.08.2011
'''
from ConfigParser import SafeConfigParser
from cloudfusion.pyfusebox.virtualfile import VirtualFile
from cloudfusion.util import file_decorator
from cloudfusion.store.transparent_caching_store import TransparentMultiprocessingCachingStore
from cloudfusion.store.metadata_caching_store import MetadataCachingStore
import random
from cloudfusion.store.transparent_chunk_caching_store import TransparentChunkMultiprocessingCachingStore
import subprocess
from subprocess import PIPE
import os
from os.path import expanduser
from cloudfusion.store.caching_store import ENABLE_PROFILING
from cloudfusion.util.string import get_id_key, get_secret_key


class VirtualConfigFile(VirtualFile):
    '''Responsible for the (re)configuration of :class:`cloudfusion.pyfusebox.ConfigurablePyFuseBox`,
    everytime it contents is written to it.'''
    INITIAL_TEXT="""
#explanation of config parameter 1
#explanation of config parameter 2
#explanation of config parameter 3

"""
    def __init__(self, path, pyfusebox):
        super( VirtualConfigFile, self ).__init__(path)
        self.pyfusebox = pyfusebox
        self._recently_registered_name = ''
        
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
            if not self.pyfusebox.store_initialized:
                self.auto_register()
                self._initialize_store()
            else:
                self._reconfigure_store()
        return written_bytes
    
    def _reconfigure_store(self):
        self.logger.debug("change store configuration")
        conf = self.get_store_config_data()
        enable_logging = conf.get('enable_logging', None)
        if enable_logging != None:
            enable_logging = enable_logging.lower() in ['true', '1', 'y', 'yes']
            if enable_logging:
                self.logger.debug("enable logging")
                self.pyfusebox.enable_logging()
            else:
                self.logger.debug("disable logging")
                self.pyfusebox.disable_logging()
        enable_profiling = conf.get(ENABLE_PROFILING, None)
        if enable_profiling != None:
            enable_profiling = enable_profiling.lower() in ['true', '1', 'y', 'yes']
            conf[ENABLE_PROFILING] = enable_profiling
            self.pyfusebox.store.set_configuration(conf)
    
    def auto_register(self):
        conf = self.get_store_config_data()
        service = conf['name']
        auth = self.get_service_auth_data()
        if 'user' in auth and self._recently_registered_name != auth['user'] and \
                'autoregister' in conf and conf['autoregister'].lower() == "true":
            ABS_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            
            JAVA = '/usr/bin/java'
            HOME = expanduser("~")
            CLASSPATH = ['/usr/share/java/jna.jar',
                         '/usr/share/java/asm3.jar',
                         '/usr/share/java/asm3-commons.jar',
                         '/usr/share/java/antlr3-runtime.jar',
                         '/usr/share/java/libconstantine-java.jar',
                         '/usr/share/java/jython.jar',
                         '/usr/share/java/sikuli-script.jar',
                         '/usr/share/maven-repo/com/google/guava/guava/debian/guava-debian.jar',
                         '/usr/share/maven-repo/org/jruby/ext/posix/jnr-posix/debian/jnr-posix-debian.jar',
                         '/usr/share/java/jaffl.jar',
                         '/usr/share/java/jna.jar',
                         '/usr/share/maven-repo/jline/jline/1.0/jline-1.0.jar',
                         ]
            PARAMETERS = ['-Dfile.encoding=UTF-8',
                          '-Dpython.home=/usr/share/jython',
                          '-Dsikuli.console=true',
                          '-Dpython.path="/usr/share/sikuli/Lib"',
                          "-Dpython.cachedir=\"%s/.jython-cache\"" % HOME,
                          ]
            JYTHON = '%s -cp "%s" %s org.python.util.jython ' % (JAVA, ':'.join(CLASSPATH), ' '.join(PARAMETERS))
            if service.lower() == "dropbox" or service.lower() == "db":
                self.logger.debug("auto registration")
                SCRIPT_PATH = ABS_PATH + '/autoregistration/dropbox_autoregistration.py'
                p = subprocess.Popen(['%s "%s"' % (JYTHON, SCRIPT_PATH)], stdin=PIPE, shell=True)
                p.stdin.write(auth['user']+"\n")
                p.stdin.write(auth['password']+"\n")
                p.communicate() #wait for process to exit
            elif 'url' in auth and auth['url'].find('t-online') != -1: #url = https://webdav.mediencenter.t-online.de:443
                self.logger.debug("auto registration")
                SCRIPT_PATH = ABS_PATH + '/autoregistration/tonline_autoregistration.py'
                p = subprocess.Popen(['%s "%s"' % (JYTHON, SCRIPT_PATH)], stdin=PIPE, shell=True)
                p.stdin.write(auth['user']+"\n")
                p.stdin.write(auth['password']+"\n")
                p.communicate() #wait for process to exit
            self._recently_registered_name = auth['user'] #store if you are already registered
    
    def _unify_auth(self, auth):
        '''Add id and secret.'''
        id_key = get_id_key(auth)
        secret_key = get_secret_key(auth)
        if id_key and secret_key:
            auth['id'] = auth[id_key]
            auth['secret'] = auth[secret_key] 

    def _initialize_store(self):
        '''Parametrize the store implementation with the settings in the configuration file
        Also, it is determined which wrappers should envelope the store for caching,
        or to provide a monitoring layer. '''
        self.logger.debug("_initialize_store:")
        conf = self.get_store_config_data()
        service = conf['name']
        self.logger.debug("got service name")
        cache_time = int(conf.get('cache', 240))
        type = conf.get('type', '') #chunk
        max_chunk_size = conf.get(('max_chunk_size', 4)) 
        metadata_cache_time = int(conf.get('metadata_cache', 0))
        cache_size = int(conf.get('cache_size', 2000))
        hard_cache_size_limit = int(conf.get('hard_cache_size_limit', 10000))
        cache_id = str(conf.get('cache_id', random.random()))
        cache_dir = str(conf.get('cache_dir', os.path.expanduser("~")+'/.cache/cloudfusion'))
        cache_dir = cache_dir[:-1] if cache_dir[-1] == '/' else cache_dir # remove slash at the end
        self.logger.debug("got cache parameter")
        auth = self.get_service_auth_data()
        self._unify_auth(auth)
        auth['cache_id'] = cache_id # workaround; Dropbox needs access to cache_id to create a temporary directory with its name, to distinguish sessions
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
            elif service.find('rive') >= 0:
                from cloudfusion.store.gdrive.google_drive import GoogleDrive
                store = GoogleDrive(auth)
            elif service.lower() == "gs" or service.find('oogle') >= 0:
                from cloudfusion.store.gs.bulk_get_metadata_google_store import BulkGetMetadataGoogleStore
                store = BulkGetMetadataGoogleStore(auth)
            elif service.lower() == "s3" or service.find('mazon') >= 0:
                from cloudfusion.store.s3.bulk_get_metadata_amazon_store import BulkGetMetadataAmazonStore
                store = BulkGetMetadataAmazonStore(auth)
            elif service.lower() == "webdav" or service.find('dav') >= 0:
                from cloudfusion.store.webdav.bulk_get_metadata_webdav_store import BulkGetMetadataWebdavStore
                store = BulkGetMetadataWebdavStore(auth)
            elif service.lower() == "local" or service.lower() == "hdd" or service.lower() == "disk":
                from cloudfusion.store.local_drive.local_hd_store import LocalHDStore
                store = LocalHDStore(auth)
            else: # default
                from cloudfusion.store.dropbox.dropbox_store import DropboxStore
                store = DropboxStore(auth)
            self.logger.debug("got store")
        except Exception as e:
            import traceback
            self.logger.debug(traceback.format_exc())
            raise e
        return store
