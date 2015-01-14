'''
Module to test all implementations of the :class:`Store` interface, 
as well as the extension interface :class:`BulkGetMetadata`.
The actual test only tests one service; For instance test_dropbox
only tests the Store implementation :class:`DropboxStore`, but it might
also test integration with various wrapper classes like :class:`MetadataCachingStore`
or :class:`MultiprocessingCachingStore`.

The tests can be executed in parallel by calling each test individually
in a separate nosetests process like this:

nosetests -v -s -x --logging-filter=dropbox cloudfusion.tests.store_tests:test_dropbox &

nosetests -v -s -x --logging-filter=sugarsync cloudfusion.tests.store_tests:test_sugarsync &

nosetests -v -s -x --logging-filter=harddrive cloudfusion.tests.store_tests:test_local &

nosetests -v -s -x --logging-filter=amazon cloudfusion.tests.store_tests:test_amazon &

nosetests -v -s -x --logging-filter=google cloudfusion.tests.store_tests:test_google &

nosetests -v -s -x --logging-filter=google_drive cloudfusion.tests.store_tests:test_gdrive &

nosetests -v -s -x --logging-filter=webdav cloudfusion.tests.store_tests:test_webdav_tonline &

nosetests -v -s -x --logging-filter=webdav cloudfusion.tests.store_tests:test_webdav_gmx &

nosetests -v -s -x --logging-filter=webdav cloudfusion.tests.store_tests:test_webdav_box &

nosetests -v -s -x --logging-filter=webdav cloudfusion.tests.store_tests:test_webdav_yandex &


Created on Dec 18, 2014

@author: joe
'''

from cloudfusion.store.dropbox.dropbox_store import DropboxStore
from cloudfusion.store.sugarsync.sugarsync_store import SugarsyncStore
from functools import partial
from nose.tools import *
from cloudfusion.store.store import *
from cloudfusion.store.caching_store import MultiprocessingCachingStore
from cloudfusion.store.metadata_caching_store import MetadataCachingStore
import os.path, time
import tempfile
from ConfigParser import SafeConfigParser
import cloudfusion
from cloudfusion.store.chunk_caching_store import ChunkMultiprocessingCachingStore
from cloudfusion.store.bulk_get_metadata import BulkGetMetadata
from cloudfusion.tests.config import *
from cloudfusion.tests.path_name import PathName
from cloudfusion.store.webdav.bulk_get_metadata_webdav_store import BulkGetMetadataWebdavStore
from cloudfusion.store.local_drive.local_hd_store import LocalHDStore
from cloudfusion.store.s3.amazon_store import AmazonStore
from cloudfusion.store.s3.bulk_get_metadata_amazon_store import BulkGetMetadataAmazonStore
from cloudfusion.store.gs.bulk_get_metadata_google_store import BulkGetMetadataGoogleStore
from cloudfusion.store.gdrive.google_drive import GoogleDrive
from cloudfusion.store.transparent_chunk_caching_store import TransparentChunkMultiprocessingCachingStore
from cloudfusion.store.transparent_caching_store import TransparentMultiprocessingCachingStore

pathname = PathName()
store = None

def teardown_func():
    try:
        store.delete(pathname.testdir().get_path(), True)
    except Exception, e:
        pass

@with_setup(teardown=teardown_func)
def test_sugarsync():
    global store
    pathname.set_utf8()
    config = get_sugarsync_config()
    store = SugarsyncStore(config)
    metadatacache_store = ChunkMultiprocessingCachingStore( SugarsyncStore(config) )
    for test in _generate_store_tests(store, "SugarsyncStore"):
        yield test
    for test in _generate_store_tests(metadatacache_store, "ChunkCache SugarsyncStore"):
        yield test

@with_setup(teardown=teardown_func)
def test_dropbox():
    global store
    pathname.set_restricted_utf8()
    config = get_dropbox_config()
    store = DropboxStore(config)
    metadatacache_store = MetadataCachingStore( store )
    for test in _generate_store_tests(store, "DropboxStore"):
        yield test
    for test in _generate_store_tests(metadatacache_store, "MetaDataCache DropboxStore"):
        yield test
    
@with_setup(teardown=teardown_func)
def test_webdav_tonline():
    global store
    pathname.set_utf8()
    config = get_webdav_tonline_config()
    store = BulkGetMetadataWebdavStore(config)
    for test in _generate_store_tests(store, "BulkGetMetadataWebdavStore tonline"):
        yield test
    for test in _generate_bulk_get_metadata_tests(store, "BulkGetMetadataWebdavStore tonline"):
        yield test

@with_setup(teardown=teardown_func)
def test_webdav_gmx():
    global store
    pathname.set_restricted_utf8()
    config = get_webdav_gmx_config()
    store = BulkGetMetadataWebdavStore(config)
    metadatacache_store = MetadataCachingStore( store )
    transparent_store = TransparentMultiprocessingCachingStore( MetadataCachingStore(store) )
    for test in _generate_store_tests(store, "BulkGetMetadataWebdavStore gmx", 
                                      include_space_tests=False):
        yield test
    for test in _generate_bulk_get_metadata_tests(store, "BulkGetMetadataWebdavStore gmx"):
        yield test
    for test in _generate_store_tests(metadatacache_store , "MetaDataCache BulkGetMetadataWebdavStore gmx", 
                                      include_space_tests=False):
        yield test
    for test in _generate_store_tests(transparent_store, "TransparentCachingStore MetadataCache gmx", 
                                      include_space_tests=False):
        yield test

@with_setup(teardown=teardown_func)
def test_webdav_yandex():
    global store
    pathname.set_utf8()
    config = get_webdav_yandex_config()
    store = BulkGetMetadataWebdavStore(config)
    transparent_store = TransparentChunkMultiprocessingCachingStore( MetadataCachingStore(store) )
    for test in _generate_store_tests(store, "BulkGetMetadataWebdavStore yandex"):
        yield test
    for test in _generate_bulk_get_metadata_tests(store, "BulkGetMetadataWebdavStore yandex"):
        yield test
    for test in _generate_store_tests(transparent_store, "TransparentChunkCachingStore MetadataCache yandex", 
                                      include_space_tests=False):
        yield test
 
@with_setup(teardown=teardown_func)
def test_webdav_box():
    global store
    pathname.set_restricted_utf8()
    config = get_webdav_box_config()
    store = BulkGetMetadataWebdavStore(config)
    transparent_store = TransparentChunkMultiprocessingCachingStore(store)
    for test in _generate_store_tests(store, "BulkGetMetadataWebdavStore box"):
        yield test
    for test in _generate_bulk_get_metadata_tests(store, "BulkGetMetadataWebdavStore box"):
        yield test
    for test in _generate_store_tests(transparent_store, "TransparentChunkCachingStore Box", 
                                      include_space_tests=False):
        yield test

@with_setup(teardown=teardown_func)
def test_local():
    global store
    pathname.set_utf8()
    config = get_local_config()
    store = LocalHDStore(config)
    metadatacache_store = MetadataCachingStore( store )
    transparent_store = TransparentMultiprocessingCachingStore(store)
    transparent_metacache_store = TransparentMultiprocessingCachingStore( MetadataCachingStore(store) )
    transparent_chunk_store = TransparentChunkMultiprocessingCachingStore(store)
    transparent_chunk_metacache_store = TransparentChunkMultiprocessingCachingStore( MetadataCachingStore(store) )
    for test in _generate_store_tests(store, "LocalHDStore", include_space_tests=False):
        yield test
    for test in _generate_store_tests(metadatacache_store, 
                                      "MetaDataCache LocalHDStore", include_space_tests=False):
        yield test
    for test in _generate_store_tests(transparent_store, 
                                      "TransparentCachingStore LocalHDStore", 
                                      include_space_tests=False):
        yield test
    for test in _generate_store_tests(transparent_metacache_store, 
                                      "TransparentCachingStore MetaDataCache LocalHDStore", 
                                      include_space_tests=False):
        yield test
    for test in _generate_store_tests(transparent_chunk_store, 
                                      "TransparentChunkCachingStore LocalHDStore", 
                                      include_space_tests=False):
        yield test
    for test in _generate_store_tests(transparent_chunk_metacache_store, 
                                      "TransparentChunkCachingStore MetaDataCache LocalHDStore", 
                                      include_space_tests=False):
        yield test
        
@with_setup(teardown=teardown_func)
def test_amazon():
    global store
    pathname.set_utf8()
    config = get_amazon_config()
    store = BulkGetMetadataAmazonStore(config)
    metadatacache_store = MetadataCachingStore( store )
    for test in _generate_store_tests(store, "BulkGetMetadataAmazonStore"):
        yield test
    for test in _generate_bulk_get_metadata_tests(store, "BulkGetMetadataAmazonStore"):
        yield test
    for test in _generate_store_tests(metadatacache_store , "MetaDataCache BulkGetMetadataAmazonStore", 
                                      include_space_tests=False):
        yield test
        
@with_setup(teardown=teardown_func)
def test_google():
    global store
    pathname.set_utf8()
    config = get_google_config()
    store = BulkGetMetadataGoogleStore(config)
    metadatacache_store = MetadataCachingStore( store )
    for test in _generate_store_tests(store, "BulkGetMetadataGoogleStore"):
        yield test
    for test in _generate_bulk_get_metadata_tests(store, "BulkGetMetadataAmazonStore"):
        yield test
    for test in _generate_store_tests(metadatacache_store , "MetaDataCache BulkGetMetadataGoogleStore", 
                                      include_space_tests=False):
        yield test

@with_setup(teardown=teardown_func)
def test_gdrive():
    global store
    pathname.set_utf8()
    config = get_gdrive_config()
    store = GoogleDrive(config)
    metadatacache_store = MetadataCachingStore( store )
    chunkcache_store = ChunkMultiprocessingCachingStore( store )
    for test in _generate_store_tests(store, "GoogleDrive"):
        yield test
    for test in _generate_store_tests(chunkcache_store, "ChunkCachingStore GoogleDrive"):
        yield test
    for test in _generate_store_tests(metadatacache_store , "MetaDataCache GoogleDrive", 
                                      include_space_tests=False):
        yield test
    
def _create_test_directory(store):
    try:
        store.create_directory(pathname.MAINDIR)
    except AlreadyExistsError:
        pass
    try:
        store.create_directory(pathname.testdir().get_path())
    except AlreadyExistsError:
        pass

def _generate_bulk_get_metadata_tests(store, description_of_store):
    _create_test_directory(store)
    test = partial(_test_bulk_get_metadata, store)
    test.description = description_of_store+": get bulk metadata"
    yield (test, ) 
    store.delete(pathname.testdir().get_path(), True)

def _generate_store_tests(store, description_of_store, include_space_tests=True):
    '''Generate general tests for store.
    :param store: The object to test.
    :type store: :class:`Store`
    :param description_of_store: String to describe the store instance.
    :param include_space_tests: Indicates if tests about the free, used, and overall space should be executed.
    :type include_space_tests: boolean'''
    store.delete(pathname.testdir().get_path(), True)
    _create_test_directory(store)
    if include_space_tests:
        test = partial(_test_get_free_space, store)
        test.description = description_of_store+": getting free space"
        yield (test, )
        test = partial(_test_get_overall_space, store)
        test.description = description_of_store+": getting overall space"
        yield (test, )
        test = partial(_test_get_used_space, store)
        test.description = description_of_store+": getting used space"
        yield (test, )
    test = partial(_test_fail_on_is_dir, store)
    test.description = description_of_store+": fail on determining if file system object is a file or a directory"
    yield (test, ) 
    test = partial(_test_fail_on_get_bytes, store)
    test.description = description_of_store+": fail on getting number of bytes from file"
    yield (test, ) 
    test = partial(_test_fail_on_get_modified, store)
    test.description = description_of_store+": fail on getting modified time"
    yield (test, ) 
    test = partial(_test_create_delete_directory, store)
    test.description = description_of_store+": creating and deleting directory"
    yield (test, )
    test = partial(_test_store_delete_file, store)
    test.description = description_of_store+": creating and deleting file"
    yield (test, )
    test = partial(_test_get_file, store)
    test.description = description_of_store+": getting file"
    yield (test, )
    test = partial(_test_duplicate, store)
    test.description = description_of_store+": copying (duplicating) file and directory"
    yield (test, )
    test = partial(_test_move_directory, store)
    test.description = description_of_store+": moving directory"
    yield (test, )
    test = partial(_test_move_file, store)
    test.description = description_of_store+": moving file"
    yield (test, )
    test = partial(_test_get_bytes, store)
    test.description = description_of_store+": getting number of bytes from file"
    yield (test, )
    test = partial(_test_is_dir, store)
    test.description = description_of_store+": determine if file system object is a file or a directory"
    yield (test, )
    test = partial(_test_account_info, store)
    test.description = description_of_store+": getting account info"
    yield (test, )
    test = partial(_test_get_modified, store)
    test.description = description_of_store+": getting modified time"
    yield (test, )
    test = partial(_test_get_directory_listing, store)
    test.description = description_of_store+": getting directory listing"
    yield (test, )  
    test = partial(_test_exists, store)
    test.description = description_of_store+": determine if file and directory exists"
    yield (test, )  
    test = partial(_test_nested_move, store)
    test.description = description_of_store+": move nested files"
    yield (test, ) 
    _create_test_directory(store)
    test = partial(_test_nested_duplicate, store)
    test.description = description_of_store+": duplicate nested files"
    yield (test, ) 
    store.delete(pathname.testdir().get_path(), True)

def _assert_all_in(in_list, all_list):
    assert all(item in in_list for item in all_list), "expected all items in %s to be found in %s" % (all_list, in_list)
    
def _test_get_file(store):
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path(), pathname.file().get_filename())
    first_resp = store.get_file(pathname.get_path())
    second_resp = store.get_file(pathname.get_path())
    store.delete(pathname.testdir().file().get_path(), is_dir=False)
    assert first_resp == second_resp, "first response should be same as second response, but %s != %s" % (first_resp, second_resp)
    with open(pathname.LOCAL_TESTFILE_PATH) as file:
        assert file.read() == first_resp, "Remote file differs from the local file."
            
def _test_fail_on_is_dir(store): 
    assert_raises(NoSuchFilesytemObjectError, store.is_dir, pathname.testdir().file('non existent file').get_path())
    assert_raises(NoSuchFilesytemObjectError, store.is_dir, pathname.testdir().file('non existent dir').get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    store.delete(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME, False)
    store.create_directory(pathname.testdir().file('deleted dir').get_path())
    store.delete(pathname.testdir().file('deleted dir').get_path(), True)
    assert_raises(NoSuchFilesytemObjectError, store.is_dir, pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME)
    assert_raises(NoSuchFilesytemObjectError, store.is_dir, pathname.testdir().file('deleted dir').get_path())
        
def _test_fail_on_get_bytes(store):
    assert_raises(NoSuchFilesytemObjectError, store.get_bytes, pathname.testdir().file('non existent file').get_path())
    assert_raises(NoSuchFilesytemObjectError, store.get_bytes, pathname.testdir().file('non existent dir').get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    store.delete(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME, False)
    store.create_directory(pathname.testdir().file('deleted dir').get_path())
    store.delete(pathname.testdir().file('deleted dir').get_path(), True)
    assert_raises(NoSuchFilesytemObjectError, store.get_bytes, pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME)
    assert_raises(NoSuchFilesytemObjectError, store.get_bytes, pathname.testdir().file('deleted dir').get_path())
    
def _test_fail_on_get_modified(store):
    assert_raises(NoSuchFilesytemObjectError, store.get_modified, pathname.testdir().file('non existent file').get_path())
    assert_raises(NoSuchFilesytemObjectError, store.get_modified, pathname.testdir().file('non existent dir').get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    store.delete(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME, False)
    store.create_directory(pathname.testdir().file('deleted dir').get_path())
    store.delete(pathname.testdir().file('deleted dir').get_path(), True)
    assert_raises(NoSuchFilesytemObjectError, store.get_modified, pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME)
    assert_raises(NoSuchFilesytemObjectError, store.get_modified, pathname.testdir().file('deleted dir').get_path())

def _test_get_bytes(store):
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path()) 
    res = store.get_bytes(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME)
    store.delete(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME, False)
    assert res > 0 and res < 10, "stored file should be between one and ten bytes big, but has a size of %s bytes" % res

def _test_is_dir(store):
    assert store.is_dir(pathname.testdir().get_path()) == True
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    assert store.is_dir(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME) == False 
    store.delete(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME, False)
        
def _test_account_info(store):
    assert type(store.account_info()) == str
            
def _test_get_modified(store):
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    file_modified_time = int(store.get_modified(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME))
    now_time = time.time()
    store.delete(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME, False)
    assert _assert_equal_with_variance( file_modified_time, now_time, 15, "modified time stamp of copied file is off by %s seconds" %  abs(file_modified_time-now_time) )
    store.create_directory(pathname.testdir().dir('modified dir').get_path())
    dir_modified_time = store.get_modified(pathname.testdir().dir('modified dir').get_path())
    now_time = time.time()
    store.delete(pathname.testdir().dir('modified dir').get_path(), True)
    assert _assert_equal_with_variance( dir_modified_time, now_time, 15, "modified time stamp of copied file is off by %s seconds" %  abs(dir_modified_time-now_time) )
    #assert not store.is_dir(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_PATH) 

def _assert_equal_with_variance(val1, val2, variance, msg =""):
    return (val1<=val2+variance) and (val1>=val2-variance), msg
        

def _test_get_free_space(store):
    free_space = store.get_free_space()
    used_space = store.get_used_space()
    overall_space = store.get_overall_space()
    assert abs(free_space - (overall_space - used_space)) < 6000, "free space should amount to overall space minus used space (%s) but is %s" % (overall_space - used_space, free_space)

def _test_get_overall_space(store):
    overall_space = store.get_overall_space()
    try: 
        int(overall_space)
    except Exception as e:
        assert False, "exception on getting overall space: "+str(e) 
        
def _test_get_used_space(store):
    used_space = store.get_used_space()
    try: 
        int(used_space)
    except Exception as e:
        assert False, "exception on getting used space"+str(e)  

def _test_get_directory_listing(store): 
    _create_directories(store, pathname.testdir().get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    listing = store.get_directory_listing(pathname.testdir().get_path())
    cached_listing1 = store.get_directory_listing(pathname.testdir().get_path())
    cached_listing2 = store.get_directory_listing(pathname.testdir().get_path())
    _delete_directories(store, pathname.testdir().get_path())
    _delete_file(store, pathname.LOCAL_TESTFILE_NAME, pathname.testdir().get_path())
    root = pathname.testdir().get_path()+"/"
    _assert_all_in(listing, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+pathname.LOCAL_TESTFILE_NAME]) 
    _assert_all_in(cached_listing1, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+pathname.LOCAL_TESTFILE_NAME]) 
    _assert_all_in(cached_listing2, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+pathname.LOCAL_TESTFILE_NAME]) 

def _test_bulk_get_metadata(store): 
    if not isinstance(store, BulkGetMetadata):
        return
    _create_directories(store, pathname.testdir().get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path()) #testfile
    time.sleep(5) #wait for file to be stored (eventual consistency)
    metadata = store.get_bulk_metadata(pathname.testdir().get_path())
    cached_metadata1 = store.get_bulk_metadata(pathname.testdir().get_path())
    cached_metadata2 = store.get_bulk_metadata(pathname.testdir().get_path())
    _delete_directories(store, pathname.testdir().get_path())
    _delete_file(store, pathname.LOCAL_TESTFILE_NAME, pathname.testdir().get_path())
    root = pathname.testdir().get_path()+"/"
    for path in [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+pathname.LOCAL_TESTFILE_NAME]:
        for metadata in [metadata, cached_metadata1, cached_metadata2]:
            if path != root+pathname.LOCAL_TESTFILE_NAME:
                assert metadata[path]['is_dir'] == True
                assert metadata[path]['bytes'] == 0
                assert 'modified' in metadata[path]
            else:
                assert metadata[path]['is_dir'] == False
                assert metadata[path]['bytes'] > 0 and metadata[path]['bytes'] < 10, "stored file should be between one and ten bytes big, but has a size of %s bytes" % metadata[path]['bytes']
                assert 'modified' in metadata[path]  

def _test_move_directory(store):
    source = pathname.copy().testdir().dir('source dir')
    dest = pathname.copy().testdir().dir('destination dir')
    store.create_directory(source.get_path())
    store.move(source.get_path(), dest.get_path())
    assert _dir_exists(store, dest.get_path())
    store.delete(dest.get_path(), True)
            
def _test_move_file(store):
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    assert store.exists(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME) 
    store.move(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME, pathname.testdir().file('moved file').get_path())
    assert store.exists(pathname.testdir().file('moved file').get_path()) 
    store.delete(pathname.testdir().file('moved file').get_path(), False)
    
def _test_nested_move(store):
    source = pathname.copy().testdir().dir('source dir')
    dest = pathname.copy().testdir().dir('destination dir')
    store.create_directory(source.get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, source.get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, source.get_path(), remote_file_name=source.copy().file(nr=2).get_filename())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, source.get_path(), remote_file_name=source.copy().file(nr=3).get_filename())
    store.create_directory(source.copy().dir('nested dir').get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, source.copy().dir('nested dir').get_path(), remote_file_name=source.copy().file().get_filename())
    store.move(source.get_path(), dest.get_path())
    assert _dir_exists(store, dest.copy().dir('nested dir').get_path())
    assert store.exists(dest.copy().dir('nested dir').file().get_path())
    contents = store.get_file(dest.copy().dir('nested dir').file().get_path())
    with open(pathname.LOCAL_TESTFILE_PATH) as _file:
        assert _file.read() == contents, "move file differs from the local file."
    contents = store.get_file(dest.copy().file(nr=2).get_path())
    with open(pathname.LOCAL_TESTFILE_PATH) as _file:
        assert _file.read() == contents, "move file differs from the local file."
    assert not store.exists(source.copy().dir('nested dir').file().get_path())
    assert not store.exists(source.copy().dir('nested dir').get_path())
    store.delete(dest.get_path(), True)

def _test_create_delete_directory(store):
    _create_directories(store, pathname.testdir().get_path())
    _delete_directories(store, pathname.testdir().get_path())

def _dir_exists(store, path):
    exists = store.exists(path)
    if not exists:
        return False
    is_dir = store.is_dir(path)
    return is_dir

def _create_directories(store, root_dir="/"):
    if root_dir[-1] != "/":
        root_dir+="/"
    store.create_directory(root_dir+"Test1")
    assert _dir_exists(store, root_dir+"Test1")
    store.create_directory(root_dir+"tesT2")
    assert _dir_exists(store, root_dir+"tesT2")
    store.create_directory(root_dir+"testdub")
    assert _dir_exists(store, root_dir+"testdub")
    try:
        assert store.create_directory(root_dir+"testdub") != 200
    except AlreadyExistsError:
        pass
    store.create_directory(root_dir+"testcasesensitivity")
    assert _dir_exists(store, root_dir+"testcasesensitivity")
    try:
        assert store.create_directory(root_dir+"testcasesensitivity".upper() ) != 200
    except AlreadyExistsError:
        pass
        
def _delete_directories(store, root_dir="/"):
    if root_dir[-1] != "/":
        root_dir+="/"
    store.delete(root_dir+"Test1", True)
    assert not store.exists(root_dir+"Test1")
    store.delete(root_dir+"tesT2", True)
    assert not store.exists(root_dir+"tesT2")
    store.delete(root_dir+"testdub", True)
    assert not store.exists(root_dir+"testdub")
    store.delete(root_dir+"testcasesensitivity", True)
    assert not store.exists(root_dir+"testcasesensitivity")
    
def _test_store_delete_file(store):
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    assert store.exists(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME)
    _delete_file(store, pathname.LOCAL_TESTFILE_NAME, pathname.testdir().get_path())
    assert not store.exists(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME)
    store.store_file(pathname.LOCAL_BIGTESTFILE_PATH, pathname.testdir().get_path())
    assert store.exists(pathname.testdir().get_path()+"/"+pathname.LOCAL_BIGTESTFILE_NAME)
    _delete_file(store, pathname.LOCAL_BIGTESTFILE_NAME, pathname.testdir().get_path())
    assert not store.exists(pathname.testdir().get_path()+"/"+pathname.LOCAL_BIGTESTFILE_NAME)
    empty_fileobject = tempfile.SpooledTemporaryFile()
    store.store_fileobject(empty_fileobject, pathname.testdir().file('empty_file').get_path())
    assert store.exists(pathname.testdir().file('empty_file').get_path())
    store.delete(pathname.testdir().file('empty_file').get_path(), is_dir=False)
    assert not store.exists(pathname.testdir().file('empty_file').get_path())
    local_fileobject = open(pathname.LOCAL_TESTFILE_PATH)
    store.store_fileobject(local_fileobject, pathname.testdir().file('empty_file').get_path())
    assert store.exists(pathname.testdir().file('empty_file').get_path())
    store.delete(pathname.testdir().file('empty_file').get_path(), is_dir=False)
    assert not store.exists(pathname.testdir().file('empty_file').get_path())
    
def _test_exists(store):
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    assert store.exists(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME)
    _delete_file(store, pathname.LOCAL_TESTFILE_NAME, pathname.testdir().get_path())
    assert store.exists(pathname.testdir().get_path())
    assert store.exists(pathname.testdir().get_path())
    assert not store.exists(pathname.testdir().dir('non existent dir').get_path())
    assert not store.exists(pathname.testdir().file('non existent file').get_path())
    assert not store.exists(pathname.testdir().dir('non existent dir2').file('non existent file2').get_path())
    assert not store.exists(pathname.testdir().dir('non existent dir3').dir('nested non existent').get_path())

def _delete_file(store, filename, root_dir="/"):
    if root_dir[-1] != "/":
        root_dir += "/"
    store.delete(root_dir+filename, False)
    
def _test_duplicate(store):
    origin = pathname.copy().testdir().dir('origin').file()
    copy = pathname.copy().testdir().dir('copy').file()
    store.create_directory(origin.get_parent())
    assert _dir_exists(store, origin.get_parent())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, origin.get_parent(), origin.get_filename()) 
    assert store.exists(origin.get_path())
    # TODO: Error - copy should be overwritten
    store.duplicate(origin.get_parent(), copy.get_parent())
    assert _dir_exists(store, copy.get_parent())
    # Copy over existing file:
    store.duplicate(origin.get_path(), copy.get_path())
    assert store.exists(copy.get_path())
    store.delete(origin.get_parent(), True)
    store.delete(copy.get_parent(), True)
    
def _test_nested_duplicate(store):
    origin = pathname.copy().testdir().dir('origin')
    copy = pathname.copy().testdir().dir('copy')
    store.create_directory(origin.get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, origin.get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, origin.get_path(), remote_file_name=origin.copy().file(nr=2).get_filename())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, origin.get_path(), remote_file_name=origin.copy().file(nr=3).get_filename())
    store.create_directory(origin.copy().dir('nested dir').get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, origin.copy().dir('nested dir').get_path(), remote_file_name=origin.copy().file('nested file').get_filename())
    store.duplicate(origin.get_path(), copy.get_path())
    assert _dir_exists(store, copy.get_path())
    assert _dir_exists(store, copy.copy().dir('nested dir').get_path())
    assert store.exists(copy.copy().dir('nested dir').file('nested file').get_path())
    contents = store.get_file(copy.copy().dir('nested dir').file('nested file').get_path())
    with open(pathname.LOCAL_TESTFILE_PATH) as file:
        assert file.read() == contents, "duplicated file differs from the local file."
    contents = store.get_file(copy.copy().file(nr=2).get_path())
    with open(pathname.LOCAL_TESTFILE_PATH) as file:
        assert file.read() == contents, "duplicated file differs from the local file."
    store.delete(origin.get_path(), True)
    store.delete(copy.get_path(), True)
    assert not store.exists(copy.get_path())
    assert not store.exists(copy.copy().dir('nested dir').get_path())
    assert not store.exists(copy.copy().file(nr=3).get_path())
    
#assert_all_in(resp.data.keys(), [u'is_deleted', u'thumb_exists',u'bytes', u'modified', u'path', u'is_dir',u'size', u'root', u'hash', u'contents', u'icon'])
       

        
# TODO: list root directory
