'''
Module to test all implementations of the :class:`Store` interface, 
as well as the extension interface :class:`BulkGetMetadata`.
The actual test only tests one service; For instance test_dropbox
only tests the Store implementation :class:`DropboxStore`, but it might
also test integration with various wrapper classes like :class:`MetadataCachingStore`
or :class:`MultiprocessingCachingStore`.

The tests can be executed in parallel by calling each test individually
in a separate nosetests process like this:

nosetests -v -s -x cloudfusion.tests.store_tests:test_dropbox &

nosetests -v -s -x cloudfusion.tests.store_tests:test_sugarsync &

nosetests -v -s -x cloudfusion.tests.store_tests:test_local &

nosetests -v -s -x cloudfusion.tests.store_tests:test_amazon &

nosetests -v -s -x cloudfusion.tests.store_tests:test_google &

nosetests -v -s -x cloudfusion.tests.store_tests:test_gdrive &

nosetests -v -s -x cloudfusion.tests.store_tests:test_webdav_tonline &

nosetests -v -s -x cloudfusion.tests.store_tests:test_webdav_gmx &

nosetests -v -s -x cloudfusion.tests.store_tests:test_webdav_box &

nosetests -v -s -x cloudfusion.tests.store_tests:test_webdav_yandex &


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
from cloudfusion.tests.path_constants import *
from cloudfusion.store.webdav.bulk_get_metadata_webdav_store import BulkGetMetadataWebdavStore
from cloudfusion.store.local_drive.local_hd_store import LocalHDStore
from cloudfusion.store.s3.amazon_store import AmazonStore
from cloudfusion.store.s3.bulk_get_metadata_amazon_store import BulkGetMetadataAmazonStore
from cloudfusion.store.gs.bulk_get_metadata_google_store import BulkGetMetadataGoogleStore
from cloudfusion.store.gdrive.google_drive import GoogleDrive
from cloudfusion.store.transparent_chunk_caching_store import TransparentChunkMultiprocessingCachingStore
from cloudfusion.store.transparent_caching_store import TransparentMultiprocessingCachingStore

store = None

def teardown_func():
    try:
        store.delete(REMOTE_TESTDIR, True)
    except Exception, e:
        pass

@with_setup(teardown=teardown_func)
def test_sugarsync():
    global store 
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
    config = get_webdav_tonline_config()
    store = BulkGetMetadataWebdavStore(config)
    for test in _generate_store_tests(store, "BulkGetMetadataWebdavStore tonline"):
        yield test
    for test in _generate_bulk_get_metadata_tests(store, "BulkGetMetadataWebdavStore tonline"):
        yield test

@with_setup(teardown=teardown_func)
def test_webdav_gmx():
    global store 
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
        store.create_directory(REMOTE_TESTDIR_PART1)
    except AlreadyExistsError:
        pass
    try:
        store.create_directory(REMOTE_TESTDIR)
    except AlreadyExistsError:
        pass

def _generate_bulk_get_metadata_tests(store, description_of_store):
    _create_test_directory(store)
    test = partial(_test_bulk_get_metadata, store)
    test.description = description_of_store+": get bulk metadata"
    yield (test, ) 
    store.delete(REMOTE_TESTDIR, True)

def _generate_store_tests(store, description_of_store, include_space_tests=True):
    '''Generate general tests for store.
    :param store: The object to test.
    :type store: :class:`Store`
    :param description_of_store: String to describe the store instance.
    :param include_space_tests: Indicates if tests about the free, used, and overall space should be executed.
    :type include_space_tests: boolean'''
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
    store.delete(REMOTE_TESTDIR, True)

def _assert_all_in(in_list, all_list):
    assert all(item in in_list for item in all_list), "expected all items in %s to be found in %s" % (all_list, in_list)
    
def _test_get_file(store):
    store.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR, REMOTE_TESTFILE_NAME)
    first_resp = store.get_file(REMOTE_TESTDIR+"/"+REMOTE_TESTFILE_NAME)
    second_resp = store.get_file(REMOTE_TESTDIR+"/"+REMOTE_TESTFILE_NAME)
    _delete_file(store, REMOTE_TESTFILE_NAME, REMOTE_TESTDIR)
    assert first_resp == second_resp, "first response should be same as second response, but %s != %s" % (first_resp, second_resp)
    assert len(first_resp) == 4, "length of file from remote side should be 4 bytes, since in testfile I stored the word 'test' but is %s bytes" % len(first_resp)
    
def _test_fail_on_is_dir(store): 
    assert_raises(NoSuchFilesytemObjectError, store.is_dir, REMOTE_NON_EXISTANT_FILE)
    assert_raises(NoSuchFilesytemObjectError, store.is_dir, REMOTE_NON_EXISTANT_DIR)
    store.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    store.delete(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME, False)
    store.create_directory(REMOTE_DELETED_DIR)
    store.delete(REMOTE_DELETED_DIR, True)
    assert_raises(NoSuchFilesytemObjectError, store.is_dir, REMOTE_DELETED_FILE)
    assert_raises(NoSuchFilesytemObjectError, store.is_dir, REMOTE_DELETED_DIR)
        
def _test_fail_on_get_bytes(store):
    assert_raises(NoSuchFilesytemObjectError, store.get_bytes, REMOTE_NON_EXISTANT_FILE)
    assert_raises(NoSuchFilesytemObjectError, store.get_bytes, REMOTE_NON_EXISTANT_DIR)
    store.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    store.delete(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME, False)
    store.create_directory(REMOTE_DELETED_DIR)
    store.delete(REMOTE_DELETED_DIR, True)
    assert_raises(NoSuchFilesytemObjectError, store.get_bytes, REMOTE_DELETED_FILE)
    assert_raises(NoSuchFilesytemObjectError, store.get_bytes, REMOTE_DELETED_DIR)
    
def _test_fail_on_get_modified(store):
    assert_raises(NoSuchFilesytemObjectError, store.get_modified, REMOTE_NON_EXISTANT_FILE)
    assert_raises(NoSuchFilesytemObjectError, store.get_modified, REMOTE_NON_EXISTANT_DIR)
    store.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    store.delete(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME, False)
    store.create_directory(REMOTE_DELETED_DIR)
    store.delete(REMOTE_DELETED_DIR, True)
    assert_raises(NoSuchFilesytemObjectError, store.get_modified, REMOTE_DELETED_FILE)
    assert_raises(NoSuchFilesytemObjectError, store.get_modified, REMOTE_DELETED_DIR)

def _test_get_bytes(store):
    store.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR) 
    res = store.get_bytes(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME)
    store.delete(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME, False)
    assert res == 4, "stored file should be 4 bytes big, but has a size of %s bytes" % res

def _test_is_dir(store):
    assert store.is_dir(REMOTE_TESTDIR) == True
    store.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    assert store.is_dir(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME) == False 
    store.delete(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME, False)
        
def _test_account_info(store):
    assert type(store.account_info()) == str
            
def _test_get_modified(store):
    store.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    file_modified_time = int(store.get_modified(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME))
    now_time = time.time()
    store.delete(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME, False)
    assert _assert_equal_with_variance( file_modified_time, now_time, 15, "modified time stamp of copied file is off by %s seconds" %  abs(file_modified_time-now_time) )
    store.create_directory(REMOTE_MODIFIED_TESTDIR)
    dir_modified_time = store.get_modified(REMOTE_MODIFIED_TESTDIR)
    now_time = time.time()
    store.delete(REMOTE_MODIFIED_TESTDIR, True)
    assert _assert_equal_with_variance( dir_modified_time, now_time, 15, "modified time stamp of copied file is off by %s seconds" %  abs(dir_modified_time-now_time) )
    #assert not store.is_dir(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_PATH) 

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
    _create_directories(store, REMOTE_TESTDIR)
    store.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    listing = store.get_directory_listing(REMOTE_TESTDIR)
    cached_listing1 = store.get_directory_listing(REMOTE_TESTDIR)
    cached_listing2 = store.get_directory_listing(REMOTE_TESTDIR)
    _delete_directories(store, REMOTE_TESTDIR)
    _delete_file(store, LOCAL_TESTFILE_NAME, REMOTE_TESTDIR)
    root = REMOTE_TESTDIR+"/"
    _assert_all_in(listing, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+LOCAL_TESTFILE_NAME]) 
    _assert_all_in(cached_listing1, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+LOCAL_TESTFILE_NAME]) 
    _assert_all_in(cached_listing2, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+LOCAL_TESTFILE_NAME]) 

def _test_bulk_get_metadata(store): 
    if not isinstance(store, BulkGetMetadata):
        return
    _create_directories(store, REMOTE_TESTDIR)
    store.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR) #testfile
    time.sleep(5) #wait for file to be stored (eventual consistency)
    metadata = store.get_bulk_metadata(REMOTE_TESTDIR)
    cached_metadata1 = store.get_bulk_metadata(REMOTE_TESTDIR)
    cached_metadata2 = store.get_bulk_metadata(REMOTE_TESTDIR)
    _delete_directories(store, REMOTE_TESTDIR)
    _delete_file(store, LOCAL_TESTFILE_NAME, REMOTE_TESTDIR)
    root = REMOTE_TESTDIR+"/"
    for path in [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+LOCAL_TESTFILE_NAME]:
        for metadata in [metadata, cached_metadata1, cached_metadata2]:
            if path != root+LOCAL_TESTFILE_NAME:
                assert metadata[path]['is_dir'] == True
                assert metadata[path]['bytes'] == 0
                assert 'modified' in metadata[path]
            else:
                assert metadata[path]['is_dir'] == False
                assert metadata[path]['bytes'] == 4
                assert 'modified' in metadata[path]  

def _test_move_directory(store):
    store.create_directory(REMOTE_MOVE_TESTDIR_ORIGIN)
    store.move(REMOTE_MOVE_TESTDIR_ORIGIN, REMOTE_MOVE_TESTDIR_RENAMED)
    assert _dir_exists(store, REMOTE_MOVE_TESTDIR_RENAMED)
    store.delete(REMOTE_MOVE_TESTDIR_RENAMED, True)
            
def _test_move_file(store):
    store.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    assert store.exists(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME) 
    store.move(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME, REMOTE_TESTDIR+"/"+REMOTE_MOVE_TESTFILE_RENAMED)
    assert store.exists(REMOTE_TESTDIR+"/"+REMOTE_MOVE_TESTFILE_RENAMED) 
    store.delete(REMOTE_TESTDIR+"/"+REMOTE_MOVE_TESTFILE_RENAMED, False)

def _test_create_delete_directory(store):
    _create_directories(store, REMOTE_TESTDIR)
    _delete_directories(store, REMOTE_TESTDIR)

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
    store.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    assert store.exists(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME)
    _delete_file(store, LOCAL_TESTFILE_NAME, REMOTE_TESTDIR)
    assert not store.exists(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME)
    store.store_file(LOCAL_BIGTESTFILE_PATH, REMOTE_TESTDIR)
    assert store.exists(REMOTE_TESTDIR+"/"+LOCAL_BIGTESTFILE_NAME)
    _delete_file(store, LOCAL_BIGTESTFILE_NAME, REMOTE_TESTDIR)
    assert not store.exists(REMOTE_TESTDIR+"/"+LOCAL_BIGTESTFILE_NAME)
    empty_fileobject = tempfile.SpooledTemporaryFile()
    store.store_fileobject(empty_fileobject, REMOTE_TESTDIR+"/"+"empty_file")
    assert store.exists(REMOTE_TESTDIR+"/"+"empty_file")
    _delete_file(store, "empty_file", REMOTE_TESTDIR)
    assert not store.exists(REMOTE_TESTDIR+"/"+"empty_file")
    local_fileobject = open(LOCAL_TESTFILE_PATH)
    store.store_fileobject(local_fileobject, REMOTE_TESTDIR+"/"+"empty_file")
    assert store.exists(REMOTE_TESTDIR+"/"+"empty_file")
    _delete_file(store, "empty_file", REMOTE_TESTDIR)
    assert not store.exists(REMOTE_TESTDIR+"/"+"empty_file")
    
def _test_exists(store):
    store.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    assert store.exists(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME)
    _delete_file(store, LOCAL_TESTFILE_NAME, REMOTE_TESTDIR)
    assert store.exists(REMOTE_TESTDIR)
    assert store.exists(REMOTE_TESTDIR)
    assert not store.exists(REMOTE_NON_EXISTANT_DIR)
    assert not store.exists(REMOTE_NON_EXISTANT_FILE)

def _delete_file(store, filename, root_dir="/"):
    if root_dir[-1] != "/":
        root_dir += "/"
    store.delete(root_dir+filename, False)
    
def _test_duplicate(store):
    store.create_directory(REMOTE_DUPLICATE_TESTDIR_ORIGIN)
    assert _dir_exists(store, REMOTE_DUPLICATE_TESTDIR_ORIGIN)
    store.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR, REMOTE_TESTFILE_NAME) 
    assert store.exists(REMOTE_TESTDIR+"/"+REMOTE_TESTFILE_NAME)
    store.duplicate(REMOTE_DUPLICATE_TESTDIR_ORIGIN, REMOTE_DUPLICATE_TESTDIR_COPY)
    assert _dir_exists(store, REMOTE_DUPLICATE_TESTDIR_COPY)
    store.duplicate(REMOTE_DUPLICATE_TESTFILE_ORIGIN, REMOTE_DUPLICATE_TESTFILE_COPY)
    assert store.exists(REMOTE_DUPLICATE_TESTFILE_COPY)
    store.delete(REMOTE_DUPLICATE_TESTDIR_ORIGIN, True)
    store.delete(REMOTE_DUPLICATE_TESTDIR_COPY, True)
    store.delete(REMOTE_DUPLICATE_TESTFILE_ORIGIN, False)
    store.delete(REMOTE_DUPLICATE_TESTFILE_COPY, False)
    
#assert_all_in(resp.data.keys(), [u'is_deleted', u'thumb_exists',u'bytes', u'modified', u'path', u'is_dir',u'size', u'root', u'hash', u'contents', u'icon'])
       

        
