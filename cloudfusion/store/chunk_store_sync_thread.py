from __future__ import division
from cloudfusion.store.store_worker import ReadWorker, RemoveWorker, WorkerStats,\
    WriteWorkerProcesses
from cloudfusion.store.chunk_store_worker import ChunkWriteWorker
from threading import Thread, RLock
import time
from cloudfusion.store.store import StoreSpaceLimitError, StoreAccessError, NoSuchFilesytemObjectError,\
    StoreAutorizationError
import tarfile
import os
import shelve
import tempfile
import ntplib
import base64
import types
from cloudfusion.util.file_decorator import DataFileWrapper
from contextlib import closing
import atexit
from cloudfusion.util import file_util

def get_parent_dir(path): # helper function to get parent directory ending with '/'
    ret = os.path.dirname(path)
    if ret != '/': 
        ret += '/' 
    return ret

class Archive(object):
    '''Archive representation used in ChunkFactory'''
    def __init__(self, directory):
        self.files = {} #map filenames as which they should be visible in cloudfusion to filepaths with actual data on the local disk
        self.size_in_bytes = 0
        self.parent_dir = directory
        self.first_access = time.time()

class Chunk(object):
    '''Chunk representation:
    :param parent_dir: directory location to store the chunk
    :param fileobject: file in the local file system with its location in fileobject.name
    :param filepaths: list of paths of files in the chunk
    '''
    def __init__(self, parent_dir, fileobject, filepaths):
        self.parent_dir = parent_dir
        self.fileobject = fileobject
        self.filepaths = filepaths

class ChunkFactory(object):
    '''Factory that returns added files packed together as archives, if they are in the same directory.'''
    def __init__(self, logger, max_time_to_upload=200, max_chunk_size=4):
        ''':param max_time_to_upload: time in seconds until the chunkfactory returns a file inside a chunk after the file has been added to it
        :param max_chunk_size: size of the chunk in MB that should not be exceeded when packing several files together'''
        self.logger = logger
        self.max_time_to_upload = max_time_to_upload
        self.max_chunk_size = max_chunk_size * 1000 * 1000 #convert to bytes
        self.archives = {} #dictionary mapping parent directory to archive
        self.completed_archives = {} #dictionary mapping parent directory to list of archives
    
    def _swap_out_completed_archives(self):
        '''Checks for all archives if they are ready to be uploaded, 
           and swap them to self.completed_archives'''
        archives_to_delete = []
        for path, archive in self.archives.iteritems():
            if self._is_complete(archive):
                path = archive.parent_dir
                self.completed_archives[path] = self.completed_archives.get(path,[]) + [archive]
                archives_to_delete.append(path)
        for path in archives_to_delete:        
            del self.archives[path]    
            
    def _is_complete(self, archive):
        ''':returns: True iff the archive is ready to be uploaded'''
        return archive.size_in_bytes > self.max_chunk_size \
                    or time.time() > self.max_time_to_upload + archive.first_access 
        
    def in_progress(self, filepath):
        fname = os.path.basename(filepath)
        directory = get_parent_dir(filepath)
        if directory in self.archives:
            if fname in self.archives[directory].files:
                return True
        if directory in self.completed_archives:
            for archive in self.completed_archives[directory]:
                if fname in archive.files:
                    return True
        return False
            
    def _get_archive(self, filepath):
        '''Get the archive to store filepath in, creating a new one if it does not exist.'''
        directory = get_parent_dir(filepath)
        self._swap_out_completed_archives()
        if not directory in self.archives:
            self.logger.debug("new archive: "+directory)
            new_archive = Archive(directory)
            self.archives[directory] = new_archive
        return self.archives[directory]     
        
    def add(self, local_file, filepath):
        '''Adds a new file to the chunk factory. 
        :param local_file: fileobject with its absolute path in property name
        :param filepath: the filepath that local_file should be stored to in the wrapped store
        '''
        fname = os.path.basename(filepath)
        archive = self._get_archive(filepath)
        archive.files[fname] = local_file.name
        archive.size_in_bytes += os.path.getsize(local_file.name)
        local_file.close()
        
    def get_new_chunk(self):
        '''Get a chunk, if one is ready for upload according to max_time_to_upload, and max_chunk_size.
        The chunk file returned needs to be deleted if it is not used anymore. It is stored in chunk.name
        The list is removed from the list of archives that are ready to be uploaded.       
        :returns: Instance of Chunk or None if no chunk is available.'''
        self._swap_out_completed_archives()
        for path, archives in self.completed_archives.iteritems():
            for archive in archives: 
                chunk = self._create_archive(archive)
                archives = self.completed_archives[path]
                archives.remove(archive)
                self.completed_archives[path] = archives
                filepaths = map(lambda x: path+x, archive.files.keys()) 
                return Chunk(path, chunk, filepaths)
        return None
    
    def get_size_of_next_chunk(self):
        ''':returns: size in bytes of the next chunk returned by :meth:`get_new_chunk` or 0 if there is none.'''
        self._swap_out_completed_archives()
        ret = 0
        for path, archives in self.completed_archives.iteritems():
            for archive in archives:
                for fname, filepath in archive.files.iteritems():
                    ret += file_util.get_file_size_in_mb(filepath)
                return ret
        return 0
    
    def _create_archive(self, archive):
        '''Create an actual tar archive in the file system.
        :returns: fileobject with path of the tar archive in the file system in the name property'''
        ret = tempfile.NamedTemporaryFile(delete=False)
        tempname = ret.name
        ret.close()
        with closing(tarfile.open(tempname, "w")) as tar: #backwards compatibility; tarfile does not support with statement until python 2.7
            for fname, filepath in archive.files.iteritems():
                tar.add(filepath,fname)
                os.remove(filepath)
        ret = open(tempname)
        return ret
        

    def force_get_all_chunks(self):
        '''Force the factory to return chunks for all added files, ignoring max_time_to_upload, and max_chunk_size.
        :returns: a list of Chunk instances or an empty list if no chunk is available'''
        ret = []
        for path, archives in self.completed_archives.iteritems():
            for archive in archives:
                #print "force_get_all_chunks archive with files: "+repr(archive.files)
                chunk = self._create_archive(archive)
                filepaths = map(lambda x: path+x, archive.files.keys()) 
                ret.append(Chunk(path, chunk, filepaths))
        self.completed_archives = {}
        for path, archive in self.archives.iteritems():
            #print "force_get_all_chunks 2 archive with files: "+repr(archive.files)
            chunk = self._create_archive(archive)
            filepaths = map(lambda x: path+x, archive.files.keys()) 
            ret.append(Chunk(path, chunk, filepaths))
        self.archives = {}
        return ret
    
    def remove(self, filepath):
        '''Removes added file if possible.
        :returns: True iff the file could be removed'''
        filepath = os.path.basename(filepath) #TODO:check if this is correct; actually, there could be several objects with the same filename
        ret = False
        file_to_delete = None
        archive_to_swap = None
        for path, archives in self.completed_archives.iteritems():
            for archive in archives:
                if filepath in archive.files:
                    file_to_delete = archive.files[filepath]
                    del archive.files[filepath]
                    archive_to_swap = archive
                    archives = self.completed_archives[path]
                    archives.remove(archive)
                    self.completed_archives[path] = archives
                    ret = True
                    break
        if archive_to_swap:
            class FakeFileObject(object):
                def __init__(self, name):
                    self.name = name
                def close(self):
                    pass
            for store_filepath, disk_filepath in archive_to_swap.files.iteritems():
                fake_fileobj = FakeFileObject(disk_filepath)
                self.add(fake_fileobj, disk_filepath)
            
        for archive in self.archives.values():
            if filepath in archive.files:
                file_to_delete = archive.files[filepath]
                del archive.files[filepath]
                ret = True
                break
        try:
            if file_to_delete:
                os.remove(file_to_delete)
        except Exception, e:
            pass
        return ret
        
class PersistentChunkMapper(object):
    '''Persistently maps between chunk names and filepaths of files in the chunk'''
    def __init__(self, temp_dir, logger):
        self.logger = logger
        self.temp_dir = temp_dir
        self.chunk_mapping_db_filename = self.temp_dir+"/chunk-mapping" # TODO:  load from store! / merge?
        self.filepath_mapping_db_filename = self.temp_dir+"/filepath-mapping" # TODO:  load from store! / merge?
        self.alias_mapping_db_filename = self.temp_dir+"/alias-mapping" # TODO:  load from store! / merge?
        atexit.register( lambda : self._close() )
        self.chunk_mapping = shelve.open(self.chunk_mapping_db_filename) 
        self.filepath_mapping = shelve.open(self.filepath_mapping_db_filename) 
        self.alias_mapping = shelve.open(self.alias_mapping_db_filename) 
        self.garbage = []
        self._session_id = None
        
    def _close(self):
        try:
            self.chunk_mapping.close()
            self.filepath_mapping.close()
            self.alias_mapping.close()
        except Exception, e:
            pass
        
    def __num_to_alpha(self, num):
        '''From: http://stackoverflow.com/questions/10326118/encoding-a-numeric-string-into-a-shortened-alphanumeric-string-and-back-again'''
        num = hex(num)[2:].rstrip("L")
        if len(num) % 2:
            num = "0" + num
        ret = base64.b32encode(num.decode('hex'))
        return ret

    def get_next_chunk_uuid(self):
        '''Get globally unique identifier for the next chunk'''
        if not self._session_id:
            try:
                self._time_offset = ntplib.NTPClient().request('pool.ntp.org').offset
            except Exception, e:
                self._time_offset = 0
            unique_num = int( 100 * (time.time() + self._time_offset) ) #get globally unique number
            self._session_id = self.__num_to_alpha(unique_num)
            self.chunk_ctr = 0
        self.chunk_ctr += 1
        ret = "chunk_"+self._session_id +"_"+ self.__num_to_alpha(self.chunk_ctr)+".tar"
        print ret
        return ret
    
    def get_files_in_chunk(self, chunk_name):
        ''':param chunk_name: absolute path to the chunk
        :returns: list of absolute filepaths for files in the chunk'''
        self.logger.debug("absolute chunk_name in get_files_in_chunk:"+chunk_name)
        parent_dir = get_parent_dir(chunk_name)
        chunk_name = os.path.basename(chunk_name).encode("utf8") #we only need the name of the chunk, as it is a unique identifier
        if chunk_name in self.alias_mapping:
            filenames = self.alias_mapping[chunk_name]
        elif chunk_name in self.chunk_mapping:
            filenames = self.chunk_mapping[chunk_name]
        else:
            return []
        #print ("filepaths in get_files_in_chunk: "+parent_dir+" "+repr(filenames))
        filepaths = map(lambda x: parent_dir+x, filenames) # append directory to filenames
        return filepaths 
    
    def iterate_files_from_chunk(self, chunk_content, chunk_id):
        ''':param chunk_content: raw chunk content
        :param chunk_id: unique id of the chunk
        :returns: a generator iterating over the file is the chunk'''
        tar = tarfile.open(name=None, mode='r', fileobj=DataFileWrapper(chunk_content))
        chunk_id = chunk_id.encode("utf8")
        if chunk_id in self.alias_mapping:
            filepaths = self.alias_mapping[chunk_id]
        else:
            filepaths = self.chunk_mapping[chunk_id]
        for filepath in filepaths:
            file_content = tar.extractfile(filepath)
            modified_date = tar.getmember(filepath).mtime
            yield (filepath, file_content.read(1000000000), modified_date)
    
    def put(self, chunk_uuid, filepaths):
        '''Adds a chunk mapping between chunk_uuid and filepaths'''
        files = map(os.path.basename, filepaths)
        chunk_uuid = chunk_uuid.encode("utf8")
        self.chunk_mapping[chunk_uuid] = files #create tuple with (filename,filename_alias)
        for filepath in filepaths:
            filepath = filepath.encode("utf8")
            self.filepath_mapping[filepath] = chunk_uuid
            #print (filepath+" <-- "+chunk_uuid)
        self.filepath_mapping.sync()
        self.chunk_mapping.sync()
        
    def add_aliases(self, chunk_uuid, alias_filepaths, new_alias_filepaths): #dont use put, as it wuold overwrite filepath_mapping with new chunk
        '''Adds an alias of a file in the chunk with id chunk_uuid'''
        new_alias_filenames = [os.path.basename(x).encode("utf8") for x in new_alias_filepaths] #replace previous alias
        #aliases = [os.path.basename(new_filepath_alias) if x == os.path.basename(filepath_alias) else os.path.basename(filepath_alias) for x in files] #replace previous alias
        self.logger.debug("new aliases: "+repr(new_alias_filepaths)+" for files: "+repr(alias_filepaths)+" in "+chunk_uuid)
        chunk_uuid = chunk_uuid.encode("utf8")
        self.alias_mapping[chunk_uuid] = new_alias_filenames
        for alias in new_alias_filepaths:
            self.filepath_mapping[alias] = chunk_uuid
            self.logger.debug(alias+" <-- "+chunk_uuid)
        self.filepath_mapping.sync()
        self.alias_mapping.sync()
    
    def get_chunk_uuid(self, filepath):
        ''':returns: the uuid of the chunk that filepath is stored in or None if it does not exist'''
        filepath = filepath.encode("utf8")
        if not filepath in self.filepath_mapping:
            return None
        self.logger.debug(filepath+" --> "+self.filepath_mapping[filepath])
        return self.filepath_mapping[filepath]       
    
    def remove_file(self, filepath):
        '''Remove filepath from the mapping.'''
        filepath = filepath.encode("utf8")
        if not filepath in self.filepath_mapping:
            return
        chunk_id = self.filepath_mapping[filepath]
        self.logger.debug(filepath+" <--X-- "+chunk_id)
        del self.filepath_mapping[filepath]
        filename = os.path.basename(filepath)
        if chunk_id in self.alias_mapping:
            aliases = self.alias_mapping[chunk_id]
            #print ("rempve "+filepath+" from aliases: "+repr(aliases)+" in chunk "+chunk_id)
            idx = aliases.index(filename)
            aliases.remove(filename)
            self.alias_mapping[chunk_id] = aliases
            if chunk_id in self.chunk_mapping:  #alias might be assigned in a duplicate alias chunk, which does not exist in chunk_mapping
                del self.chunk_mapping[chunk_id][idx] 
            if len(aliases) == 0:
                del self.alias_mapping[chunk_id]
                if chunk_id in self.chunk_mapping: #alias might be assigned in a duplicate alias chunk, which does not exist in chunk_mapping
                    del self.chunk_mapping[chunk_id]
                self.garbage.append(get_parent_dir(filepath)+chunk_id)
        else:
            files = self.chunk_mapping[chunk_id]
            files.remove(filename) 
            self.chunk_mapping[chunk_id] = files
            if len(files) == 0:
                del self.chunk_mapping[chunk_id]
                self.garbage.append(get_parent_dir(filepath)+chunk_id)
        self.filepath_mapping.sync()
        self.chunk_mapping.sync()
        self.alias_mapping.sync()
    
    def get_empty_chunks(self):
        ''':returns: chunks for garbage collection'''
        ret = self.garbage
        self.garbage = []
        return ret
        
        
class ChunkStoreSyncThread(object):
    """Synchronizes between cache and store"""
    def __init__(self, cache, store, temp_dir, logger, max_writer_threads=30):
        self.temp_dir = temp_dir
        self.logger = logger
        self.stats = WorkerStats()
        self.cache = cache
        self.store = store
        try:
            os.makedirs(self.temp_dir)
        except Exception, e:
            pass
        self.chunk_mapper = PersistentChunkMapper(temp_dir,self.logger) 
        self.max_writer_threads = max_writer_threads
        self.WRITE_TIMELIMIT = 60*60*2 #2h
        self.lock = RLock()
        self.protect_cache_from_write_access = RLock() #could also be normal lock
        self.oldest_modified_date = {} #keep track of modified date of a cache entry when it is first enqueued for upload. Their contents might change during upload.
        self.removers = []
        self.writers = []
        self.readers = []
        self._stop = False
        self.thread = None
        self.last_reconnect = time.time()
        self._heartbeat = time.time()
        #used for waiting when quota errors occur
        self.skip_starting_new_writers_for_next_x_cycles = 0
        self.upload_process_pool = WriteWorkerProcesses(store, logger)
        self.logger.info("initialized ChunkStoreSyncThread")
        self.chunk_factory = ChunkFactory(self.logger) 
    
    def _get_max_threads(self, size_in_mb):
        ''':returns: the number of upload worker threads that should be used
        according to the the file size and the average time needed to upload a file.'''
        def get_average_upload_time():
            upload_time = 0
            write_workers = self.stats.write_workers[-10:]
            if len(write_workers) == 0:
                return 0
            for ww in write_workers:
                upload_time += ww.get_endtime() - ww.get_starttime()
            return upload_time / len(write_workers)
        
        if size_in_mb <= 0.1:
            return self.max_writer_threads
        # use less threads if they are not finished within one point five seconds
        # also use less threads if the files are larger
        average_upload_time = get_average_upload_time()
        if average_upload_time < 1.5:
            slowdown = 1
        elif average_upload_time < 2:
            slowdown = 2
        else:
            slowdown = 4
        ret = self.max_writer_threads / (size_in_mb+1) / slowdown
        if ret < 3:
            ret = 3
        return ret
    
    def restart(self):
        self.stop()
        self.thread.join(60*5)
        #stop write- and readworkers
        for reader in self.readers:
            if not reader.is_finished():
                reader.stop() #causes error: AttributeError: 'NoneType' object has no attribute 'join'
        for remover in self.removers:
            remover.stop()
        self.start()
        
    def get_downloaded(self):
        """Get amount of data downloaded from a store in MB"""
        return self.stats.downloaded / 1000.0 / 1000
    
    def get_uploaded(self):
        """Get amount of data uploaded to a store in MB"""
        return self.stats.uploaded / 1000.0 / 1000
    
    def get_download_rate(self):
        """Get download rate in MB/s"""
        return self.stats.get_download_rate() / 1000.0 / 1000
    
    def get_upload_rate(self):
        """Get upload rate in MB/s"""
        return self.stats.get_upload_rate() / 1000.0 / 1000
    
    def get_exception_stats(self):
        return self.stats.exceptions_log
    
    def start(self):
        self._stop = False
        self.thread = Thread(target=self.run)
        self.thread.setDaemon(True)
        self.thread.start()
    
    def stop(self):
        self._stop = True
        
    def _remove_finished_writers(self):
        writers_to_be_removed = []
        for writer in self.writers:
            if writer.is_finished():
                writers_to_be_removed.append(writer)
                self.stats.add_finished_worker(writer)
                with self.protect_cache_from_write_access: #otherwise, fuse thread could delete current cache entry
                    if writer.is_successful():
                        self.chunk_mapper.put(writer.chunk_uuid, writer.filepaths)
                        for path in writer.filepaths:
                            if self.cache.exists(path): #stop() call in delete method might not have prevented successful write
                                modified_during_upload =  self.cache.get_modified(path) > self.oldest_modified_date[path] #keyerror in oldest_m_p
                                #print "set clean "+path+"? - "+str(not modified_during_upload)
                                #print str(self.cache.get_modified(path))+" <= "+ str(self.oldest_modified_date[path])
                                if not modified_during_upload: #actual modified date is >= oldest modified date
                                    self.set_dirty_cache_entry(path, False) # set_dirty might delete item, if cache limit is reached #[shares_resource: write self.entries]
                                    #self.chunk_factory.remove(path)# remove entries from factory ?
                                    self.logger.debug("remove old modified path: "+path)
                                    del self.oldest_modified_date[path]
                            else:
                                self.logger.debug("remove old modified path: "+path)
                                del self.oldest_modified_date[path]
        for writer in writers_to_be_removed:
            self.writers.remove(writer)
            
    def _remove_sleeping_writers(self):
        for writer in self.writers:
            if writer.is_sleeping(): 
                writer.kill()
                self.logger.exception('Terminated sleeping writer.')
                
    def _check_for_failed_writers(self):
        for writer in self.writers:
            if writer.is_finished():
                if writer.get_error():
                    if isinstance(writer.get_error(), StoreSpaceLimitError): #quota error? -> stop writers 
                        self.skip_starting_new_writers_for_next_x_cycles = 4*30 #4 means one minute
                    
    def _remove_finished_readers(self):
        readers_to_be_removed = []
        for reader in self.readers:
            if reader.is_finished():
                readers_to_be_removed.append(reader)
            if reader.is_successful():
                content = reader.get_result() # block until read is done
                self.refresh_cache_entry(reader.path, content, self.store.get_metadata(reader.path)['modified']) #[shares_resource: write self.entries]
                self.stats.add_finished_worker(reader)
        for reader in readers_to_be_removed:
            self.readers.remove(reader)
                
    def _remove_successful_removers(self):
        removers_to_be_removed = []
        for remover in self.removers:
            if remover.is_finished() and remover.is_successful():
                removers_to_be_removed.append(remover)
        for remover in removers_to_be_removed:
            self.removers.remove(remover)
                    
    def _restart_unsuccessful_removers(self):
        for remover in self.removers:
            if remover.is_finished() and not remover.is_successful():
                remover.start()
    
    def _garbage_collect_chunks(self):
        '''Garbage collect stale chunks'''
        garbage_chunks = self.chunk_mapper.get_empty_chunks()
        for chunk in garbage_chunks:
            remover = RemoveWorker(self.store, chunk, False, self.logger)
            remover.start()
    
    def is_in_progress(self, path):
        ''':returns: True iff *path* is currently uploaded or being removed'''
        with self.lock:
            if self.chunk_factory.in_progress(path):
                return True
            if self._get_writer(path): #if there is an active writer uploading path
                return True
            for remover in self.removers:
                filepaths = self.chunk_mapper.get_files_in_chunk(remover.path)
                if path in filepaths:
                    return True
        return False
    
    def _get_writer(self, path):
        ''':returns: active writer that uploads path or None if there is no such writer'''
        for writer in self.writers:
            if path in writer.filepaths:
                return writer
        return None
    
    def _get_reader(self, path):
        ''':returns: active reader that uploads path or None if there is no such writer'''
        for reader in self.readers:
            if path == reader.path:
                return reader
        return None
    
    def last_heartbeat(self):
        ''''Get time since last heartbeat in seconds.'''
        last_heartbeat = self._heartbeat
        return time.time()-last_heartbeat

    def __sleep(self, seconds):
        '''Sleep until *seconds* have passed since last call'''
        if not hasattr(self.__sleep.im_func, 'last_call'):
            self.__sleep.im_func.last_call = time.time()
        last_call = self.__sleep.im_func.last_call
        time_since_last_call = time.time() - last_call
        time_to_sleep_in_s = seconds - time_since_last_call
        if time_to_sleep_in_s > 0:
            time.sleep( time_to_sleep_in_s )
        self.__sleep.im_func.last_call = time.time()
        
    def _get_time_to_sleep(self):
        if len(self.writers) > 5:
            return 0.5
        if len(self.writers) > 2:
            return 1
        if len(self.writers) > 1:
            return 2
        return 60
    
    def run(self): 
        #TODO: check if the cached entries have changed remotely (delta request) and update asynchronously
        #TODO: check if entries being transferred have changed and stop transfer
        while not self._stop:
            self.logger.debug("StoreSyncThread run")
            self._heartbeat = time.time()
            self.__sleep( self._get_time_to_sleep() )
            self._reconnect()
            cnt_writers = len(self.writers)
            self.__sleep(1)
            while True:
                self.tidy_up()
                if cnt_writers == 0:
                    self.__sleep(60)
                    break
                elif len(self.writers) <= cnt_writers / 3:
                    # wait until two thirds of the writers could finish
                    break
                self.__sleep(0.25)
            if self.skip_starting_new_writers_for_next_x_cycles > 0:
                self.skip_starting_new_writers_for_next_x_cycles -= 1
                continue
            self.enqueue_lru_entries()
    
    def _reconnect(self):
        if time.time() > self.last_reconnect + 30*60: #reconnect after 30Min
            with self.lock:
                self.store.reconnect()
            self.last_reconnect = time.time()
    
    def tidy_up(self):
        """Remove finished workers and restart unsuccessful delete jobs."""
        with self.lock:
            self._garbage_collect_chunks()
            self._check_for_failed_writers()
            self._remove_finished_writers()
            self._remove_sleeping_writers()
            self._remove_finished_readers()
            self._remove_successful_removers()
            self._restart_unsuccessful_removers()
            
    def duplicate(self, path_to_src, path_to_dest):
        #print "hiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii"
        with self.lock:
            #print "duplicate sync a"
            self.sync()
            #print "duplicate sync b" 
            src_is_file = self.chunk_mapper.get_chunk_uuid(path_to_src)
            src_directory = get_parent_dir(path_to_src)
            dest_dirctory = get_parent_dir(path_to_dest)
            if src_is_file: #then destination is a file or a non existent file with the same filename as src
                #print ("duplicate %s --> %s"%(path_to_src, path_to_dest))
                old_chunk_uuid = self.chunk_mapper.get_chunk_uuid(path_to_src) 
                new_chunk_uuid = self.chunk_mapper.get_next_chunk_uuid()
                self.store.duplicate(src_directory+old_chunk_uuid, dest_dirctory+new_chunk_uuid)
                filepaths = self.chunk_mapper.get_files_in_chunk(src_directory+old_chunk_uuid)
                alias_filepaths = [path_to_dest if os.path.basename(x) == os.path.basename(path_to_src) else x for x in filepaths] #replace previous alias
                self.chunk_mapper.add_aliases(new_chunk_uuid, filepaths, alias_filepaths)
            elif not src_is_file: #then destination is directory with different name
                #print "duplicate src is directory"
                self.store.duplicate(path_to_src, path_to_dest)
                #print "duplicate dir"
                dirs = [path_to_dest]
                while len(dirs)>0:
                    d = dirs.pop()
                    #print "duplicate pop: "+d
                    listing = self.store.get_directory_listing(d)
                    for item in listing:
                        item = os.path.basename(item)
                        #print "duplicate iterate:"+item
                        if self.chunk_mapper.get_chunk_uuid(src_directory+item): #if mapping exists, item is a file
                            new_chunk_uuid = self.chunk_mapper.get_next_chunk_uuid()
                            filepaths = self.chunk_mapper.get_files_in_chunk(src_directory+item)
                            new_filepaths = map(lambda x: d+'/'+os.path.basename(x), filepaths)
                            #print ("move %s --> %s"%(path_to_src, path_to_dest))
                            self.store.move(d+'/'+old_chunk_uuid, d+'/'+new_chunk_uuid)
                            self.chunk_mapper.put(new_chunk_uuid, new_filepaths)
                        else:
                            #print ("add dir "+d+'/'+item)
                            dirs.append(d+'/'+item) #add to directories to iterate through
    
    def enqueue_lru_entries(self): 
        """Start new writer jobs with expired least recently used cache entries."""
        #TODO: check for user quota error and pause or do exponential backoff
        #TODO: check for internet connection availability and pause or do exponential backoff
        #Entries can be deleted during this method!!!
        dirty_entry_keys = self.cache.get_dirty_lru_entries(800)##KeyError: '########################## ######## list_tail ###### #############################' lru_cache.py return self.entries[self.entries[LISTTAIL]] if self.entries[LISTTAIL] else None
        for path in dirty_entry_keys:
            try:
                if not self.cache.is_expired(path): ##KeyError: '/fstest.7548/d010/66334873' cache.py return time.time() > self.entries[key].updated + self.expire
                    break
                if self.is_in_progress(path):
                    continue
            except (KeyError, IOError):
                self.logger.exception("Key was deleted during synchronization")
                continue
            size_in_mb = self.chunk_factory.get_size_of_next_chunk()
            if len(self.writers) >= self._get_max_threads(size_in_mb):
                return
            expired_file_entry = self.cache.peek_file(path)
            self.chunk_factory.add(expired_file_entry, path)
            self.logger.debug("add old modified path: "+path)
            self.oldest_modified_date[path] = self.cache.get_modified(path) #might change during upload, if new file contents is written to the cache entry
            chunk = self.chunk_factory.get_new_chunk()
            if chunk:
                chunk_uuid = self.chunk_mapper.get_next_chunk_uuid()
                new_worker = ChunkWriteWorker(self.store, chunk.parent_dir, chunk_uuid, chunk.fileobject, chunk.filepaths, self.upload_process_pool, self.logger)
                new_worker.start()
                self.writers.append(new_worker)
        
        size_in_mb = self.chunk_factory.get_size_of_next_chunk()
        if len(self.writers) >= self._get_max_threads(size_in_mb):
            return
        chunk = self.chunk_factory.get_new_chunk()
        if chunk:
            chunk_uuid = self.chunk_mapper.get_next_chunk_uuid()
            new_worker = ChunkWriteWorker(self.store, chunk.parent_dir, chunk_uuid, chunk.fileobject, chunk.filepaths, self.upload_process_pool, self.logger)
            new_worker.start()
            self.writers.append(new_worker)
    
    def enqueue_dirty_entries(self): 
        """Start new writer jobs with dirty cache entries to synchronize *all* files."""
        self._acquire_two_locks() #otherwise, fuse thread could delete current cache entry
        dirty_entry_keys = self.cache.get_dirty_lru_entries(self.max_writer_threads)
        for path in dirty_entry_keys:
            if self.is_in_progress(path):
                continue
            size_in_mb = self.chunk_factory.get_size_of_next_chunk()
            if len(self.writers) >= self._get_max_threads(size_in_mb):
                return
            expired_file_entry = self.cache.peek_file(path)
            self.chunk_factory.add(expired_file_entry, path)
            self.oldest_modified_date[path] = self.cache.get_modified(path) #might change during upload, if new file contents is written to the cache entry
            chunk = self.chunk_factory.get_new_chunk()
            if chunk:
                chunk_uuid = self.chunk_mapper.get_next_chunk_uuid()
                new_worker = ChunkWriteWorker(self.store, chunk.parent_dir, chunk_uuid, chunk.fileobject, chunk.filepaths, self.upload_process_pool, self.logger)
                new_worker.start()
                self.writers.append(new_worker)
        chunk_list = self.chunk_factory.force_get_all_chunks() # force factory to return all chunks, to synchronize all files
        for chunk in chunk_list:
            chunk_uuid = self.chunk_mapper.get_next_chunk_uuid()
            new_worker = ChunkWriteWorker(self.store, chunk.parent_dir, chunk_uuid, chunk.fileobject, chunk.filepaths, self.upload_process_pool, self.logger)
            new_worker.start()
            self.writers.append(new_worker)        
        self._release_two_locks()
    
    def sync(self):
        with self.lock: # does not block writing..
            #print "sync"
            self.logger.info("StoreSyncThread sync")
            while True:
                time.sleep(3)
                self.tidy_up()
                self.enqueue_dirty_entries()
                #print "sync dirty entries:"+repr(self.cache.get_dirty_lru_entries(10))
                if not self.cache.get_dirty_lru_entries(1):  
                    return
            self.logger.info("StoreSyncThread endsync")
        
    
    def delete(self, path, is_dir): 
        '''Delete path from the remote store.'''
        with self.lock:
            self.chunk_factory.remove(path)
            self.chunk_mapper.remove_file(path)
            #TODO: stop workers working on "empty" chunks 
            chunk_uuid = self.chunk_mapper.get_chunk_uuid(path)
            if not chunk_uuid: # path is a directory
                try:
                    remover = RemoveWorker(self.store, path, True, self.logger)
                    remover.start()
                except Exception, e:
                    print str(e)
                    pass #TODO:log exception
            
            
            
    def read(self, path):
        with self.lock:
            if not self._get_reader(path): #ongoing read operation 
                reader = ReadWorker(self.store, path, self.logger)
                reader.start()
                self.readers.append(reader)
    
    def is_cached_version_invalid(self, path):
        """:returns: True if the stores version is newer than the cached entry or does not exist and False otherwise."""
        return not self.cache.exists(path) #for now cached versions are always valid
        #if self.entries.exists(path):
            # TODO: check if online metadata suggests newer version of the file (check if new metadata has been uploaded to store by different session)
            #actual_modified_date = self._get_actual_modified_date(path) # we need to actually download the file & look inside
            #cached_modified_date = self.entries.get_modified(path)
            #if actual_modified_date > cached_modified_date:
            #    self.logger.debug("invalid cache entry: actual_modified_date > cached_modified_date of %s: %s > %s" % (path, actual_modified_date, cached_modified_date))
            #    return True
        #else:
            #return True
        #return False
            
    def blocking_read(self, path):
        with self.lock:
            chunk_id = self.chunk_mapper.get_chunk_uuid(path)
            if not chunk_id: 
                raise NoSuchFilesytemObjectError(path, 0)
            parent_dir = get_parent_dir(path)
            self.read(parent_dir+chunk_id)
            reader = self._get_reader(parent_dir+chunk_id)
            tar_content = reader.get_result() # block until read is done
            self.stats.add_finished_worker(reader)
            if reader.get_error():
                err = reader.get_error()
                if not err in [StoreAccessError, NoSuchFilesytemObjectError, StoreAutorizationError]:
                    err = StoreAccessError(str(err),0)
                raise err
            for filepath, file_content, modified_date in self.chunk_mapper.iterate_files_from_chunk(tar_content, chunk_id):
                if not self.cache.exists(parent_dir+filepath):
                    if self.chunk_mapper.filepath_mapping[(parent_dir+filepath).encode("utf8")] == chunk_id: # only if the file contents in the chunk is the current version
                        self.refresh_cache_entry(parent_dir+filepath, file_content, modified_date) #[shares_resource: write self.entries]
            self.readers.remove(reader)
            
    def delete_cache_entry(self, path):
        with self.protect_cache_from_write_access:
            self.cache.delete(path)
            
    def _acquire_two_locks(self):
        while self.protect_cache_from_write_access.acquire(True) and not self.lock.acquire(False): #acquire(False) returns False if it cannot acquire the lock
            self.protect_cache_from_write_access.release()
            time.sleep(0.0001) #give other threads a chance to get both locks

    def _release_two_locks(self):
        self.lock.release()
        self.protect_cache_from_write_access.release()
    
    #these are called from this thread while multiprocessingstore instance operates.., but should only be called in between its methods
    #how can we accomplish this? -> second lock
    
    def write_cache_entry(self, path, contents):
        with self.protect_cache_from_write_access:
            self.cache.write(path, contents)
        
    def refresh_cache_entry(self, path, contents, modified):
        with self.protect_cache_from_write_access:
            self.cache.refresh(path, contents, modified)
            
    def set_dirty_cache_entry(self, path, is_dirty): #may be called by this class
        with self.protect_cache_from_write_access:
            #give it some time until deletion:
            self.cache.update(path)
            self.cache.set_dirty(path, is_dirty)
    
    def set_modified_cache_entry(self, path, updatetime): #may be called by this class 
        with self.protect_cache_from_write_access:
            self.cache.set_modified(path, updatetime)
            
