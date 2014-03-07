from cloudfusion.store.store_worker import WriteWorker

class ChunkWriteWorker(WriteWorker):
    def __init__(self, store, chunk_dir, chunk_uuid, chunk, filepaths, logger):
        self.chunk_dir = chunk_dir
        self.chunk_uuid = chunk_uuid
        self.filepaths = filepaths
        super( ChunkWriteWorker, self ).__init__(store, chunk_dir+chunk_uuid, chunk, logger)