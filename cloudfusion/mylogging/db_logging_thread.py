'''
Created on Jul 29, 2013

@author: joe
'''

import tempfile
import sqlite3
from time import sleep
import multiprocessing
import pickle
import logging
from cloudfusion.mylogging.db_handler import DBHandler

'''
Process log records from a :class:`cloudfusion.mylogging.db_handler.DBHandler` instance.
DBHandler instances can send log records from multiple processes, simultaneously. Killing those processes does not corrupt logging.
'''
_logging_db_filename = None
_logging_db_file = None
_last_rowid = 0
abort = multiprocessing.Value('i', 0)

def start():
    """Start processing records in a background thread."""
    global _logging_db_filename, _logging_db_file
    _logging_db_file = tempfile.NamedTemporaryFile()
    _logging_db_filename = _logging_db_file.name
    process = multiprocessing.Process(target=_serve_until_stopped)
    process.daemon = True
    conn = sqlite3.connect(_logging_db_filename, 60, isolation_level=None) # isolation_level=None -> commit immediately
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS logging (Id INTEGER PRIMARY KEY, record BLOB);") # 
    cursor.execute("PRAGMA journal_mode=WAL;") # slight optimization for concurrent access
    conn.close()
    process.start()
    
def create_dbhandler():
    '''Get a database logging handler to log to the database logging thread. 
    Only call this method after calling :func:`start` once.
    '''
    return DBHandler(_logging_db_filename)

def make_logger_multiprocessingsave(logger):
    logger.handlers = []
    db_handler = create_dbhandler()
    logger.addHandler(db_handler)
    return logger
    
def get_logging_db_identifier():
    """:returns: identifier of the database in use for :class:`cloudfusion.mylogging.db_handler.DBHandler`"""
    return _logging_db_filename  

def __get_records(cursor, conn):
    """ Get and delete next record from database.
    :returns: next record from database or None"""
    global _last_rowid
    INDEX = 0; VALUE = 1
    ret = []
    cursor.execute("SELECT * FROM logging WHERE ROWID > %s ORDER BY ROWID ASC;" % _last_rowid)
    rows = cursor.fetchall()
    if len(rows) == 0:
        return ret
    _last_rowid = rows[-1][INDEX]
    for row in rows:
        record_blob = row[VALUE]
        try:
            ret.append(pickle.loads(str(record_blob)))
        except Exception, e:
            import sys, traceback
            sys.stderr.write("Failing to unpickle %s\n" % str(record_blob))
            traceback.print_exc(file=sys.stderr)
    return ret
    
def _serve_until_stopped():
    global abort
    conn = sqlite3.connect(_logging_db_filename, 60, isolation_level=None) 
    cursor = conn.cursor()
    abort.value = 0
    try_cnt = 0
    while not abort.value:
        try:
            sleep(1)
            try_cnt += 1
            records = __get_records(cursor, conn)
            if not records: 
                continue
            for record in records:
                _handleRecord(record)
            _clean_db(cursor, conn)
            try_cnt = 0
        except sqlite3.OperationalError:
            if try_cnt > 2:
                import sys, traceback
                sys.stderr.write("Retrying to get logging records or delete them the %s time\n" % try_cnt)
                traceback.print_exc(file=sys.stderr)

def _clean_db(cursor, conn):
    global _last_rowid
    if _last_rowid > 1000:
        cursor.execute("DELETE FROM logging WHERE ROWID <= %s ;" % _last_rowid)
        _last_rowid = 0
            
def stop():
    global abort 
    abort.value = 1


def _handleRecord(record):
    logger_name = record.name
    logger = logging.getLogger(logger_name)
    logger.handle(record)
