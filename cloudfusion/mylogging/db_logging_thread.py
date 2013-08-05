'''
Created on Jul 29, 2013

@author: joe
'''

import tempfile
import sqlite3
from time import sleep
from threading import Thread
import pickle
import logging
from cloudfusion.mylogging.db_handler import DBHandler

'''
Process log records from a :class:`cloudfusion.mylogging.db_handler.DBHandler` instance.
DBHandler instances can send log records from multiple processes, simultaneously. Killing those processes does not corrupt logging.
'''
_logging_db_filename = None
_logging_db_file = None
abort = False

def start():
    """Start processing records in a background thread."""
    global _logging_db_filename, _logging_db_file
    _logging_db_file = tempfile.NamedTemporaryFile()
    _logging_db_filename = _logging_db_file.name
    thread = Thread(target=_serve_until_stopped)
    thread.start()
    
def create_dbhandler():
    return DBHandler(_logging_db_filename)
    
def get_logging_db_identifier():
    """:returns: identifier of the database in use for :class:`cloudfusion.mylogging.db_handler.DBHandler`"""
    return _logging_db_filename  

def __get_next_record(cursor, conn):
    """ Get and delete next record from database.
    :returns: next record from database or None"""
    cursor.execute("SELECT * FROM logging ORDER BY ROWID ASC LIMIT 1 ;")
    row = cursor.fetchone()
    cursor.execute("DELETE FROM logging WHERE ROWID = (SELECT MIN(ROWID) FROM logging) ;")
    conn.commit()
    if row == None:
        return None
    record_blob = row[0]
    record = pickle.loads(str(record_blob))
    return record
    
def _serve_until_stopped():
    global abort
    conn = sqlite3.connect(_logging_db_filename, 60) 
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS logging (record BLOB);")
    abort = False
    while not abort:
        try:
            record = __get_next_record(cursor, conn)
            if not record: 
                sleep(0.5)
                continue
            _handleRecord(record)
        except Exception:
            import sys, traceback
            traceback.print_exc(file=sys.stderr)
            
def stop():
    global abort 
    abort = True


def _handleRecord(record):
    logger_name = record.name
    logger = logging.getLogger(logger_name)
    logger.handle(record)
