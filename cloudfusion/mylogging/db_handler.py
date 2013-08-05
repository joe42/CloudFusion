import logging
import logging.handlers
import sqlite3
import pickle
import os

class DBHandler(logging.Handler):
    """
    This logging handler sends events to :mod:`cloudfusion.mylogging.db_logging_thread`.
    """

    def __init__(self, db_identifier): 
        """:param db_identifier: identifier from :func:`cloudfusion.mylogging.db_logging_thread.get_logging_db_identifier`"""
        logging.Handler.__init__(self)
        self.db_identifier = db_identifier
        self.conn = None
        self.cursor = None
        self.pid = os.getpid()
        self.reconnect()
        self.cursor.execute("CREATE TABLE IF NOT EXISTS logging (record BLOB);")
        
    def reconnect(self):
        self.conn = sqlite3.connect(self.db_identifier, 60, check_same_thread = False) 
        self.cursor = self.conn.cursor()
 
    def emit(self, record):
        """
        Emit a record.

        Writes the LogRecord to the database.
        """
        if self.pid != os.getpid(): #reconnect if dbhandler is in new process fork
            self.reconnect()
            self.pid = os.getpid()
        ei = record.exc_info
        if ei:
            self.format(record) 
            record.exc_text
            record.exc_info = None # to avoid Unpickleable error
        s = pickle.dumps(record)
        if ei:
            record.exc_info = ei # for next handler
        self.cursor.execute("INSERT INTO logging (record) VALUES (?);", (sqlite3.Binary(s),))
        self.conn.commit()
        
    