import logging
import logging.handlers
import sqlite3
import pickle
import os
from time import sleep
import sys

class DBHandler(logging.Handler):
    """
    This logging handler sends events to :mod:`cloudfusion.mylogging.db_logging_thread`.
    """

    def __init__(self, db_identifier): 
        """:param db_identifier: identifier from :func:`cloudfusion.mylogging.db_logging_thread.get_logging_db_identifier`"""
        logging.Handler.__init__(self)
        self.db_identifier = db_identifier
        if not db_identifier:
            sys.stderr.write("Logging handler DBHandler was initialized before calling cloudfusion.mylogging.db_logging_thread.start(). Nothing will be logged.\n")
        self.conn = None
        self.cursor = None
        self.pid = -1
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.setFormatter(formatter)
        
    def reconnect(self):
        self.conn = sqlite3.connect(self.db_identifier, check_same_thread = False, isolation_level=None) 
        self.cursor = self.conn.cursor()
 
    def emit(self, record):
        """
        Emit a record.

        Writes the LogRecord to the database.
        """
        try:
            if not self.db_identifier:
                return
            if self.pid != os.getpid(): #reconnect if dbhandler is in new process fork or if it is not yet connected
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
            retry = True
            try_cnt = 0
            while retry:
                try:
                    try_cnt += 1
                    self.cursor.execute("INSERT INTO logging (record) VALUES (?);", (sqlite3.Binary(s),))
                    retry = False
                    return
                except sqlite3.OperationalError, e:
                    if try_cnt > 2:
                        sys.stderr.write("Lost logging message: %s\n" % self.format(record))
                        return
        except:
            import traceback
            traceback.print_exc()
            sys.stderr.write("Lost logging message: %s\n" % self.format(record))
        
    