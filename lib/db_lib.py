import sys
import mysql.connector
from mysql.connector import errorcode as mysqlErrCode

import logging

logger = logging.getLogger(__name__)

class db_lib():
    
    def __init__(self,new=False,dbname=""):
        
        logger.info("Connecting to host")
        try:
            self.db = mysql.connector.connect(
                            host = "localhost",
                            user = "admin",
                            passwd = "passwd",
                            use_pure = True)
            logger.info("Connected to host")
        except:
            logger.error("Error during connection to the host")
            sys.exit()
        self.cursor = self.db.cursor()
        self.dbname = dbname
        
        if new:
            self.new_db()
        else:
            self.load_db(self.dbname)
    
    def new_db(self):
        """
        Create new database
        """
        query = "CREATE DATABASE {}".format(self.dbname)
        try:
            self.cursor.execute(query)
            logger.info("Created database {}".format(self.dbname))
        except mysql.connector.Error as err:
            if err.errno == mysqlErrCode.ER_DB_CREATE_EXISTS:
                logger.error("Existing database")
                sys.exit()
            else:
                logger.error(err)
                sys.exit()
        
        return self.cursor
    
    def load_db(self,dbname):
        """
        Load existing database
        """
        query = "USE {}".format(dbname)
        try:
            self.cursor.execute(query)
            logger.info("Using database {}".format(dbname))
        except mysql.connector.Error as err:
            if err.errno == mysqlErrCode.ER_BAD_DB_ERROR:
                logger.error("Non existing database")
                sys.exit()
            else:
                logger.error(err)
                sys.exit()
        
    def close(self):
        """
        Close the database
        """
        self.db.close()
    
    def delete_db(self):
        """
        Delete existing database
        """
        try:
            self.cursor.execute("DROP SCHEMA %s" %self.dbname)
            logger.info("Deleted database {}".format(self.dbname))
        except mysql.connector.Error as err:
            logger.error(err)
            sys.exit()
    
    def execute(self,query):
        self.cursor.execute(query)
    
    def commit(self):
        self.db.commit()