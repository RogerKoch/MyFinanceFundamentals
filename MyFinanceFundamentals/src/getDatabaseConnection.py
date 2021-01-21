#!/usr/bin/python
# -*- coding: UTF-8 -*-

import mysql.connector

class DatabaseManager():
    
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_config'):
            cls._config=super(DatabaseManager, cls).__new__(cls, *args, **kwargs)
            cls._dbConnections = {}
            
        return cls._config
    
    def setDBConnection(self, dbName):
        if dbName in list(self._dbConnections.keys()):
            return self._dbConnections.get(dbName)
        else:
            if dbName == 'stockInfo':
                    mydb = mysql.connector.connect(
                                    host='161.97.157.97',
                                    user='stockInfo',
                                    password='stockInfo2020',
                                    database='stockInfo'
                                    )
                
                    #myCursor = mydb.cursor(buffered=True)
                    
                    self._dbConnections[dbName] = mydb
            
            
            return mydb
        
