#!/usr/bin/python
# -*- coding: UTF-8 -*-

import yfinance as yf

from getDatabaseConnection import DatabaseManager
from datetime import datetime, timedelta
from dateutil.parser import parse

import time
import math

def updateTimeseries(endDate, startDate=None, onlyNew=False):
        
    db = DatabaseManager()
    mydb = db.setDBConnection('stockInfo')
    
    errorCursor = mydb.cursor()
    errorQuery = """Call stockInfo.spInsError ('{0}', '{1}', '{2}')"""    
    try:
        myCursor = mydb.cursor()    
        
        if startDate is None:
            #GetStartDate
            myCursor.execute("SELECT max(TSDate) FROM stockInfo.Timeseries;")
            myStartDates = myCursor.fetchall()
            startDate =  myStartDates[0][0]
            if startDate is None:
                startDate = '2019-12-31'
            
        #Get all ISINs
        #Is much faster then call an insert SP multiple times
        insertQuery = """INSERT INTO stockInfo.Timeseries
                            (ISIN, TSDate, TSValue)
                            VALUES
                            (%s, %s, %s)
                            ON DUPLICATE KEY UPDATE TSValue = Values(TSValue)"""
                            
        insertCursor = mydb.cursor()
        if onlyNew is True:
            endDateTime = parse(endDate)
            endDateTimeM1 = endDateTime - timedelta(days=1)
            endDateCalc = endDateTimeM1.strftime('%Y-%m-%d')            
            selectUnivers = """SELECT bd.ISIN, bd.yahooTicker FROM stockInfo.Timeseries AS ts
                                RIGHT JOIN stockInfo.BasicData AS bd
                                ON ts.ISIN=bd.ISIN
                                WHERE ts.ISIN is NULL
                                GROUP BY bd.isin
                                UNION
                                SELECT ts.isin, bs.yahooTicker FROM stockInfo.Timeseries AS ts
                                INNER JOIN stockInfo.BasicData AS bs
                                ON bs.ISIN=ts.isin
                                GROUP BY isin
                                HAVING max(tsdate) < '{0}'
                            """.format(endDateCalc)
        else:
            selectUnivers = """SELECT isin, yahooTicker FROM stockInfo.BasicData"""
            
        try:
            myCursor.execute(selectUnivers)
            allIsins = myCursor.fetchall()
            for sqlRow in allIsins:
                ticker = yf.Ticker(sqlRow[1])
                isin = sqlRow[0]
#                 print(isin)
                ts=ticker.history(start=startDate, end=endDate, interval='1d')
                dataList = []
                for index, row in ts.iterrows():
                    tsDate = index.strftime('%Y-%m-%d')            
                    tsValue = float(row.Close)
                    if math.isnan(tsValue):
                        tsValue = 0
                    insertData = (isin, tsDate, tsValue)
                    dataList.append(insertData)
                    #insertCursor.execute(insertQuery, insertData)
                insertCursor.executemany(insertQuery, dataList)
                mydb.commit()
                time.sleep(1)
        except Exception as e:
            if 'isin' in locals():
                errorCursor.execute(errorQuery.format(isin, 'updateTimeseries', str(e).replace("'", "''")))
            else:
                errorCursor.execute(errorQuery.format('N/A', 'updateTimeseries', str(e).replace("'", "''")))
        insertCursor.close()            
        myCursor.close()
    except Exception as e:
        errorCursor.execute(errorQuery.format('N/A', 'updateTimeseries', str(e).replace("'", "''")))
    
    mydb.commit()    
    errorCursor.close()        
    

if __name__ == '__main__':
    updateTimeseries(None, None, False)
