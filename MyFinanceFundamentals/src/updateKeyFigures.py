#!/usr/bin/python
# -*- coding: UTF-8 -*-

import yfinance as yf

from getDatabaseConnection import DatabaseManager
import time
from datetime import datetime, timedelta

def updateKeyFigures():
    
    startDate = datetime.today() - timedelta(days=1)
    
    db = DatabaseManager()
    mydb = db.setDBConnection('stockInfo')
    errorCursor = mydb.cursor()
    errorQuery = """Call stockInfo.spInsError ('{0}', '{1}', '{2}')"""

    try:
        
        myCursor = mydb.cursor()    
                
        #Get all ISINs
        insertQuery = ("""Call stockInfo.spInsKeyFigures (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")
        insertCursor = mydb.cursor()
        
        #myCursor.execute('SELECT isin, yahooTicker FROM stockInfo.BasicData')
        myCursor.execute('SELECT isin, yahooTicker FROM stockInfo.BasicData LIMIT 10')
        allIsins = myCursor.fetchall()
        for sqlRow in allIsins:
            try:
                ticker = yf.Ticker(sqlRow[1])
                isin = sqlRow[0]
                kf = ticker.info
                hight_52 = kf.get('fiftyTwoWeekHigh',None)
                low_52 = kf.get('fiftyTwoWeekLow',None)
                peRatio = kf.get('trailingPE',None)
                bookValue = kf.get('bookValue',None)
                priceToBook = kf.get('priceToBook',None)
                dividendRate = kf.get('dividendRate',None)
                dividendYield = kf.get('dividendYield',None)
                dividendRateDate = kf.get('exDividendDate',None)
                if dividendRateDate is not None:
                    dividendRateDate = datetime.fromtimestamp(dividendRateDate).strftime('%Y-%m-%d')
                
                insertData = (isin, startDate, hight_52, low_52, peRatio, bookValue, priceToBook, dividendRate, dividendYield, dividendRateDate)
                insertCursor.execute(insertQuery, insertData)
                mydb.commit()
            except KeyError as e:
                errorCursor.execute(errorQuery.format(isin, 'updateKeyFigures', str(e).replace("'", "''")))
            except ValueError as e:
                errorCursor.execute(errorQuery.format(isin, 'updateKeyFigures', str(e).replace("'", "''")))          
            time.sleep(1)
        insertCursor.close()            
        myCursor.close()
    except Exception as e:
        if 'isin' in locals():
            errorCursor.execute(errorQuery.format(isin, 'updateKeyFigures', str(e).replace("'", "''")))
        else:
            errorCursor.execute(errorQuery.format('N/A', 'updateKeyFigures', str(e).replace("'", "''")))
        
    errorCursor.execute(errorQuery.format('FINISH', 'updateKeyFigures', 'FINISH IMPORT'))    
    errorCursor.close()
    mydb.commit()

if __name__ == '__main__':
    updateKeyFigures()
