#!/usr/bin/python
# -*- coding: UTF-8 -*-


from getDatabaseConnection import DatabaseManager
import yfinance as yf
import time
from urllib.error import HTTPError


def updateBasicData():

    db = DatabaseManager()
    mydb = db.setDBConnection('stockInfo')
    errorCursor = mydb.cursor()
    errorQuery = """Call stockInfo.spInsError ('{0}', '{1}', '{2}')"""    
    
    
    try:
        
        selectCursor = mydb.cursor(buffered=True)
        
        selectQuery = "select ISIN, yahooTicker FROM `stockInfo`.`BasicData` WHERE Sector is Null OR Industry is NULL"
        selectCursor.execute(selectQuery)
        
        for (isin, yahooTicker) in selectCursor:
            try:
                ticker = yf.Ticker(yahooTicker) 
                ti = ticker.info
                if 'industry' in ti:
                    industry = ti['industry'].replace("'", "''")
                else:
                    industry= ''
                if 'sector' in ti:
                    sector = ti['sector'].replace("'", "''")
                else:
                    sector = ''
                if 'currency' in ti:
                    currency = ti['currency'].replace("'", "''")
                else:
                    currency = ''
            
                insertQuery = ("""Call stockInfo.spUpSectorIndustry (%s, %s, %s, %s)""")
                insertCursor = mydb.cursor()
                insertData = (isin, sector, industry, currency)
                insertCursor.execute(insertQuery, insertData)
                mydb.commit()
                insertCursor.close()
    
            except KeyError as e:
                errorCursor.execute(errorQuery.format(isin, 'updateBasicData', str(e).replace("'", "''")))
            except ValueError as e:
                errorCursor.execute(errorQuery.format(isin, 'updateBasicData', str(e).replace("'", "''")))
            except HTTPError as e:
                errorCursor.execute(errorQuery.format(isin, 'updateBasicData', str(e).replace("'", "''")))
            
            time.sleep(1)
        
        selectCursor.close()
        
    except Exception as e:
        if 'isin' in locals():
            errorCursor.execute(errorQuery.format(isin, 'updateBasicData', str(e).replace("'", "''")))
        else:
            errorCursor.execute(errorQuery.format('N/A', 'updateBasicData', str(e).replace("'", "''")))

    errorCursor.execute(errorQuery.format('FINISH', 'updateBasicData', 'FINISH IMPORT'))    
    errorCursor.close()
    mydb.commit()
    

if __name__ == '__main__':
    updateBasicData()


