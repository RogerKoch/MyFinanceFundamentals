#!/usr/bin/python
# -*- coding: UTF-8 -*-

from getDatabaseConnection import DatabaseManager
import time
import yfinance as yf


def updateDividend():
    
    db = DatabaseManager()
    mydb = db.setDBConnection('stockInfo')
    errorCursor = mydb.cursor()
    errorQuery = """Call stockInfo.spInsError ('{0}', '{1}', '{2}')"""    
    
    
    try:
        
        myCursor = mydb.cursor()    
                
        #Get all ISINs
        insertQuery = ("""Call stockInfo.spInsDivFigures (%s, %s, %s)""")
        insertCursor = mydb.cursor()
        
        #myCursor.execute('SELECT isin, yahooTicker FROM stockInfo.BasicData')
        myCursor.execute('SELECT isin, yahooTicker FROM stockInfo.BasicData LIMIT 10')
        allIsins = myCursor.fetchall()
        for sqlRow in allIsins:    
            try:
                isin = sqlRow[0]
                ticker = yf.Ticker(sqlRow[1])
                dividends = ticker.dividends
                
                first = True
                divDict = {}
                currentYear = 1900
                curYearList = []
                
                for divDate, divValue in dividends.items():
                    divYear=int(divDate.strftime('%Y-%m-%d').split('-')[0])
                    if currentYear == 1900:
                        currentYear = divYear
                    if divYear != currentYear:
                        if first is True:
                            first = False
                        else:
                            divDict[currentYear] = curYearList
                        curYearList = []                
                        currentYear = divYear
                    curYearList.append(float(divValue))
                if currentYear > 1900:
                    divDict[currentYear] = curYearList
                
            
                for divKey in divDict:
                    insertData = (isin, divKey, sum(divDict.get(divKey)))
                    insertCursor.execute(insertQuery, insertData)
                mydb.commit()
                time.sleep(1)
            except KeyError as e:
                errorCursor.execute(errorQuery.format(isin, 'updateDividend', str(e).replace("'", "''")))
            except ValueError as e:
                errorCursor.execute(errorQuery.format(isin, 'updateDividend', str(e).replace("'", "''")))
            except Exception as e:
                    errorCursor.execute(errorQuery.format(isin, 'updateDividend', str(e).replace("'", "''")))
                            
                
        insertCursor.close()
        myCursor.close()
    except Exception as e:
        errorCursor.execute(errorQuery.format('N/A', 'updateDividend', str(e).replace("'", "''")))
    
    errorCursor.execute(errorQuery.format('FINISH', 'updateDividend', 'FINISH IMPORT'))    
    errorCursor.close()
    mydb.commit()



if __name__ == '__main__':
    updateDividend()
