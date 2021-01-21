#!/usr/bin/python
# -*- coding: UTF-8 -*-

from bs4 import BeautifulSoup

from getDatabaseConnection import DatabaseManager
import time
import requests




def updateDilutedEPS():
    
    db = DatabaseManager()
    mydb = db.setDBConnection('stockInfo')
    
    errorCursor = mydb.cursor()
    errorQuery = """Call stockInfo.spInsError ('{0}', '{1}', '{2}')"""    
    
    
    try:
        myCursor = mydb.cursor()    
    
        baseURL = 'https://finance.yahoo.com/quote/{0}/financials?p={0}'
                
        #Get all ISINs    
        insertQuery = ("""Call stockInfo.spInsEPSFigures (%s, %s, %s)""")
        insertCursor = mydb.cursor()
        
        #myCursor.execute('SELECT isin, yahooTicker FROM stockInfo.BasicData')
        myCursor.execute('SELECT isin, yahooTicker FROM stockInfo.BasicData LIMIT 10')
        allIsins = myCursor.fetchall()
        for sqlRow in allIsins:    
            try:
                isin = sqlRow[0]
                ticker = sqlRow[1]
        
                runURL = baseURL.format(ticker)
                   
                page_content = requests.get(runURL).text
                bs = BeautifulSoup(page_content, 'lxml')
                finHeader =bs.find('span', text='Breakdown').parent.parent
                        
                epsRow = bs.find('span', text='Diluted EPS').parent.parent.parent
                valueRows = epsRow.find_all('span')
                headerRows = finHeader.find_all('span')
                                                       
                for i in range(1,len(valueRows)):
                    _m,_d,year = headerRows[-i].text.split('/')
                    epsDiluted = float(valueRows[-i].text) * 1000    
                    insertData = (isin, year, epsDiluted)
                    insertCursor.execute(insertQuery, insertData)
                mydb.commit()            
                time.sleep(1)
            except KeyError as e:
                errorCursor.execute(errorQuery.format(isin, 'updateDilutedEPS', str(e).replace("'", "''")))
            except ValueError as e:
                errorCursor.execute(errorQuery.format(isin, 'updateDilutedEPS', str(e).replace("'", "''")))
            except Exception as e:
                    errorCursor.execute(errorQuery.format(isin, 'updateDilutedEPS', str(e).replace("'", "''")))
                        
        mydb.commit()  
        insertCursor.close()
        myCursor.close()
    except Exception as e:
        errorCursor.execute(errorQuery.format('N/A', 'updateDilutedEPS', str(e).replace("'", "''")))
        
    errorCursor.execute(errorQuery.format('FINISH', 'updateDilutedEPS', 'FINISH IMPORT'))    
    errorCursor.close()
    mydb.commit()
    


if __name__ == '__main__':
    updateDilutedEPS()



